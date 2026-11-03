use crate::{
    bridge::Bridge,
    models::{Outbound, now_ms},
    ports::{Channel, CodexPort},
    store::{Store, enqueue, enqueue_thread},
    wechat::WechatApiError,
};
use anyhow::{Context, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn deliver_next(
    store: &Store,
    channel: &dyn Channel,
    cancel: &CancellationToken,
    now: u64,
) -> Result<bool> {
    // Subscribe before reading so a switch between queue inspection and send
    // cannot leave an old request waiting through its whole HTTP timeout.
    let mut bindings = store.subscribe_bindings();
    let state = store.read()?;
    if let Some(message) = state.outbox.first()
        && !selected(&state, message)
    {
        discard(store, &message.id)?;
        return Ok(true);
    }
    let Some(message) = state
        .outbox
        .first()
        .filter(|m| m.next_attempt_at <= now)
        .cloned()
    else {
        return Ok(false);
    };
    let token = state
        .peers
        .get(&message.peer)
        .filter(|value| !value.is_empty());
    let sending_cancel = cancel.child_token();
    let result = match token {
        Some(token) => {
            let sending = channel.send(&message, token, &sending_cancel);
            tokio::pin!(sending);
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        sending_cancel.cancel();
                        return Ok(false);
                    }
                    changed = bindings.changed(), if message.thread_id.is_some() => {
                        changed.context("Channel binding notifications closed")?;
                        let state = store.read()?;
                        if !selected(&state, &message) || !state.outbox.iter().any(|m| m.id == message.id) {
                            sending_cancel.cancel();
                            discard(store, &message.id)?;
                            return Ok(true);
                        }
                    }
                    result = &mut sending => break result,
                }
            }
        }
        None => Err(anyhow::anyhow!(
            "No WeChat reply credentials received for this user yet"
        )),
    };
    match result {
        Ok(()) => store.update(|s| {
            s.outbox.retain(|m| m.id != message.id);
            Ok(())
        })?,
        Err(_) if cancel.is_cancelled() => return Ok(false),
        Err(error) if requires_login(&error) => return Err(error),
        Err(error) => store.update(|s| {
            if let Some(pending) = s.outbox.iter_mut().find(|m| m.id == message.id) {
                pending.attempts = pending.attempts.saturating_add(1);
                pending.error = Some(error.to_string());
                pending.next_attempt_at =
                    now.saturating_add(60_000_u64.min(1_000 * 2_u64.pow(pending.attempts.min(6))));
            }
            Ok(())
        })?,
    }
    Ok(true)
}

fn selected(state: &crate::models::State, message: &Outbound) -> bool {
    message.thread_id.as_deref().is_none_or(|thread_id| {
        state.bindings.get(&message.peer).map(String::as_str) == Some(thread_id)
    })
}

fn discard(store: &Store, id: &str) -> Result<()> {
    store.update(|state| {
        state.outbox.retain(|message| message.id != id);
        Ok(())
    })
}

fn requires_login(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<WechatApiError>()
        .is_some_and(WechatApiError::requires_login)
}

async fn pause(cancel: &CancellationToken, ms: u64) {
    tokio::select! { _ = cancel.cancelled() => {}, _ = tokio::time::sleep(Duration::from_millis(ms)) => {} }
}

pub async fn run_bridge(
    store: Arc<Store>,
    bridge: Arc<Bridge>,
    codex: Arc<dyn CodexPort>,
    channel: Arc<dyn Channel>,
    owner_id: String,
    cancel: CancellationToken,
) -> Result<()> {
    let stop = cancel.child_token();
    let mut events = codex.subscribe();
    let epoch = Arc::new(AtomicU64::new(0));
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    bridge.recover()?;
    let event_bridge = bridge.clone();
    let event_codex = codex.clone();
    let event_stop = stop.clone();
    let event_epoch = epoch.clone();
    tasks.spawn(async move {
        loop {
            let event = tokio::select! { _ = event_stop.cancelled() => return Ok(()), message = events.recv() => message };
            match event {
                Ok(message) => {
                    let disconnected = message.method == "bridge/disconnected";
                    event_bridge.on_codex(message).await?;
                    if disconnected { event_epoch.fetch_add(1, Ordering::SeqCst); }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    eprintln!("Codex event stream overflowed. Reconnecting and checking native history.");
                    event_bridge.recover()?;
                    event_epoch.fetch_add(1, Ordering::SeqCst);
                    event_codex.close().await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => anyhow::bail!("Codex event stream closed"),
            }
        }
    });
    // A signal during initialization must still close the client, never the daemon.
    let initial = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        initial = async { codex.start().await?; bridge.connected().await } => initial,
    };
    if initial.is_ok() && !stop.is_cancelled() {
        let poll_store = store.clone();
        let poll_channel = channel.clone();
        let poll_stop = stop.clone();
        tasks.spawn(async move {
            while !poll_stop.is_cancelled() {
                let cursor = poll_store.read()?.cursor;
                match poll_channel.poll(&cursor, &poll_stop).await {
                    Ok(batch) => poll_store.accept_batch(
                        &batch.cursor,
                        batch
                            .messages
                            .into_iter()
                            .filter(|m| m.peer == owner_id)
                            .collect(),
                    )?,
                    Err(_) if poll_stop.is_cancelled() => break,
                    Err(error) if requires_login(&error) => return Err(error),
                    Err(error) => {
                        eprintln!("WeChat receive failed: {error}");
                        pause(&poll_stop, 2_000).await;
                    }
                }
            }
            Ok(())
        });
        let inbox_store = store.clone();
        let inbox_bridge = bridge.clone();
        let inbox_codex = codex.clone();
        let inbox_stop = stop.clone();
        tasks.spawn(async move {
            let mut connected_epoch = 0;
            while !inbox_stop.is_cancelled() {
                let current_epoch = epoch.load(Ordering::SeqCst);
                if current_epoch != connected_epoch {
                    let restored = tokio::select! {
                        _ = inbox_stop.cancelled() => break,
                        restored = async { inbox_codex.start().await?; inbox_bridge.connected().await } => restored,
                    };
                    if let Err(error) = restored { eprintln!("Codex reconnect failed: {error}"); pause(&inbox_stop, 2_000).await; continue; }
                    connected_epoch = current_epoch;
                    continue;
                }
                let entry = inbox_store.read()?.inbox.values().find(|m| m.status == "pending").cloned();
                if let Some(entry) = entry {
                    if let Err(error) = inbox_bridge.handle(entry.clone()).await {
                        eprintln!("Message handling failed: {error}");
                        inbox_store.update(|state| {
                            if let Some(current) = state.inbox.get_mut(&entry.message.id)
                                && current.status != "done" && current.status != "unknown" {
                                current.status = "unknown".into();
                                if let Some(thread_id) = &entry.thread_id {
                                    enqueue_thread(state, &entry.message.peer, thread_id, "Message outcome unknown. Check /status before retrying.");
                                } else {
                                    enqueue(state, &entry.message.peer, "Message outcome unknown. Check /status before retrying.");
                                }
                            }
                            Ok(())
                        })?;
                    }
                } else { pause(&inbox_stop, 100).await; }
            }
            Ok(())
        });
        let send_store = store.clone();
        let send_stop = stop.clone();
        tasks.spawn(async move {
            while !send_stop.is_cancelled() {
                deliver_next(&send_store, &*channel, &send_stop, now_ms()).await?;
                pause(&send_stop, 200).await;
            }
            Ok(())
        });
    }
    let result = match initial {
        Err(error) => Err(error),
        Ok(()) => tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = tasks.join_next() => match result {
                Some(result) => match result { Ok(result) => result, Err(error) => Err(error).context("Bridge task failed") },
                None => Ok(()),
            },
        },
    };
    stop.cancel();
    let closed = codex.close().await;
    while tasks.join_next().await.is_some() {}
    result.and(closed)
}
