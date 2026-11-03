use anyhow::{Result, bail};
use async_trait::async_trait;
use codex_wechat::{
    bridge::Bridge,
    models::{Inbound, Outbound, PollBatch, RpcMessage},
    ports::{Channel, CodexPort},
    runtime::{deliver_next, run_bridge},
    store::{Store, enqueue},
    wechat::WechatApiError,
};
use serde_json::Value;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Notify, broadcast};
use tokio_util::sync::CancellationToken;

struct MockChannel {
    sent: Mutex<Vec<(String, String)>>,
    fail: AtomicBool,
    expired: bool,
}
#[async_trait]
impl Channel for MockChannel {
    async fn poll(&self, _: &str, cancel: &CancellationToken) -> Result<PollBatch> {
        cancel.cancelled().await;
        Ok(PollBatch::default())
    }
    async fn send(&self, message: &Outbound, token: &str, _: &CancellationToken) -> Result<()> {
        self.sent
            .lock()
            .unwrap()
            .push((message.id.clone(), token.into()));
        if self.expired {
            return Err(WechatApiError { code: -14 }.into());
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            bail!("offline");
        }
        Ok(())
    }
}
struct PendingCodex {
    events: broadcast::Sender<RpcMessage>,
    started: Notify,
    stopped: Notify,
    closed: AtomicBool,
}
impl PendingCodex {
    fn new() -> Self {
        Self {
            events: broadcast::channel(16).0,
            started: Notify::new(),
            stopped: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }
}
#[async_trait]
impl CodexPort for PendingCodex {
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn request(&self, _: &str, _: Value) -> Result<Value> {
        self.started.notify_one();
        let stopped = self.stopped.notified();
        if !self.closed.load(Ordering::SeqCst) {
            stopped.await;
        }
        bail!("disconnected")
    }
    async fn respond(&self, _: Value, _: Value) -> Result<()> {
        Ok(())
    }
    async fn respond_error(&self, _: Value, _: i64, _: &str) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<RpcMessage> {
        self.events.subscribe()
    }
    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.stopped.notify_waiters();
        Ok(())
    }
}

#[tokio::test]
async fn failed_delivery_is_durable_and_reuses_identity_with_latest_token() -> Result<()> {
    let store = Store::memory()?;
    store.update(|s| {
        s.peers.insert("owner".into(), "old".into());
        enqueue(s, "owner", "reply");
        Ok(())
    })?;
    let channel = MockChannel {
        sent: Mutex::new(vec![]),
        fail: AtomicBool::new(true),
        expired: false,
    };
    let cancel = CancellationToken::new();
    deliver_next(&store, &channel, &cancel, 100).await?;
    assert_eq!(store.read()?.outbox[0].attempts, 1);
    assert_eq!(store.read()?.outbox[0].next_attempt_at, 2100);
    store.update(|s| {
        s.peers.insert("owner".into(), "new".into());
        Ok(())
    })?;
    assert!(!deliver_next(&store, &channel, &cancel, 200).await?);
    deliver_next(&store, &channel, &cancel, 10000).await?;
    assert!(store.read()?.outbox.is_empty());
    let sent = channel.sent.lock().unwrap();
    assert_eq!(sent[0].0, sent[1].0);
    assert_eq!(sent[1].1, "new");
    Ok(())
}

#[tokio::test]
async fn shutdown_closes_rpc_before_waiting_for_pending_input() -> Result<()> {
    let store = Arc::new(Store::memory()?);
    store.accept_batch(
        "1",
        vec![Inbound {
            id: "m".into(),
            peer: "owner".into(),
            text: "/new".into(),
            context_token: "ctx".into(),
            notice: None,
        }],
    )?;
    let codex = Arc::new(PendingCodex::new());
    let bridge = Arc::new(Bridge::new(
        store.clone(),
        codex.clone(),
        "/project".into(),
        "owner".into(),
    ));
    let channel = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
        fail: AtomicBool::new(false),
        expired: false,
    });
    let cancel = CancellationToken::new();
    let task = tokio::spawn(run_bridge(
        store,
        bridge,
        codex.clone(),
        channel,
        "owner".into(),
        cancel.clone(),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(2), codex.started.notified()).await?;
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), task).await???;
    assert!(codex.closed.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn expired_credentials_stop_all_loops_and_keep_pending_reply() -> Result<()> {
    let store = Arc::new(Store::memory()?);
    store.update(|s| {
        s.peers.insert("owner".into(), "ctx".into());
        enqueue(s, "owner", "keep this");
        Ok(())
    })?;
    let codex = Arc::new(PendingCodex::new());
    let bridge = Arc::new(Bridge::new(
        store.clone(),
        codex.clone(),
        "/project".into(),
        "owner".into(),
    ));
    let channel = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
        fail: AtomicBool::new(false),
        expired: true,
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        run_bridge(
            store.clone(),
            bridge,
            codex.clone(),
            channel,
            "owner".into(),
            CancellationToken::new(),
        ),
    )
    .await?;
    assert!(
        result
            .unwrap_err()
            .downcast_ref::<WechatApiError>()
            .is_some()
    );
    assert_eq!(store.read()?.outbox.len(), 1);
    assert!(codex.closed.load(Ordering::SeqCst));
    Ok(())
}

struct DelayedChannel {
    started: Notify,
    release: Notify,
    cancel: Mutex<Option<CancellationToken>>,
    finished: AtomicBool,
}
impl DelayedChannel {
    fn new() -> Self {
        Self {
            started: Notify::new(),
            release: Notify::new(),
            cancel: Mutex::new(None),
            finished: AtomicBool::new(false),
        }
    }
}
#[async_trait]
impl Channel for DelayedChannel {
    async fn poll(&self, _: &str, _: &CancellationToken) -> Result<PollBatch> {
        unreachable!()
    }
    async fn send(&self, _: &Outbound, _: &str, cancel: &CancellationToken) -> Result<()> {
        *self.cancel.lock().unwrap() = Some(cancel.clone());
        self.started.notify_one();
        self.release.notified().await;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn switching_cancels_pending_native_send_even_if_focus_immediately_returns() -> Result<()> {
    use codex_wechat::store::{enqueue_thread, select_thread};
    for switch_back in [false, true] {
        let store = Arc::new(Store::memory()?);
        store.update(|s| {
            select_thread(s, "owner", "A");
            s.peers.insert("owner".into(), "ctx".into());
            enqueue_thread(s, "owner", "A", "old body");
            Ok(())
        })?;
        let channel = Arc::new(DelayedChannel::new());
        let (task_store, task_channel) = (store.clone(), channel.clone());
        let delivery = tokio::spawn(async move {
            deliver_next(&task_store, &*task_channel, &CancellationToken::new(), 10).await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            channel.started.notified(),
        )
        .await?;
        store.update(|s| {
            select_thread(s, "owner", "B");
            enqueue(s, "owner", "Switched to: B");
            Ok(())
        })?;
        if switch_back {
            store.update(|s| {
                select_thread(s, "owner", "A");
                Ok(())
            })?;
        }
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), delivery).await???);
        assert!(
            channel
                .cancel
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_cancelled()
        );
        channel.release.notify_one();
        assert!(!channel.finished.load(Ordering::SeqCst));
        assert_eq!(store.read()?.outbox.len(), 1);
        assert_eq!(store.read()?.outbox[0].text, "Switched to: B");
    }
    Ok(())
}

#[tokio::test]
async fn another_peers_switch_does_not_cancel_selected_delivery() -> Result<()> {
    use codex_wechat::store::{enqueue_thread, select_thread};
    let store = Arc::new(Store::memory()?);
    store.update(|s| {
        select_thread(s, "owner", "A");
        s.peers.insert("owner".into(), "ctx".into());
        enqueue_thread(s, "owner", "A", "selected body");
        Ok(())
    })?;
    let channel = Arc::new(DelayedChannel::new());
    let (task_store, task_channel) = (store.clone(), channel.clone());
    let delivery = tokio::spawn(async move {
        deliver_next(&task_store, &*task_channel, &CancellationToken::new(), 10).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        channel.started.notified(),
    )
    .await?;
    store.update(|s| {
        select_thread(s, "someone-else", "B");
        Ok(())
    })?;
    tokio::task::yield_now().await;
    assert!(
        !channel
            .cancel
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    channel.release.notify_one();
    assert!(tokio::time::timeout(std::time::Duration::from_secs(1), delivery).await???);
    assert!(channel.finished.load(Ordering::SeqCst));
    assert!(store.read()?.outbox.is_empty());
    Ok(())
}

#[tokio::test]
async fn stale_scoped_retry_is_discarded_before_its_backoff_without_blocking_command_reply()
-> Result<()> {
    let store = Store::memory()?;
    store.update(|s| {
        s.bindings.insert("owner".into(), "B".into());
        s.peers.insert("owner".into(), "ctx".into());
        enqueue(s, "owner", "stale body");
        s.outbox[0].thread_id = Some("A".into());
        s.outbox[0].next_attempt_at = 60000;
        enqueue(s, "owner", "Switched to: B");
        Ok(())
    })?;
    let channel = MockChannel {
        sent: Mutex::new(vec![]),
        fail: AtomicBool::new(false),
        expired: false,
    };
    assert!(deliver_next(&store, &channel, &CancellationToken::new(), 10).await?);
    assert!(channel.sent.lock().unwrap().is_empty());
    assert!(deliver_next(&store, &channel, &CancellationToken::new(), 10).await?);
    assert_eq!(channel.sent.lock().unwrap().len(), 1);
    assert!(store.read()?.outbox.is_empty());
    Ok(())
}
