use anyhow::bail;
use codex_wechat::{
    models::{Inbound, State, ThreadChoice},
    store::{InstanceLock, Store, database_path, enqueue},
};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn transactions_dedup_and_persistence() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("bridge.sqlite");
    let store = Store::open(&file)?;
    let message = Inbound {
        id: "m".into(),
        peer: "owner".into(),
        text: "hi".into(),
        context_token: "old".into(),
        notice: None,
    };
    store.accept_batch("1", vec![message.clone()])?;
    store.update(|s| {
        s.inbox["m"].status = "done".into();
        enqueue(s, "owner", "  exact\n\ntext 😀  ");
        s.switch_choices.insert(
            "owner".into(),
            vec![ThreadChoice {
                thread_id: "native-id".into(),
                title: "Saved".into(),
            }],
        );
        Ok(())
    })?;
    store.accept_batch(
        "2",
        vec![Inbound {
            context_token: "new".into(),
            ..message
        }],
    )?;
    let result: anyhow::Result<()> = store.update(|s| {
        s.cursor = "bad".into();
        bail!("rollback")
    });
    assert!(result.is_err());
    assert_eq!(store.read()?.cursor, "2");
    let reader = Store::read_only(&file)?;
    assert_eq!(reader.read()?.peers["owner"], "new");
    drop(store);
    let reopened = Store::open(&file)?;
    let state = reopened.read()?;
    assert_eq!(state.inbox.len(), 1);
    assert_eq!(state.inbox["m"].status, "done");
    assert_eq!(state.outbox[0].text, "  exact\n\ntext 😀  ");
    assert_eq!(state.switch_choices["owner"][0].thread_id, "native-id");
    Ok(())
}

#[test]
fn typescript_v1_json_is_compatible() -> anyhow::Result<()> {
    let value = json!({"cursor":"old","peers":{},"bindings":{"owner":"thread"},"inbox":{"m":{"id":"m","peer":"owner","text":"hi","contextToken":"ctx","status":"done"}},"outbox":[{"id":"o","peer":"owner","text":"reply","attempts":1,"nextAttemptAt":17}],"runs":{"r":{"id":"r","threadId":"thread","turnId":"turn","peer":"owner","status":"running","startedAtMs":1000,"items":{"z":{"text":"first","queued":true},"a":{"text":"second","phase":"partial_answer","completed":true}}}},"interactions":{}});
    let state: State = serde_json::from_value(value)?;
    assert!(state.switch_choices.is_empty());
    assert_eq!(
        state.runs["r"].items.keys().cloned().collect::<Vec<_>>(),
        vec!["z", "a"]
    );
    assert_eq!(state.inbox["m"].message.context_token, "ctx");
    let encoded = serde_json::to_value(state)?;
    assert_eq!(encoded["outbox"][0]["nextAttemptAt"], 17);
    assert_eq!(encoded["runs"]["r"]["items"]["z"]["queued"], true);
    let _: HashMap<String, serde_json::Value> = serde_json::from_value(encoded)?;
    Ok(())
}

#[test]
fn instance_lock_and_namespaces() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("instance.sqlite");
    let first = InstanceLock::acquire(&file)?;
    assert!(InstanceLock::acquire(&file).is_err());
    drop(first);
    let _second = InstanceLock::acquire(&file)?;
    assert_eq!(
        database_path(dir.path(), "bot1"),
        database_path(dir.path(), "bot1")
    );
    assert_ne!(
        database_path(dir.path(), "bot1"),
        database_path(dir.path(), "bot2")
    );
    Ok(())
}

#[test]
fn selection_prunes_native_queue_and_scoped_enqueue_respects_binding() -> anyhow::Result<()> {
    use codex_wechat::store::{enqueue_thread, select_thread};
    let store = Store::memory()?;
    store.update(|s| {
        select_thread(s, "owner", "A");
        assert!(enqueue_thread(s, "owner", "A", "old output"));
        assert!(!enqueue_thread(s, "owner", "B", "muted"));
        enqueue(s, "owner", "command reply");
        select_thread(s, "owner", "B");
        assert!(enqueue_thread(s, "owner", "B", "new output"));
        assert_eq!(s.outbox.len(), 2);
        Ok(())
    })?;
    assert_eq!(store.read()?.outbox[1].thread_id.as_deref(), Some("B"));
    Ok(())
}

#[tokio::test]
async fn binding_notifications_only_follow_committed_binding_changes() -> anyhow::Result<()> {
    let store = Store::memory()?;
    let mut changed = store.subscribe_bindings();
    let failed: anyhow::Result<()> = store.update(|s| {
        s.bindings.insert("owner".into(), "A".into());
        bail!("rollback");
    });
    assert!(failed.is_err());
    assert!(!changed.has_changed()?);
    store.update(|s| {
        s.cursor = "no focus change".into();
        Ok(())
    })?;
    assert!(!changed.has_changed()?);
    store.update(|s| {
        s.bindings.insert("owner".into(), "B".into());
        Ok(())
    })?;
    changed.changed().await?;
    assert!(!changed.has_changed()?);
    Ok(())
}
