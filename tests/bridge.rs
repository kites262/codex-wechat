use anyhow::{Result, anyhow};
use async_trait::async_trait;
use codex_wechat::{
    bridge::Bridge,
    codex::CodexRpcError,
    models::{Inbound, InboxEntry, RpcMessage, Run},
    ports::CodexPort,
    store::Store,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, broadcast};

#[derive(Default)]
struct StartGate {
    entered: Notify,
    released: Notify,
}

type Handler = Arc<dyn Fn(&str, &Value) -> Result<Value> + Send + Sync>;
struct MockCodex {
    calls: Mutex<Vec<(String, Value)>>,
    responses: Mutex<Vec<(Value, Value)>>,
    errors: Mutex<Vec<(Value, i64, String)>>,
    handler: Mutex<Handler>,
    gate: Mutex<Option<Arc<StartGate>>>,
    settings_gate: Mutex<Option<Arc<StartGate>>>,
    events: broadcast::Sender<RpcMessage>,
}
fn default_response(method: &str, params: &Value) -> Result<Value> {
    Ok(match method {
        "thread/start" => {
            json!({"thread":{"id":"fresh","cwd":"/project","status":{"type":"idle"},"turns":[]}})
        }
        "thread/read" | "thread/resume" => {
            json!({"thread":{"id":params["threadId"],"cwd":"/project","status":{"type":"idle"},"turns":[]}})
        }
        "thread/turns/list" | "thread/items/list" => json!({"data":[]}),
        "turn/start" => json!({"turn":{"id":"turn-1"}}),
        _ => json!({}),
    })
}
impl MockCodex {
    fn new() -> Self {
        Self {
            calls: Mutex::new(vec![]),
            responses: Mutex::new(vec![]),
            errors: Mutex::new(vec![]),
            handler: Mutex::new(Arc::new(default_response)),
            gate: Mutex::new(None),
            settings_gate: Mutex::new(None),
            events: broadcast::channel(16).0,
        }
    }
    fn set(&self, f: impl Fn(&str, &Value) -> Result<Value> + Send + Sync + 'static) {
        *self.handler.lock().unwrap() = Arc::new(f);
    }
}
#[async_trait]
impl CodexPort for MockCodex {
    async fn start(&self) -> Result<()> {
        Ok(())
    }
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        self.calls
            .lock()
            .unwrap()
            .push((method.into(), params.clone()));
        let handler = self.handler.lock().unwrap().clone();
        let gate = self.gate.lock().unwrap().clone();
        if method == "turn/start"
            && let Some(gate) = gate
        {
            gate.entered.notify_one();
            gate.released.notified().await;
        }
        let settings_gate = self.settings_gate.lock().unwrap().clone();
        if method == "thread/settings/update"
            && let Some(gate) = settings_gate
        {
            gate.entered.notify_one();
            gate.released.notified().await;
        }
        handler(method, &params)
    }
    async fn respond(&self, id: Value, result: Value) -> Result<()> {
        self.responses.lock().unwrap().push((id, result));
        Ok(())
    }
    async fn respond_error(&self, id: Value, code: i64, message: &str) -> Result<()> {
        self.errors.lock().unwrap().push((id, code, message.into()));
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<RpcMessage> {
        self.events.subscribe()
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
struct Fixture {
    store: Arc<Store>,
    codex: Arc<MockCodex>,
    bridge: Bridge,
    counter: usize,
}
impl Fixture {
    fn new() -> Self {
        Self::with_store(Arc::new(Store::memory().unwrap()))
    }
    fn with_store(store: Arc<Store>) -> Self {
        let codex = Arc::new(MockCodex::new());
        let bridge = Bridge::new(
            store.clone(),
            codex.clone(),
            PathBuf::from("/project"),
            "alice".into(),
        );
        Self {
            store,
            codex,
            bridge,
            counter: 0,
        }
    }
    async fn input(&mut self, text: &str) {
        self.input_peer(text, "alice", None).await;
    }
    async fn input_peer(&mut self, text: &str, peer: &str, notice: Option<&str>) {
        self.counter += 1;
        let id = self.counter.to_string();
        self.store
            .accept_batch(
                &id,
                vec![Inbound {
                    id: id.clone(),
                    peer: peer.into(),
                    text: text.into(),
                    context_token: "token".into(),
                    notice: notice.map(str::to_owned),
                }],
            )
            .unwrap();
        let entry = self.store.read().unwrap().inbox[&id].clone();
        self.bridge.handle(entry).await.unwrap();
    }
    fn bind(&self, id: &str) {
        self.store
            .update(|s| {
                s.bindings.insert("alice".into(), id.into());
                Ok(())
            })
            .unwrap();
    }
    fn output(&self) -> Vec<String> {
        self.store
            .read()
            .unwrap()
            .outbox
            .into_iter()
            .map(|v| v.text)
            .collect()
    }
    fn clear(&self) {
        self.store
            .update(|s| {
                s.outbox.clear();
                Ok(())
            })
            .unwrap();
    }
    fn calls(&self, method: &str) -> Vec<Value> {
        self.codex
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == method)
            .map(|(_, p)| p.clone())
            .collect()
    }
    fn runs(&self) -> Vec<Run> {
        self.store.read().unwrap().runs.into_values().collect()
    }
    async fn event(&self, method: &str, params: Value) {
        self.bridge
            .on_codex(RpcMessage {
                id: None,
                method: method.into(),
                params,
            })
            .await
            .unwrap();
    }
    async fn item(&self, id: &str, text: &str, phase: Option<&str>) {
        self.event("item/completed",json!({"threadId":"A","turnId":"turn-1","item":{"type":"agentMessage","id":id,"text":text,"phase":phase}})).await;
    }
    async fn done(&self, turn: Value) {
        self.event("turn/completed", json!({"threadId":"A","turn":turn}))
            .await;
    }
    async fn request(&self, id: i64, method: &str, params: Value) -> String {
        self.bridge
            .on_codex(RpcMessage {
                id: Some(json!(id)),
                method: method.into(),
                params,
            })
            .await
            .unwrap();
        self.store
            .read()
            .unwrap()
            .interactions
            .values()
            .find(|r| r.rpc_id == json!(id))
            .map(|r| r.code.clone())
            .unwrap_or_default()
    }
}
fn rpc_error(code: i64) -> anyhow::Error {
    CodexRpcError {
        code,
        message: "native rejected".into(),
        data: None,
    }
    .into()
}

fn effort_response(method: &str, params: &Value) -> Result<Value> {
    match method {
        "thread/read" | "thread/resume" => Ok(json!({
            "thread": {
                "id": params["threadId"], "cwd": "/project",
                "model": "current-model", "reasoningEffort": "low",
                "status": {"type": "idle"}, "turns": []
            }
        })),
        "model/list" => Ok(json!({"data": [{
            "id": "current-model-id", "model": "current-model",
            "supportedReasoningEfforts": [
                {"reasoningEffort": "low"}, {"reasoningEffort": "high"}
            ]
        }]})),
        _ => default_response(method, params),
    }
}

fn settings_event(thread_id: &str, effort: &str) -> RpcMessage {
    RpcMessage {
        id: None,
        method: "thread/settings/updated".into(),
        params: json!({
            "threadId": thread_id,
            "threadSettings": {"model": "current-model", "effort": effort}
        }),
    }
}

fn pending_input(f: &Fixture, id: &str, text: &str) -> InboxEntry {
    f.store
        .accept_batch(
            id,
            vec![Inbound {
                id: id.into(),
                peer: "alice".into(),
                text: text.into(),
                context_token: "token".into(),
                notice: None,
            }],
        )
        .unwrap();
    f.store.read().unwrap().inbox[id].clone()
}

#[tokio::test]
async fn effort_lists_current_models_paginated_native_levels_with_current_marker() {
    let mut f = Fixture::new();
    f.bind("A");
    f.codex.set(|method, params| {
        if method != "model/list" {
            return effort_response(method, params);
        }
        assert_eq!(params["includeHidden"], true);
        assert_eq!(params["limit"], 100);
        if params["cursor"].is_null() {
            Ok(json!({"data": [{
                "id": "other", "model": "other-model",
                "supportedReasoningEfforts": [{"reasoningEffort": "medium"}]
            }], "nextCursor": "second"}))
        } else {
            assert_eq!(params["cursor"], "second");
            Ok(json!({"data": [{
                "id": "current-model-id", "model": "current-model", "hidden": true,
                "supportedReasoningEfforts": [
                    {"reasoningEffort": "low"}, {"reasoningEffort": "high"},
                    {"reasoningEffort": "ultra"}, {"reasoningEffort": "high"}
                ]
            }]}))
        }
    });
    f.input("/effort").await;
    assert_eq!(
        f.output(),
        vec![
            "Reasoning effort (current-model):\n\nlow *\n\nhigh\n\nultra\n\n* current\n\n/effort <level>"
        ]
    );
    assert_eq!(f.calls("model/list").len(), 2);
    assert!(f.calls("thread/resume").is_empty());
    assert!(f.calls("thread/settings/update").is_empty());
    assert_eq!(
        f.store.read().unwrap().outbox[0].thread_id.as_deref(),
        Some("A")
    );
}

#[tokio::test]
async fn effort_requires_a_selection_single_argument_and_exact_advertised_level() {
    let mut f = Fixture::new();
    for command in ["/effort", "/effort high", "/effort low high"] {
        f.input(command).await;
    }
    assert!(f.codex.calls.lock().unwrap().is_empty());
    assert!(f.output()[0].contains("Use /new"));
    assert!(f.output()[1].contains("Use /new"));
    assert_eq!(f.output()[2], "Request failed: Usage: /effort [level]");
    f.clear();
    f.bind("A");
    f.codex.set(effort_response);
    for command in ["/effort HIGH", "/effort medium", "/effort ultra"] {
        f.input(command).await;
    }
    assert!(
        f.output()
            .iter()
            .all(|text| text.contains("Supported: low, high."))
    );
    assert!(f.calls("thread/settings/update").is_empty());
    assert!(f.calls("turn/start").is_empty());
    assert!(
        f.store
            .read()
            .unwrap()
            .inbox
            .values()
            .all(|entry| entry.status == "done")
    );
}

#[tokio::test]
async fn effort_rejects_missing_or_invalid_catalog_without_mutation() {
    for page in [
        json!({"data": []}),
        json!({"data": [{"model": "current-model", "supportedReasoningEfforts": []}]}),
        json!({"data": [{"model": "current-model", "supportedReasoningEfforts": [{}]}]}),
        json!({"data": [] , "nextCursor": "repeated"}),
    ] {
        let mut f = Fixture::new();
        f.bind("A");
        f.codex.set(move |method, params| {
            if method == "model/list" {
                Ok(page.clone())
            } else {
                effort_response(method, params)
            }
        });
        f.input("/effort high").await;
        assert!(f.output()[0].starts_with("Request failed:"));
        assert!(f.calls("model/list").len() <= 2);
        assert!(f.calls("thread/settings/update").is_empty());
        assert_eq!(f.store.read().unwrap().inbox["1"].status, "done");
    }
}

#[tokio::test]
async fn effort_same_native_value_is_a_noop_without_waiting_for_a_change_event() {
    let mut f = Fixture::new();
    f.bind("A");
    f.codex.set(effort_response);
    f.input("/effort low").await;
    assert_eq!(f.output(), vec!["Effort is already low."]);
    assert!(f.calls("thread/settings/update").is_empty());
    assert_eq!(f.store.read().unwrap().inbox["1"].status, "done");
}

#[tokio::test]
async fn effort_retains_confirmation_before_ack_and_only_sends_native_effort_override() {
    let mut f = Fixture::new();
    f.bind("A");
    let events = f.codex.events.clone();
    f.codex.set(move |method, params| {
        if method == "thread/settings/update" {
            events.send(settings_event("A", "high")).unwrap();
        }
        effort_response(method, params)
    });
    f.input("/effort high").await;
    assert_eq!(
        f.calls("thread/settings/update"),
        vec![json!({"threadId": "A", "effort": "high"})]
    );
    assert!(f.calls("turn/start").is_empty());
    assert_eq!(f.output(), vec!["Effort set to high for subsequent turns."]);
    assert_eq!(
        f.store.read().unwrap().outbox[0].thread_id.as_deref(),
        Some("A")
    );
}

#[tokio::test]
async fn effort_waits_past_ack_and_unrelated_events_until_matching_native_confirmation() {
    let f = Fixture::new();
    f.bind("A");
    f.codex.set(effort_response);
    let gate = Arc::new(StartGate::default());
    *f.codex.settings_gate.lock().unwrap() = Some(gate.clone());
    let request = f.bridge.handle(pending_input(&f, "effort", "/effort high"));
    tokio::pin!(request);
    tokio::select! {
        result = &mut request => panic!("Completed before native ACK: {result:?}"),
        () = gate.entered.notified() => {}
    }
    gate.released.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    f.codex.events.send(settings_event("B", "high")).unwrap();
    f.codex.events.send(settings_event("A", "low")).unwrap();
    let mut request_shaped_event = settings_event("A", "high");
    request_shaped_event.id = Some(json!(99));
    f.codex.events.send(request_shaped_event).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    assert!(f.output().is_empty());
    assert_eq!(f.store.read().unwrap().inbox["effort"].status, "processing");
    f.codex.events.send(settings_event("A", "high")).unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.output(), vec!["Effort set to high for subsequent turns."]);
    assert_eq!(f.calls("thread/settings/update").len(), 1);
}

#[tokio::test]
async fn effort_native_rejection_is_final_but_lost_confirmation_is_unknown_and_never_replayed() {
    for mode in ["rejected", "transport", "disconnected", "lagged"] {
        let f = Fixture::new();
        f.bind("A");
        let events = f.codex.events.clone();
        f.codex.set(move |method, params| {
            if method == "thread/settings/update" {
                match mode {
                    "rejected" => return Err(rpc_error(-32602)),
                    "transport" => return Err(anyhow!("connection lost")),
                    "disconnected" => {
                        events
                            .send(RpcMessage {
                                id: None,
                                method: "bridge/disconnected".into(),
                                params: json!({}),
                            })
                            .unwrap();
                    }
                    "lagged" => {
                        for _ in 0..17 {
                            events.send(settings_event("B", "high")).unwrap();
                        }
                    }
                    _ => unreachable!(),
                }
            }
            effort_response(method, params)
        });
        let entry = pending_input(&f, "effort", "/effort high");
        f.bridge.handle(entry.clone()).await.unwrap();
        let state = f.store.read().unwrap();
        let output = &state.outbox[0].text;
        if mode == "rejected" {
            assert_eq!(state.inbox["effort"].status, "done");
            assert_eq!(output, "Request failed: native rejected");
        } else {
            assert_eq!(state.inbox["effort"].status, "unknown");
            assert!(output.starts_with("Outcome unknown:"));
            assert!(output.contains("Use /effort to check the current setting."));
            assert!(output.contains("This request will not be replayed."));
        }
        f.bridge.recover().unwrap();
        f.bridge.handle(entry).await.unwrap();
        assert_eq!(f.calls("thread/settings/update").len(), 1);
        assert_eq!(f.output().len(), 1);
    }
}

#[tokio::test]
async fn effort_confirmation_timeout_is_unknown_without_retrying_the_update() {
    let f = Fixture::new();
    f.bind("A");
    f.codex.set(effort_response);
    let entry = pending_input(&f, "effort", "/effort high");
    f.bridge.handle(entry.clone()).await.unwrap();
    assert_eq!(f.store.read().unwrap().inbox["effort"].status, "unknown");
    assert!(f.output()[0].contains("Timed out confirming the effort change."));
    f.bridge.handle(entry).await.unwrap();
    assert_eq!(f.calls("thread/settings/update").len(), 1);
}

#[tokio::test]
async fn effort_completion_and_errors_remain_scoped_to_the_original_selected_thread() {
    for mode in ["confirmed", "rejected", "disconnected"] {
        let f = Fixture::new();
        f.bind("A");
        let events = f.codex.events.clone();
        f.codex.set(move |method, params| {
            if method == "thread/settings/update" {
                match mode {
                    "confirmed" => {
                        events.send(settings_event("A", "high")).unwrap();
                    }
                    "rejected" => return Err(rpc_error(-32602)),
                    "disconnected" => {
                        events
                            .send(RpcMessage {
                                id: None,
                                method: "bridge/disconnected".into(),
                                params: json!({}),
                            })
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            effort_response(method, params)
        });
        let gate = Arc::new(StartGate::default());
        *f.codex.settings_gate.lock().unwrap() = Some(gate.clone());
        let request = f.bridge.handle(pending_input(&f, "effort", "/effort high"));
        tokio::pin!(request);
        tokio::select! {
            result = &mut request => panic!("Completed before selection changed: {result:?}"),
            () = gate.entered.notified() => {}
        }
        assert_eq!(
            f.store.read().unwrap().inbox["effort"].thread_id.as_deref(),
            Some("A")
        );
        f.store
            .update(|state| {
                codex_wechat::store::select_thread(state, "alice", "B");
                Ok(())
            })
            .unwrap();
        gate.released.notify_one();
        tokio::time::timeout(Duration::from_secs(1), &mut request)
            .await
            .unwrap()
            .unwrap();
        assert!(f.output().is_empty());
        assert_eq!(f.store.read().unwrap().bindings["alice"], "B");
        assert_eq!(
            f.calls("thread/settings/update"),
            vec![json!({"threadId": "A", "effort": "high"})]
        );
        assert_eq!(
            f.store.read().unwrap().inbox["effort"].status,
            if mode == "disconnected" {
                "unknown"
            } else {
                "done"
            }
        );
    }
}

#[tokio::test]
async fn new_uses_only_cwd_and_failed_or_foreign_switch_preserves_binding() {
    let mut f = Fixture::new();
    f.input("/new").await;
    assert_eq!(f.calls("thread/start"), vec![json!({"cwd":"/project"})]);
    assert_eq!(f.store.read().unwrap().bindings["alice"], "fresh");
    f.codex.set(|m, p| {
        if m == "thread/resume" {
            Err(rpc_error(-32603))
        } else {
            default_response(m, p)
        }
    });
    f.input("/switch B").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "fresh");
    f.codex.calls.lock().unwrap().clear();
    f.codex.set(|m, p| {
        if m == "thread/read" {
            Ok(json!({"thread":{"id":p["threadId"],"cwd":"/elsewhere"}}))
        } else {
            default_response(m, p)
        }
    });
    f.input("/switch foreign").await;
    assert!(f.calls("thread/resume").is_empty());
}
#[tokio::test]
async fn list_and_switch_have_separate_required_arguments_and_preserve_saved_choices() {
    let mut f = Fixture::new();
    f.bind("A");
    f.codex.set(|method, params| {
        if method == "thread/list" {
            Ok(json!({"data":[{"id":"B","cwd":"/project","name":"Recent title"}]}))
        } else {
            default_response(method, params)
        }
    });
    f.input("/list").await;
    assert!(f.output().join("\n").contains("1. Recent title"));
    let choices = serde_json::to_value(f.store.read().unwrap().switch_choices).unwrap();
    f.codex.calls.lock().unwrap().clear();
    for command in ["/switch", "/switch 1 2", "/list extra"] {
        f.input(command).await;
    }
    assert!(f.codex.calls.lock().unwrap().is_empty());
    assert_eq!(f.store.read().unwrap().bindings["alice"], "A");
    assert_eq!(
        serde_json::to_value(f.store.read().unwrap().switch_choices).unwrap(),
        choices
    );
    assert!(
        f.output()
            .join("\n")
            .contains("Usage: /switch <1-10 | threadId>")
    );
    assert!(f.output().join("\n").contains("Usage: /list"));
    f.input("/help").await;
    assert_eq!(
        f.output().last().unwrap().split("\n\n").collect::<Vec<_>>(),
        vec![
            "/new",
            "/list",
            "/switch <1-10 | threadId>",
            "/effort [level]",
            "/status",
            "/stop [threadId]",
            "/approve <code>",
            "/reject <code>",
            "/answer <code> <content>",
        ],
    );
    f.input("/switch 1").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "B");
}

#[tokio::test]
async fn removed_commands_are_unknown_and_do_not_call_native_codex_or_change_selection() {
    let mut f = Fixture::new();
    f.bind("A");
    for command in ["/sessions", "/sessions extra", "/recover", "/recover A"] {
        f.input(command).await;
    }
    assert!(f.codex.calls.lock().unwrap().is_empty());
    assert_eq!(f.store.read().unwrap().bindings["alice"], "A");
    assert_eq!(f.output().len(), 4);
    assert!(
        f.output()
            .iter()
            .all(|message| message == "Request failed: Unknown command. Use /help for commands.")
    );
    f.input("/help").await;
    let help = f.output().last().unwrap().clone();
    assert!(!help.contains("/sessions"));
    assert!(!help.contains("/recover"));
    assert!(help.contains("/answer <code> <content>"));
}

#[tokio::test]
async fn switch_automatically_reconciles_unknown_turn_without_replaying_muted_history() {
    let mut f = Fixture::new();
    f.bind("B");
    f.store
        .update(|s| {
            s.runs.insert("saved".into(), unknown_run());
            Ok(())
        })
        .unwrap();
    f.codex.set(|method, params| match method {
        "thread/turns/list" => Ok(json!({"data":[{"id":"old","status":"completed","durationMs":1000}]})),
        "thread/items/list" => Ok(json!({"data":[{"turnId":"old","item":{"type":"agentMessage","id":"missed","text":"muted history"}}]})),
        _ => default_response(method, params),
    });
    f.input("/switch A").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "A");
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.runs()[0].items["missed"].queued);
    assert_eq!(f.output(), vec!["Switched to: A"]);
    assert_eq!(f.calls("thread/turns/list").len(), 1);
    assert_eq!(f.calls("thread/items/list").len(), 1);
    assert!(f.calls("turn/start").is_empty());
    f.bridge.connected().await.unwrap();
    assert_eq!(f.output(), vec!["Switched to: A"]);
    assert_eq!(f.calls("thread/items/list").len(), 1);
    f.input("next task").await;
    assert_eq!(f.calls("turn/start").len(), 1);
}

#[tokio::test]
async fn failed_switch_history_reconciliation_preserves_existing_selection_and_output() {
    let mut f = Fixture::new();
    f.bind("B");
    f.store
        .update(|s| {
            s.runs.insert("saved".into(), unknown_run());
            Ok(())
        })
        .unwrap();
    f.codex.set(|method, params| match method {
        "thread/turns/list" => Err(rpc_error(-32603)),
        _ => default_response(method, params),
    });
    f.input("/switch A").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "B");
    assert_eq!(f.runs()[0].status, "unknown");
    assert_eq!(f.output(), vec!["Request failed: native rejected"]);
    assert!(f.calls("turn/start").is_empty());
}

#[tokio::test]
async fn numbered_switch_uses_persisted_snapshot_scoped_pagination_and_blank_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let mut f = Fixture::with_store(Arc::new(Store::open(&path).unwrap()));
    f.bind("T3");
    f.codex.set(|m, p| {
        if m == "thread/list" {
            let start = if p["cursor"].is_null() { 1 } else { 9 };
            let end = if start == 1 { 8 } else { 12 };
            let mut data = (start..=end)
                .map(
                    |i| json!({"id":format!("T{i}"),"cwd":"/project","name":format!("Title\n{i}")}),
                )
                .collect::<Vec<_>>();
            if start == 1 {
                data.push(json!({"id":"T1","cwd":"/project"}));
                data.push(json!({"id":"foreign","cwd":"/other"}));
            }
            Ok(json!({"data":data,"nextCursor":if start==1{Some("next")}else{None}}))
        } else {
            default_response(m, p)
        }
    });
    f.input("/list").await;
    let output = f.output().join("\n");
    assert!(output.contains("1. Title 1\n\n2. Title 2"));
    assert!(output.contains("3. Title 3 *"));
    assert!(!output.contains("T1"));
    assert!(!output.contains("foreign"));
    assert_eq!(
        f.calls("thread/list")
            .iter()
            .map(|p| p["limit"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![10, 2]
    );
    drop(f);
    let mut f = Fixture::with_store(Arc::new(Store::open(&path).unwrap()));
    f.counter = 10;
    f.input("/status").await;
    f.input("/switch 2").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "T2");
    assert!(f.output().contains(&"Switched to: Title 2".into()));
    for n in ["0", "11", "9999999999999999999999999999"] {
        f.input(&format!("/switch {n}")).await;
    }
    assert_eq!(f.calls("thread/resume").len(), 1);
    f.codex.set(|m, p| {
        if m == "thread/list" {
            Ok(json!({"data":[]}))
        } else {
            default_response(m, p)
        }
    });
    f.input("/list").await;
    assert!(f.store.read().unwrap().switch_choices["alice"].is_empty());
    f.input("/switch 1").await;
    assert_eq!(f.calls("thread/resume").len(), 1);
}
#[tokio::test]
async fn listing_detects_repeated_cursor_without_overwriting_previous_choices() {
    let mut f = Fixture::new();
    f.codex
        .set(|_, _| Ok(json!({"data":[],"nextCursor":"loop"})));
    f.input("/list").await;
    assert_eq!(f.calls("thread/list").len(), 2);
    assert!(f.output().join("").contains("repeated thread cursor"));
}
#[tokio::test]
async fn switched_away_run_finishes_silently_and_busy_inputs_are_never_submitted() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.input("second").await;
    assert_eq!(f.calls("turn/start").len(), 1);
    f.input("/switch B").await;
    f.clear();
    f.item("x", " native body ", Some("final_answer")).await;
    f.done(json!({"id":"turn-1","status":"completed","durationMs":30100}))
        .await;
    assert!(f.output().is_empty());
    assert_eq!(f.runs()[0].thread_id, "A");
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.runs()[0].items["x"].queued);
    assert!(f.calls("turn/interrupt").is_empty());
}

#[tokio::test]
async fn switching_prunes_old_pending_body_and_prompt_but_keeps_command_replies() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.item("old", "old pending body", Some("commentary")).await;
    let code = f
        .request(
            10,
            "item/fileChange/requestApproval",
            json!({"threadId":"A","turnId":"turn-1"}),
        )
        .await;
    f.input("/help").await;
    assert_eq!(f.output().len(), 3);
    f.input("/switch B").await;
    assert_eq!(f.output().len(), 2);
    assert!(f.output()[0].starts_with("/new\n\n/list"));
    assert_eq!(f.output()[1], "Switched to: B");
    assert!(!f.store.read().unwrap().interactions.contains_key(&code));
    assert!(f.codex.responses.lock().unwrap().is_empty());
    assert!(f.codex.errors.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_switch_preserves_pending_output_and_interactions() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.item("body", "keep this body", None).await;
    let code = f
        .request(
            10,
            "item/fileChange/requestApproval",
            json!({"threadId":"A","turnId":"turn-1"}),
        )
        .await;
    f.codex.set(|m, p| match m {
        "thread/resume" => {
            Ok(json!({"thread":{"id":p["threadId"],"cwd":"/project","status":{"type":"active"}}}))
        }
        "thread/turns/list" => Err(rpc_error(-32603)),
        _ => default_response(m, p),
    });
    f.input("/switch B").await;
    assert_eq!(f.store.read().unwrap().bindings["alice"], "A");
    assert!(f.output().contains(&"keep this body".into()));
    assert!(f.store.read().unwrap().interactions.contains_key(&code));
    f.input(&format!("/approve {code}")).await;
    assert_eq!(
        *f.codex.responses.lock().unwrap(),
        vec![(json!(10), json!({"decision":"accept"}))]
    );
}

#[tokio::test]
async fn switch_back_delivers_future_messages_without_replaying_muted_items() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.input("/switch B").await;
    f.item("muted", "while away", Some("commentary")).await;
    assert!(f.runs()[0].items["muted"].queued);
    f.input("/switch A").await;
    f.clear();
    f.item("muted", "while away", Some("commentary")).await;
    f.item("future", "welcome back", Some("final_answer")).await;
    f.done(json!({"id":"turn-1","status":"completed","durationMs":1000,"items":[{"id":"muted","type":"agentMessage","text":"while away"}]})).await;
    assert_eq!(f.output(), vec!["welcome back", "[done in 1.0s]"]);
}

#[tokio::test]
async fn new_thread_drops_old_pending_output_without_interrupting_old_task() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.item("body", "old body", None).await;
    f.input("/new").await;
    assert_eq!(f.output(), vec!["New thread: fresh"]);
    f.done(json!({"id":"turn-1","status":"completed"})).await;
    assert_eq!(f.output(), vec!["New thread: fresh"]);
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.calls("turn/interrupt").is_empty());
}

#[tokio::test]
async fn disconnection_and_subscription_failure_of_unselected_tasks_are_silent() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.input("/switch B").await;
    f.clear();
    f.bridge.recover().unwrap();
    assert!(f.output().is_empty());
    assert_eq!(f.runs()[0].status, "unknown");
    f.codex.set(|m, p| {
        if m == "thread/resume" && p["threadId"] == "A" {
            Err(rpc_error(-32603))
        } else {
            default_response(m, p)
        }
    });
    f.bridge.connected().await.unwrap();
    assert!(f.output().is_empty());
    f.bind("A");
    f.bridge.connected().await.unwrap();
    assert!(f.output().join("").contains("Subscription failed"));
}

#[tokio::test]
async fn delayed_submission_failure_after_switch_is_scoped_to_its_original_thread() {
    let f = Fixture::new();
    f.bind("A");
    let gate = Arc::new(StartGate::default());
    *f.codex.gate.lock().unwrap() = Some(gate.clone());
    f.codex.set(|method, params| {
        if method == "turn/start" {
            Err(anyhow!("connection lost"))
        } else {
            default_response(method, params)
        }
    });
    f.store
        .accept_batch(
            "batch",
            vec![
                Inbound {
                    id: "1".into(),
                    peer: "alice".into(),
                    text: "hello".into(),
                    context_token: "token".into(),
                    notice: None,
                },
                Inbound {
                    id: "2".into(),
                    peer: "alice".into(),
                    text: "/switch B".into(),
                    context_token: "token".into(),
                    notice: None,
                },
            ],
        )
        .unwrap();
    let first = f.store.read().unwrap().inbox["1"].clone();
    let switch = f.store.read().unwrap().inbox["2"].clone();
    let (result, ()) = tokio::join!(f.bridge.handle(first), async {
        gate.entered.notified().await;
        assert_eq!(
            f.store.read().unwrap().inbox["1"].thread_id.as_deref(),
            Some("A")
        );
        f.bridge.handle(switch).await.unwrap();
        f.clear();
        f.bridge.recover().unwrap();
        assert!(f.output().is_empty());
        gate.released.notify_one();
    });
    result.unwrap();
    assert!(f.output().is_empty());
    assert_eq!(f.store.read().unwrap().inbox["1"].status, "unknown");
    assert_eq!(f.runs()[0].status, "unknown");
}
#[tokio::test]
async fn all_phases_are_immediate_authoritative_and_final_does_not_end_turn() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.event(
        "item/agentMessage/delta",
        json!({"threadId":"A","turnId":"turn-1","itemId":"a","delta":"wrong partial"}),
    )
    .await;
    f.item("a", "  commentary\n", Some("commentary")).await;
    f.item("b", "partial", Some("partial_answer")).await;
    f.item("c", "final", Some("final_answer")).await;
    f.item("d", "legacy", None).await;
    f.item("a", "duplicate", Some("commentary")).await;
    assert_eq!(
        f.output(),
        vec!["  commentary\n", "partial", "final", "legacy"]
    );
    assert_eq!(f.runs()[0].status, "running");
    f.done(json!({"id":"turn-1","status":"completed","durationMs":0}))
        .await;
    assert_eq!(f.output().last().unwrap(), "[done in 0.0s]");
}
#[tokio::test]
async fn terminal_full_items_fill_gaps_but_summary_cannot_replace_buffered_text() {
    for view in ["full", "summary", "notLoaded"] {
        let mut f = Fixture::new();
        f.bind("A");
        f.input("hello").await;
        f.event(
            "item/agentMessage/delta",
            json!({"threadId":"A","turnId":"turn-1","itemId":"x","delta":"buffered"}),
        )
        .await;
        f.done(json!({"id":"turn-1","status":"interrupted","durationMs":65200,"itemsView":view,"items":[{"type":"agentMessage","id":"x","text":"full body"}]})).await;
        assert_eq!(
            f.output(),
            vec![
                if view == "full" {
                    "full body"
                } else {
                    "buffered"
                },
                "[stopped in 1m5.2s]"
            ]
        );
        f.done(json!({"id":"turn-1","status":"completed"})).await;
        assert_eq!(f.output().len(), 2);
    }
}
#[tokio::test]
async fn duration_native_priority_unix_timestamps_failure_and_rounding() {
    for (turn, expected) in [
        (
            json!({"startedAt":10,"completedAt":99,"durationMs":0}),
            "[done in 0.0s]",
        ),
        (
            json!({"startedAt":10,"completedAt":40.1}),
            "[done in 30.1s]",
        ),
        (json!({"durationMs":119999}), "[done in 2m0.0s]"),
        (
            json!({"status":"failed","durationMs":1200,"error":{"message":"boom"}}),
            "[failed in 1.2s]\nboom",
        ),
    ] {
        let mut f = Fixture::new();
        f.bind("A");
        f.input("hello").await;
        f.item("a", "body", None).await;
        let mut turn = turn;
        turn["id"] = json!("turn-1");
        f.done(turn).await;
        assert_eq!(f.output(), vec!["body", expected]);
    }
}
#[tokio::test]
async fn external_cli_turns_only_on_validated_subscribed_threads_are_adopted() {
    let mut f = Fixture::new();
    f.bind("A");
    f.bridge.connected().await.unwrap();
    f.event(
        "turn/started",
        json!({"threadId":"foreign","turn":{"id":"other"}}),
    )
    .await;
    assert!(f.runs().is_empty());
    f.event(
        "turn/started",
        json!({"threadId":"A","turn":{"id":"turn-1","startedAt":10}}),
    )
    .await;
    f.input("/switch B").await;
    f.clear();
    f.item("a", "from CLI", None).await;
    f.done(json!({"id":"turn-1","status":"completed","durationMs":10}))
        .await;
    assert!(f.output().is_empty());
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.calls("turn/start").is_empty());
}

#[tokio::test]
async fn hidden_cli_turn_on_visited_thread_keeps_muted_items_consumed_after_switch_back() {
    let mut f = Fixture::new();
    f.bind("A");
    f.bridge.connected().await.unwrap();
    f.input("/switch B").await;
    f.clear();
    f.event(
        "turn/started",
        json!({"threadId":"A","turn":{"id":"turn-1","startedAt":10}}),
    )
    .await;
    assert_eq!(f.runs()[0].status, "running");
    f.item("hidden", "before selection", Some("commentary"))
        .await;
    assert!(f.output().is_empty());
    assert!(f.runs()[0].items["hidden"].queued);
    f.input("/switch A").await;
    f.clear();
    f.item("future", "after selection", Some("final_answer"))
        .await;
    f.done(
        json!({"id":"turn-1","status":"completed","durationMs":1000,"itemsView":"full","items":[
            {"type":"agentMessage","id":"hidden","text":"before selection","phase":"commentary"},
            {"type":"agentMessage","id":"future","text":"after selection","phase":"final_answer"}
        ]}),
    )
    .await;
    assert_eq!(f.output(), vec!["after selection", "[done in 1.0s]"]);
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.calls("turn/start").is_empty());
    assert!(f.calls("turn/interrupt").is_empty());
}
#[tokio::test]
async fn runtime_active_is_adopted_but_loaded_idle_beats_stale_history() {
    for status in ["active", "idle"] {
        let mut f = Fixture::new();
        f.bind("A");
        f.codex.set(move |m, p| match m {
            "thread/resume" => {
                Ok(json!({"thread":{"id":p["threadId"],"cwd":"/project","status":{"type":status}}}))
            }
            "thread/turns/list" => Ok(json!({"data":[{"id":"native","status":"inProgress"}]})),
            _ => default_response(m, p),
        });
        f.input("hello").await;
        if status == "active" {
            assert!(f.calls("turn/start").is_empty());
            assert_eq!(f.runs()[0].turn_id.as_deref(), Some("native"));
        } else {
            assert_eq!(f.calls("turn/start").len(), 1);
            assert!(f.calls("thread/turns/list").is_empty());
        }
    }
}
#[tokio::test]
async fn switch_expires_old_codes_without_answering_native_requests() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    let code1 = f
        .request(
            10,
            "item/commandExecution/requestApproval",
            json!({"threadId":"A","turnId":"turn-1","command":"ls"}),
        )
        .await;
    let code2 = f
        .request(
            11,
            "item/fileChange/requestApproval",
            json!({"threadId":"A","turnId":"turn-1","reason":"edit"}),
        )
        .await;
    f.input("/switch B").await;
    f.input(&format!("/approve {code2}")).await;
    f.input(&format!("/reject {code1}")).await;
    assert!(f.codex.responses.lock().unwrap().is_empty());
    assert!(f.store.read().unwrap().interactions.is_empty());
    f.clear();
    assert!(
        f.request(
            13,
            "item/fileChange/requestApproval",
            json!({"threadId":"A","turnId":"turn-1"})
        )
        .await
        .is_empty()
    );
    f.request(14, "unsupported/request", json!({"threadId":"A"}))
        .await;
    assert!(f.output().is_empty());
    let code = f
        .request(
            12,
            "item/fileChange/requestApproval",
            json!({"threadId":"B"}),
        )
        .await;
    f.event(
        "serverRequest/resolved",
        json!({"threadId":"B","requestId":12}),
    )
    .await;
    f.input(&format!("/approve {code}")).await;
    assert!(f.codex.responses.lock().unwrap().is_empty());
    assert!(
        f.request(
            20,
            "item/fileChange/requestApproval",
            json!({"threadId":"unrelated"})
        )
        .await
        .is_empty()
    );
    assert!(f.codex.errors.lock().unwrap().is_empty());
}
#[tokio::test]
async fn native_decisions_are_respected_and_persistent_only_offers_remain_native() {
    let mut f = Fixture::new();
    f.bind("A");
    let code = f
        .request(
            1,
            "item/commandExecution/requestApproval",
            json!({"threadId":"A","availableDecisions":["cancel"]}),
        )
        .await;
    f.input(&format!("/approve {code}")).await;
    assert!(f.codex.responses.lock().unwrap().is_empty());
    f.input(&format!("/reject {code}")).await;
    assert_eq!(
        f.codex.responses.lock().unwrap()[0].1,
        json!({"decision":"cancel"})
    );
    assert!(
        f.request(
            2,
            "item/commandExecution/requestApproval",
            json!({"threadId":"A","availableDecisions":["acceptForSession"]})
        )
        .await
        .is_empty()
    );
    assert!(f.codex.errors.lock().unwrap().is_empty());
}
#[tokio::test]
async fn permission_payload_grants_only_known_requested_fields_for_one_turn() {
    let mut f = Fixture::new();
    f.bind("A");
    let code=f.request(1,"item/permissions/requestApproval",json!({"threadId":"A","permissions":{"network":{"enabled":true,"other":1},"fileSystem":{"read":["/a"],"write":[3],"entries":[{"access":"read","path":{"type":"special","value":{"kind":"project_roots"}}},{"access":"write","path":{"type":"path","path":"/b"}},{"access":"deny","path":{"type":"glob_pattern","pattern":"*.key"}},{"access":"admin","path":{"type":"path","path":"/"}}],"globScanMaxDepth":3,"other":true},"admin":true}})).await;
    f.input(&format!("/approve {code}")).await;
    let payload = f.codex.responses.lock().unwrap()[0].1.clone();
    assert_eq!(payload["scope"], "turn");
    assert_eq!(payload["permissions"]["network"], json!({"enabled":true}));
    assert_eq!(
        payload["permissions"]["fileSystem"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(payload["permissions"]["fileSystem"].get("write").is_none());
    assert!(payload["permissions"].get("admin").is_none());
}
#[tokio::test]
async fn single_question_accepts_exact_natural_whitespace_and_multi_requires_structured_answers() {
    let mut f = Fixture::new();
    f.bind("A");
    let code=f.request(1,"item/tool/requestUserInput",json!({"threadId":"A","questions":[{"id":"q","header":"Pick","question":"Choose","options":[{"label":"One","description":"first"},{"label":"Two"}]}]})).await;
    assert!(f.output().join("").contains("1. One: first\n2. Two"));
    f.input(&format!("/answer {code}  keep \nspace ")).await;
    assert_eq!(
        f.codex.responses.lock().unwrap()[0].1,
        json!({"answers":{"q":{"answers":[" keep \nspace "]}}})
    );
    let code = f
        .request(
            2,
            "item/tool/requestUserInput",
            json!({"threadId":"A","questions":[{"id":"q1"},{"id":"q2"}]}),
        )
        .await;
    f.input(&format!("/answer {code} plain")).await;
    f.input(&format!(
        "/answer {code} {{\"answers\":{{\"q1\":{{\"answers\":[\"x\"]}}}}}}"
    ))
    .await;
    assert_eq!(f.codex.responses.lock().unwrap().len(), 1);
    f.input(&format!("/answer {code} {{\"answers\":{{\"q1\":{{\"answers\":[\"x\"]}},\"q2\":{{\"answers\":[\"y\"]}}}}}}")).await;
    assert_eq!(f.codex.responses.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn mcp_form_url_and_rejection_use_native_payloads_and_current_time_works() {
    let mut f = Fixture::new();
    f.bind("A");
    let code = f
        .request(
            1,
            "mcpServer/elicitation/request",
            json!({"threadId":"A","mode":"form","requestedSchema":{"type":"object"}}),
        )
        .await;
    f.input(&format!("/answer {code} {{\"field\":\"value\"}}"))
        .await;
    assert_eq!(
        f.codex.responses.lock().unwrap()[0].1,
        json!({"action":"accept","content":{"field":"value"}})
    );
    let code = f
        .request(
            2,
            "mcpServer/elicitation/request",
            json!({"threadId":"A","mode":"url","url":"https://example.com"}),
        )
        .await;
    f.input(&format!("/approve {code}")).await;
    assert_eq!(
        f.codex.responses.lock().unwrap()[1].1,
        json!({"action":"accept"})
    );
    let code = f
        .request(
            3,
            "mcpServer/elicitation/request",
            json!({"threadId":"A","mode":"openai/form"}),
        )
        .await;
    f.input(&format!("/reject {code}")).await;
    assert_eq!(
        f.codex.responses.lock().unwrap()[2].1,
        json!({"action":"decline"})
    );
    f.request(4, "currentTime/read", json!({})).await;
    let stamp = f.codex.responses.lock().unwrap()[3].1["currentTimeAt"]
        .as_u64()
        .unwrap();
    assert!(stamp > 1_000_000_000);
}
#[tokio::test]
async fn disconnection_makes_mutations_unknown_never_replays_and_expires_interactions() {
    let mut f = Fixture::new();
    f.bind("A");
    f.codex.set(|m, p| {
        if m == "turn/start" {
            Err(anyhow!("disconnected"))
        } else {
            default_response(m, p)
        }
    });
    f.input("hello").await;
    assert_eq!(f.store.read().unwrap().inbox["1"].status, "unknown");
    assert_eq!(f.runs()[0].status, "unknown");
    let code = f
        .request(
            1,
            "item/fileChange/requestApproval",
            json!({"threadId":"A"}),
        )
        .await;
    assert!(!code.is_empty());
    f.bridge.recover().unwrap();
    f.bridge.connected().await.unwrap();
    assert!(f.store.read().unwrap().interactions.is_empty());
    assert_eq!(f.calls("turn/start").len(), 1);
    f.input("another").await;
    assert_eq!(f.calls("turn/start").len(), 1);
}
#[tokio::test]
async fn explicit_stop_targets_original_run_and_unauthorized_or_media_input_does_not_execute() {
    let mut f = Fixture::new();
    f.bind("A");
    f.input("hello").await;
    f.input("/switch B").await;
    f.input("/stop A").await;
    assert_eq!(
        f.calls("turn/interrupt"),
        vec![json!({"threadId":"A","turnId":"turn-1"})]
    );
    f.input_peer("/new", "mallory", None).await;
    f.input_peer("mixed text", "alice", Some("Unsupported media."))
        .await;
    assert!(f.calls("thread/start").is_empty());
    assert_eq!(f.calls("turn/start").len(), 1);
}
fn unknown_run() -> Run {
    Run {
        id: "saved".into(),
        thread_id: "A".into(),
        turn_id: Some("old".into()),
        peer: "alice".into(),
        status: "unknown".into(),
        started_at_ms: Some(1000.0),
        ..Default::default()
    }
}
#[tokio::test]
async fn recovery_pages_older_targets_and_items_in_history_order_without_replaying_delivered_items()
{
    let f = Fixture::new();
    f.bind("A");
    f.store
        .update(|s| {
            let mut r = unknown_run();
            r.items.insert(
                "already".into(),
                serde_json::from_value(json!({"text":"sent","queued":true})).unwrap(),
            );
            r.items.insert(
                "late".into(),
                serde_json::from_value(json!({"text":"local later"})).unwrap(),
            );
            s.runs.insert(r.id.clone(), r);
            Ok(())
        })
        .unwrap();
    f.codex.set(|m,p|match m{
        "thread/turns/list"=>if p["cursor"].is_null(){Ok(json!({"data":[{"id":"newer","status":"completed"}],"nextCursor":"older"}))}else{Ok(json!({"data":[{"id":"old","status":"completed","startedAt":1}],"nextCursor":null}))},
        "thread/items/list"=>if p["cursor"].is_null(){Ok(json!({"data":[{"turnId":"old","item":{"type":"agentMessage","id":"already","text":"sent"}},{"turnId":"old","item":{"type":"agentMessage","id":"first","text":"first"}}],"nextCursor":"more"}))}else{Ok(json!({"data":[{"turnId":"old","item":{"type":"agentMessage","id":"late","text":"last"}}]}))},
        _=>default_response(m,p)
    });
    f.bridge.connected().await.unwrap();
    assert_eq!(&f.output()[..3], &["first", "last", "[done in ?]"]);
    assert_eq!(f.calls("thread/turns/list").len(), 2);
    assert_eq!(f.calls("thread/items/list").len(), 2);
    assert!(
        f.calls("thread/items/list")
            .iter()
            .all(|p| p["sortDirection"] == "asc")
    );
    assert_eq!(f.runs()[0].status, "completed");
    assert!(f.calls("turn/start").is_empty());
}
#[tokio::test]
async fn missing_target_and_stale_inprogress_stay_unknown_even_on_idle_native_runtime() {
    for turns in [json!([]), json!([{"id":"old","status":"inProgress"}])] {
        let f = Fixture::new();
        f.bind("A");
        f.store
            .update(|s| {
                s.runs.insert("saved".into(), unknown_run());
                Ok(())
            })
            .unwrap();
        f.codex.set(move |m, p| {
            if m == "thread/turns/list" {
                Ok(json!({"data":turns}))
            } else {
                default_response(m, p)
            }
        });
        f.bridge.connected().await.unwrap();
        assert_eq!(f.runs()[0].status, "unknown");
        assert!(f.calls("turn/start").is_empty());
    }
}
#[tokio::test]
async fn item_pagination_fallback_requires_full_detail_and_never_accepts_summaries() {
    for view in ["full", "summary", "notLoaded"] {
        let f = Fixture::new();
        f.bind("A");
        f.store
            .update(|s| {
                s.runs.insert("saved".into(), unknown_run());
                Ok(())
            })
            .unwrap();
        f.codex.set(move|m,p|match m{
            "thread/turns/list"=>Ok(json!({"data":[{"id":"old","status":"completed","durationMs":100,"itemsView":if p["itemsView"]=="full"{view}else{"notLoaded"},"items":[{"type":"agentMessage","id":"body","text":"hydrated"}]}]})),
            "thread/items/list"=>Err(rpc_error(-32601)),_=>default_response(m,p)
        });
        f.bridge.connected().await.unwrap();
        if view == "full" {
            assert_eq!(f.runs()[0].status, "completed");
            assert_eq!(&f.output()[..2], &["hydrated", "[done in 0.1s]"]);
        } else {
            assert_eq!(f.runs()[0].status, "unknown");
            assert!(!f.output().contains(&"hydrated".into()));
        }
    }
}
#[tokio::test]
async fn message_delivery_markers_survive_database_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let mut f = Fixture::with_store(Arc::new(Store::open(&path).unwrap()));
    f.bind("A");
    f.input("hello").await;
    f.item("body", "already sent", Some("commentary")).await;
    f.bridge.recover().unwrap();
    drop(f);
    let mut f = Fixture::with_store(Arc::new(Store::open(&path).unwrap()));
    f.counter = 10;
    f.clear();
    f.codex.set(|m,p|match m{"thread/turns/list"=>Ok(json!({"data":[{"id":"turn-1","status":"completed","durationMs":1000}]})),"thread/items/list"=>Ok(json!({"data":[{"turnId":"turn-1","item":{"type":"agentMessage","id":"body","text":"already sent"}},{"turnId":"turn-1","item":{"type":"agentMessage","id":"new","text":"missed"}}]})),_=>default_response(m,p)});
    f.bridge.connected().await.unwrap();
    assert_eq!(&f.output()[..2], &["missed", "[done in 1.0s]"]);
    assert!(!f.output().contains(&"already sent".into()));
}

#[tokio::test]
async fn completion_before_start_response_never_regresses_provisional_run() {
    let f = Fixture::new();
    f.bind("A");
    let gate = Arc::new(StartGate::default());
    *f.codex.gate.lock().unwrap() = Some(gate.clone());
    f.store
        .accept_batch(
            "1",
            vec![Inbound {
                id: "1".into(),
                peer: "alice".into(),
                text: "hello".into(),
                context_token: "token".into(),
                notice: None,
            }],
        )
        .unwrap();
    let entry = f.store.read().unwrap().inbox["1"].clone();
    let (result, ()) = tokio::join!(f.bridge.handle(entry), async {
        gate.entered.notified().await;
        assert_eq!(f.runs()[0].status, "starting");
        f.event(
            "turn/started",
            json!({"threadId":"A","turn":{"id":"turn-1"}}),
        )
        .await;
        f.item("body", "early completion", None).await;
        f.done(json!({"id":"turn-1","status":"completed","durationMs":100}))
            .await;
        gate.released.notify_one();
    });
    result.unwrap();
    assert_eq!(f.runs()[0].status, "completed");
    assert_eq!(f.output(), vec!["early completion", "[done in 0.1s]"]);
    assert_eq!(
        f.calls("turn/start"),
        vec![json!({"threadId":"A","input":[{"type":"text","text":"hello"}]} )]
    );
}
#[tokio::test]
async fn rejected_concurrent_start_preserves_observed_native_run() {
    let f = Fixture::new();
    f.bind("A");
    let gate = Arc::new(StartGate::default());
    *f.codex.gate.lock().unwrap() = Some(gate.clone());
    f.codex.set(|m, p| {
        if m == "turn/start" {
            Err(rpc_error(-32600))
        } else {
            default_response(m, p)
        }
    });
    f.store
        .accept_batch(
            "1",
            vec![Inbound {
                id: "1".into(),
                peer: "alice".into(),
                text: "hello".into(),
                context_token: "token".into(),
                notice: None,
            }],
        )
        .unwrap();
    let entry = f.store.read().unwrap().inbox["1"].clone();
    let (result, ()) = tokio::join!(f.bridge.handle(entry), async {
        gate.entered.notified().await;
        f.event(
            "turn/started",
            json!({"threadId":"A","turn":{"id":"turn-1"}}),
        )
        .await;
        gate.released.notify_one();
    });
    result.unwrap();
    assert_eq!(f.runs()[0].status, "running");
    f.item("native", "native result", None).await;
    f.done(json!({"id":"turn-1","status":"completed","durationMs":10}))
        .await;
    assert!(f.output().contains(&"native result".into()));
}
#[tokio::test]
async fn unconfirmed_start_response_is_unknown_and_processing_inbox_is_never_replayed() {
    let mut f = Fixture::new();
    f.bind("A");
    f.codex.set(|m, p| {
        if m == "turn/start" {
            Ok(json!({"turn":{}}))
        } else {
            default_response(m, p)
        }
    });
    f.input("hello").await;
    assert_eq!(f.runs()[0].status, "unknown");
    assert_eq!(f.store.read().unwrap().inbox["1"].status, "unknown");
    f.store
        .accept_batch(
            "2",
            vec![Inbound {
                id: "2".into(),
                peer: "alice".into(),
                text: "pending uncertain".into(),
                context_token: "token".into(),
                notice: None,
            }],
        )
        .unwrap();
    f.store
        .update(|s| {
            s.inbox.get_mut("2").unwrap().status = "processing".into();
            Ok(())
        })
        .unwrap();
    f.bridge.recover().unwrap();
    let entry = f.store.read().unwrap().inbox["2"].clone();
    f.bridge.handle(entry).await.unwrap();
    assert_eq!(f.store.read().unwrap().inbox["2"].status, "unknown");
    assert_eq!(f.calls("turn/start").len(), 1);
}
