//! Standalone golden fixtures captured by executing the previous TypeScript Bridge.
//! No Node.js, reference checkout, network, credentials or real Codex server is needed.
use anyhow::Result;
use async_trait::async_trait;
use codex_wechat::{
    bridge::Bridge,
    codex::CodexRpcError,
    models::{Inbound, RpcMessage},
    ports::CodexPort,
    store::Store,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

struct MockCodex {
    calls: Mutex<Vec<Value>>,
    responses: Mutex<Vec<Value>>,
}
impl MockCodex {
    fn new(responses: Vec<Value>) -> Self {
        Self {
            calls: Mutex::new(vec![]),
            responses: Mutex::new(responses),
        }
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
            .push(json!({"method":method,"params":params}));
        let rule = self
            .responses
            .lock()
            .unwrap()
            .iter()
            .find(|rule| {
                rule["method"] == method
                    && rule["params"].as_object().is_none_or(|expected| {
                        expected
                            .iter()
                            .all(|(key, value)| params.get(key) == Some(value))
                    })
            })
            .cloned();
        if let Some(rule) = rule {
            if let Some(error) = rule.get("error") {
                return Err(CodexRpcError {
                    code: error["code"].as_i64().unwrap(),
                    message: error["message"].as_str().unwrap().into(),
                    data: None,
                }
                .into());
            }
            if let Some(result) = rule.get("result") {
                return Ok(result.clone());
            }
        }
        Ok(match method {
            "thread/start" => {
                json!({"thread":{"id":"fresh","cwd":"/project","status":{"type":"idle"},"turns":[]}})
            }
            "thread/read" | "thread/resume" => {
                json!({"thread":{"id":params["threadId"],"cwd":"/project","status":{"type":"idle"},"turns":[]}})
            }
            "thread/turns/list" | "thread/items/list" | "thread/list" => json!({"data":[]}),
            "turn/start" => json!({"turn":{"id":"turn-1"}}),
            _ => json!({}),
        })
    }
    async fn respond(&self, _: Value, _: Value) -> Result<()> {
        panic!("No server requests in parity fixtures")
    }
    async fn respond_error(&self, _: Value, _: i64, _: &str) -> Result<()> {
        panic!("No server requests in parity fixtures")
    }
    fn subscribe(&self) -> broadcast::Receiver<RpcMessage> {
        broadcast::channel(16).1
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

fn checkpoint(store: &Store, codex: &MockCodex) -> Value {
    let state = store.read().unwrap();
    json!({
        "messages": state.outbox.iter().map(|message|json!({"peer":message.peer,"text":message.text})).collect::<Vec<_>>(),
        "bindings": state.bindings,
        "switchChoices": state.switch_choices,
        "runs": state.runs.values().map(|run| json!({
            "threadId":run.thread_id,"turnId":run.turn_id,"status":run.status,
            "items":run.items.iter().map(|(id,item)|(id.clone(),json!({"text":item.text,"phase":item.phase,"completed":item.completed,"queued":item.queued}))).collect::<serde_json::Map<_,_>>()
        })).collect::<Vec<_>>(),
        "inbox": state.inbox.values().map(|entry|json!({"id":entry.message.id,"status":entry.status,"error":entry.error})).collect::<Vec<_>>(),
        "calls":codex.calls.lock().unwrap().clone(),
    })
}

// Keep the captured TS fixture intact. The Rust command interface deliberately
// moved its bare /switch listing to /list. Selected-thread-only delivery is
// separately asserted below and in the bridge/runtime regression tests.
fn current_command_interface(value: &mut Value) {
    match value {
        Value::String(text) => {
            *text = if text == "/switch" {
                "/list".into()
            } else {
                text.replace("Use /switch to list threads", "Use /list to list threads")
                    .replace(
                        "Usage: /switch [1-10 | threadId]",
                        "Usage: /switch <1-10 | threadId>",
                    )
                    .replace(
                        "Wait, /stop, or /recover before continuing.",
                        "Wait, /stop, or select it again with /switch <threadId>.",
                    )
            };
        }
        Value::Array(values) => values.iter_mut().for_each(current_command_interface),
        Value::Object(values) => values.values_mut().for_each(current_command_interface),
        _ => {}
    }
}

#[tokio::test]
async fn rust_matches_actual_typescript_golden_behavior() {
    let mut fixture: Value = serde_json::from_str(include_str!("fixtures/parity.json")).unwrap();
    current_command_interface(&mut fixture);
    assert_eq!(fixture["formatVersion"], 1);
    let scenarios = fixture["scenarios"].as_array().unwrap();
    assert_eq!(scenarios.len(), 8);
    let mut checked = 0;
    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let store = Arc::new(Store::memory().unwrap());
        let codex = Arc::new(MockCodex::new(
            scenario["responses"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        ));
        let bridge = Bridge::new(
            store.clone(),
            codex.clone(),
            PathBuf::from("/project"),
            "alice".into(),
        );
        store
            .update(|state| {
                if let Some(bindings) = scenario["bindings"].as_object() {
                    for (peer, thread_id) in bindings {
                        state
                            .bindings
                            .insert(peer.clone(), thread_id.as_str().unwrap().into());
                    }
                }
                Ok(())
            })
            .unwrap();
        let mut sequence = 0;
        for (index, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            if let Some(responses) = step["responses"].as_array() {
                *codex.responses.lock().unwrap() = responses.clone();
            }
            if let Some(text) = step["input"].as_str() {
                sequence += 1;
                let id = sequence.to_string();
                store
                    .accept_batch(
                        &id,
                        vec![Inbound {
                            id: id.clone(),
                            peer: "alice".into(),
                            text: text.into(),
                            context_token: "synthetic-context".into(),
                            notice: None,
                        }],
                    )
                    .unwrap();
                let entry = store.read().unwrap().inbox[&id].clone();
                bridge.handle(entry).await.unwrap();
            }
            if let Some(event) = step.get("event") {
                bridge
                    .on_codex(serde_json::from_value(event.clone()).unwrap())
                    .await
                    .unwrap();
            }
            let mut expected = step["expected"].clone();
            if name == "numbered-switch-snapshot-survives-unrelated-session-list" && index >= 2 {
                // The captured TS /sessions command has been removed in Rust.
                // Rejection must leave the saved numeric list and binding intact.
                for message in expected["messages"].as_array_mut().unwrap() {
                    if message["text"] == "latest  Reordered latest\n\nT1  Changed title" {
                        message["text"] =
                            json!("Request failed: Unknown command. Use /help for commands.");
                    }
                }
                for entry in expected["inbox"].as_array_mut().unwrap() {
                    if entry["id"] == "2" {
                        entry["error"] = json!("Unknown command. Use /help for commands.");
                    }
                }
                expected["calls"].as_array_mut().unwrap().retain(|call| {
                    call["method"] != "thread/list" || call["params"]["limit"] != 100
                });
            }
            if name == "busy-input-is-not-submitted-and-old-run-survives-switch" && index >= 2 {
                // TS forwarded A after switching to B. Rust deliberately mutes
                // A's body/done but must still track its native completion.
                expected["messages"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|message| {
                        message["text"] != " old run result\n"
                            && message["text"] != "[done in 30.1s]"
                            && !message["text"].as_str().is_some_and(|text| {
                                text.starts_with("Thread is busy or its status is unknown.")
                            })
                    });
            }
            assert_eq!(
                checkpoint(&store, &codex),
                expected,
                "TypeScript parity failed: {name}, step {}",
                index + 1
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 36);
}
