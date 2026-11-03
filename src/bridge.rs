use crate::codex::CodexRpcError;
use crate::models::{
    AgentItem, InboxEntry, Interaction, RpcMessage, Run, State, ThreadChoice, now_ms,
};
use crate::ports::CodexPort;
use crate::store::{Store, enqueue, enqueue_thread, select_thread};
use anyhow::{Result, anyhow, bail};
use indexmap::IndexMap;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const QUESTIONS: &str = "item/tool/requestUserInput";
const ELICITATION: &str = "mcpServer/elicitation/request";
const PERMISSIONS: &str = "item/permissions/requestApproval";
const SETTINGS_TIMEOUT: Duration = Duration::from_secs(10);
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct UnknownOutcome(String);

/// Channel routing only: native Codex owns execution, policy and history.
pub struct Bridge {
    store: Arc<Store>,
    codex: Arc<dyn CodexPort>,
    cwd: PathBuf,
    owner_id: String,
    resumed: Mutex<HashSet<String>>,
}

impl Bridge {
    pub fn new(
        store: Arc<Store>,
        codex: Arc<dyn CodexPort>,
        cwd: PathBuf,
        owner_id: String,
    ) -> Self {
        Self {
            store,
            codex,
            cwd: normalize_path(&cwd),
            owner_id,
            resumed: Mutex::new(HashSet::new()),
        }
    }

    pub async fn handle(&self, entry: InboxEntry) -> Result<()> {
        let Some(saved) = self
            .store
            .read()?
            .inbox
            .get(&entry.message.id)
            .cloned()
            .filter(|e| e.status == "pending")
        else {
            return Ok(());
        };
        let saved = self.store.update(|s| {
            let thread_id = if saved.message.notice.is_none()
                && (!saved.message.text.trim_start().starts_with('/')
                    || saved.message.text.split_whitespace().next() == Some("/effort"))
            {
                s.bindings.get(&saved.message.peer).cloned()
            } else {
                None
            };
            let current = s.inbox.get_mut(&saved.message.id).unwrap();
            current.status = "processing".into();
            current.thread_id = thread_id;
            Ok(current.clone())
        })?;
        if saved.message.peer != self.owner_id {
            self.store.update(|s| {
                let e = s.inbox.get_mut(&saved.message.id).unwrap();
                e.status = "done".into();
                e.error = Some("Unauthorized peer".into());
                Ok(())
            })?;
            return Ok(());
        }
        let outcome = if let Some(notice) = &saved.message.notice {
            self.reply(&saved.message.peer, notice.clone())
        } else {
            self.dispatch(&saved).await
        };
        self.store.update(|s| {
            let e = s.inbox.get_mut(&saved.message.id).unwrap();
            match outcome {
                Ok(()) => e.status = "done".into(),
                Err(error) => {
                    let unknown = error.is::<UnknownOutcome>();
                    e.status = if unknown { "unknown" } else { "done" }.into();
                    e.error = Some(error.to_string());
                    let text = if unknown {
                        let check =
                            if saved.message.text.split_whitespace().next() == Some("/effort") {
                                "Use /effort to check the current setting."
                            } else {
                                "Check /status before continuing."
                            };
                        format!(
                            "Outcome unknown: {error}\n{check} This request will not be replayed."
                        )
                    } else {
                        format!("Request failed: {error}")
                    };
                    if let Some(thread_id) = &saved.thread_id {
                        enqueue_thread(s, &saved.message.peer, thread_id, text);
                    } else {
                        enqueue(s, &saved.message.peer, text);
                    }
                }
            }
            Ok(())
        })
    }

    async fn dispatch(&self, entry: &InboxEntry) -> Result<()> {
        let input = &entry.message;
        let parts: Vec<&str> = input.text.split_whitespace().collect();
        let command = parts.first().copied().unwrap_or("");
        let args = &parts[parts.len().min(1)..];
        let thread_id = self.store.read()?.bindings.get(&input.peer).cloned();
        match command {
            "/new" => {
                if !args.is_empty() {
                    bail!("Usage: /new");
                }
                let result = self
                    .mutate("thread/start", json!({"cwd": self.cwd}))
                    .await?;
                let thread = self.checked_thread(&result, None)?;
                let id = string(&thread, "id").unwrap();
                self.resumed.lock().unwrap().insert(id.into());
                self.store.update(|state| {
                    select_thread(state, &input.peer, id);
                    enqueue(state, &input.peer, format!("New thread: {id}"));
                    Ok(())
                })
            }
            "/list" => {
                if !args.is_empty() {
                    bail!("Usage: /list");
                }
                self.list_recent(&input.peer).await
            }
            "/effort" => {
                if args.len() > 1 {
                    bail!("Usage: /effort [level]");
                }
                let id = entry.thread_id.as_deref().ok_or_else(|| {
                    anyhow!(
                        "Use /new to create a thread, or /list then /switch <number> to choose one."
                    )
                })?;
                self.effort(&input.peer, id, args.first().copied()).await
            }
            "/switch" => {
                if args.len() != 1 {
                    bail!("Usage: /switch <1-10 | threadId>");
                }
                let selection = args[0];
                let (id, title) = if selection.bytes().all(|b| b.is_ascii_digit()) {
                    let index = selection
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| i.checked_sub(1));
                    let state = self.store.read()?;
                    let choice = index
                        .and_then(|i| state.switch_choices.get(&input.peer).and_then(|v| v.get(i)))
                        .ok_or_else(|| anyhow!(
                            "Invalid selection. Use /list to list threads, then /switch <number>."
                        ))?;
                    (choice.thread_id.clone(), Some(choice.title.clone()))
                } else {
                    (selection.into(), None)
                };
                let thread = self.resume(&id).await?;
                self.synchronize_runtime(&input.peer, &thread).await?;
                self.store.update(|state| {
                    select_thread(state, &input.peer, &id);
                    let title = title.as_deref().unwrap_or(&id);
                    enqueue(state, &input.peer, format!("Switched to: {title}"));
                    Ok(())
                })
            }
            "/status" => {
                let state = self.store.read()?;
                let runs = state
                    .runs
                    .values()
                    .filter(|r| r.peer == input.peer && r.active())
                    .map(|r| {
                        format!(
                            "{} / {}: {}",
                            r.thread_id,
                            r.turn_id.as_deref().unwrap_or("pending"),
                            r.status
                        )
                    })
                    .collect::<Vec<_>>();
                let requests = state
                    .interactions
                    .values()
                    .filter(|r| r.peer == input.peer)
                    .map(|r| r.code.clone())
                    .collect::<Vec<_>>();
                let current = thread_id.as_deref().unwrap_or("none; use /new or /list");
                let active = if runs.is_empty() {
                    "No active turns.".into()
                } else {
                    runs.join("\n")
                };
                let pending = if requests.is_empty() {
                    String::new()
                } else {
                    format!("\nPending interactions: {}", requests.join(", "))
                };
                self.reply(
                    &input.peer,
                    format!("Current thread: {current}\n{active}{pending}"),
                )
            }
            "/stop" => {
                if args.len() > 1 {
                    bail!("Usage: /stop [threadId]");
                }
                let id = args.first().copied().or(thread_id.as_deref());
                let state = self.store.read()?;
                let run = state.runs.values()
                    .find(|r| Some(r.thread_id.as_str()) == id && r.peer == input.peer && r.active())
                    .filter(|r| r.turn_id.is_some())
                    .ok_or_else(|| anyhow!(
                        "No known active turn. Check /status or select the thread again with /switch <threadId>."
                    ))?;
                self.mutate(
                    "turn/interrupt",
                    json!({
                        "threadId": run.thread_id, "turnId": run.turn_id
                    }),
                )
                .await?;
                self.reply(&input.peer, "Stop requested.")
            }
            "/approve" | "/reject" | "/answer" => {
                self.answer(&input.peer, command, args, &input.text).await
            }
            "/help" => self.reply(
                &input.peer,
                concat!(
                    "/new\n\n/list\n\n/switch <1-10 | threadId>\n\n/effort [level]\n\n/status\n\n",
                    "/stop [threadId]\n\n/approve <code>\n\n",
                    "/reject <code>\n\n/answer <code> <content>"
                ),
            ),
            _ => {
                if command.starts_with('/') {
                    bail!("Unknown command. Use /help for commands.");
                }
                if input.text.trim().is_empty() {
                    bail!("Empty message.");
                }
                let id = thread_id.ok_or_else(|| {
                    anyhow!(
                        "Use /new to create a thread, or /list then /switch <number> to choose one."
                    )
                })?;
                self.send_input(entry, &id).await
            }
        }
    }

    fn checked_thread(&self, result: &Value, expected: Option<&str>) -> Result<Value> {
        let thread = &result["thread"];
        let id =
            string(thread, "id").ok_or_else(|| anyhow!("Codex returned an invalid thread."))?;
        let cwd =
            string(thread, "cwd").ok_or_else(|| anyhow!("Codex returned an invalid thread."))?;
        if expected.is_some_and(|v| v != id) || normalize_path(Path::new(cwd)) != self.cwd {
            bail!("This thread belongs to a different working directory.");
        }
        Ok(thread.clone())
    }

    async fn resume(&self, id: &str) -> Result<Value> {
        self.checked_thread(
            &self
                .codex
                .request("thread/read", json!({"threadId":id,"includeTurns":false}))
                .await?,
            Some(id),
        )?;
        let thread = self.checked_thread(
            &self
                .codex
                .request("thread/resume", json!({"threadId":id}))
                .await?,
            Some(id),
        )?;
        self.resumed.lock().unwrap().insert(id.into());
        Ok(thread)
    }

    pub async fn connected(&self) -> Result<()> {
        let state = self.store.read()?;
        let mut threads = IndexMap::<String, String>::new();
        for run in state.runs.values().filter(|r| r.active()) {
            threads.insert(run.thread_id.clone(), run.peer.clone());
        }
        for (peer, id) in state.bindings {
            threads.insert(id, peer);
        }
        for (id, peer) in threads {
            let outcome = async {
                let thread = self.resume(&id).await?;
                self.synchronize_runtime(&peer, &thread).await
            }
            .await;
            if let Err(error) = outcome {
                self.reply_thread(
                    &peer,
                    &id,
                    format!("Subscription failed: {error}. Check /status or /switch {id}."),
                )?;
            }
        }
        Ok(())
    }

    async fn list_recent(&self, peer: &str) -> Result<()> {
        let mut choices = Vec::new();
        let mut ids = HashSet::new();
        let mut seen = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = json!({"cwd":self.cwd,"limit":10-choices.len(),"sortKey":"updated_at","sortDirection":"desc"});
            if let Some(c) = &cursor {
                params["cursor"] = Value::String(c.clone());
            }
            let page = self.codex.request("thread/list", params).await?;
            let rows = page["data"]
                .as_array()
                .ok_or_else(|| anyhow!("Codex returned an invalid thread list."))?;
            for row in rows {
                if let (Some(id), Some(cwd)) = (string(row, "id"), string(row, "cwd")) {
                    if ids.contains(id) || normalize_path(Path::new(cwd)) != self.cwd {
                        continue;
                    }
                    ids.insert(id.to_owned());
                    let title = string(row, "name")
                        .filter(|v| !v.trim().is_empty())
                        .or_else(|| string(row, "preview").filter(|v| !v.trim().is_empty()))
                        .unwrap_or("(untitled)")
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    choices.push(ThreadChoice {
                        thread_id: id.into(),
                        title,
                    });
                    if choices.len() >= 10 {
                        break;
                    }
                }
            }
            if choices.len() >= 10 {
                break;
            }
            cursor = next_cursor(&page);
            let Some(c) = &cursor else {
                break;
            };
            if !seen.insert(c.clone()) {
                bail!("Codex returned a repeated thread cursor.");
            }
        }
        self.store.update(|s| {
            let current = s.bindings.get(peer);
            let rows = choices
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let marker = if current == Some(&c.thread_id) {
                        " *"
                    } else {
                        ""
                    };
                    format!("{}. {}{}", i + 1, c.title, marker)
                })
                .collect::<Vec<_>>();
            s.switch_choices.insert(peer.into(), choices);
            let text = if rows.is_empty() {
                "No threads in this directory. Use /new to create one.".into()
            } else {
                format!(
                    "Recent threads:\n\n{}\n\n/switch <number>",
                    rows.join("\n\n")
                )
            };
            enqueue(s, peer, text);
            Ok(())
        })
    }

    async fn effort(&self, peer: &str, thread_id: &str, requested: Option<&str>) -> Result<()> {
        let thread = if requested.is_some() && !self.resumed.lock().unwrap().contains(thread_id) {
            self.resume(thread_id).await?
        } else {
            self.checked_thread(
                &self
                    .codex
                    .request(
                        "thread/read",
                        json!({"threadId":thread_id,"includeTurns":false}),
                    )
                    .await?,
                Some(thread_id),
            )?
        };
        let model = string(&thread, "model")
            .filter(|model| !model.is_empty())
            .ok_or_else(|| anyhow!("Codex did not report the current thread's model."))?;
        let levels = self.effort_levels(model).await?;
        let current = string(&thread, "reasoningEffort");
        let Some(level) = requested else {
            let rows = levels
                .iter()
                .map(|level| format!("{level}{}", if current == Some(level) { " *" } else { "" }))
                .collect::<Vec<_>>();
            let marker = if current.is_some_and(|value| levels.iter().any(|level| level == value)) {
                "\n\n* current"
            } else {
                ""
            };
            return self.reply_thread(
                peer,
                thread_id,
                format!(
                    "Reasoning effort ({model}):\n\n{}{marker}\n\n/effort <level>",
                    rows.join("\n\n")
                ),
            );
        };
        if !levels.iter().any(|supported| supported == level) {
            bail!(
                "Unsupported effort: {level}. Supported: {}.",
                levels.join(", ")
            );
        }
        if current == Some(level) {
            return self.reply_thread(peer, thread_id, format!("Effort is already {level}."));
        }
        if self.store.read()?.bindings.get(peer).map(String::as_str) != Some(thread_id) {
            bail!("Selected thread changed. Run /effort again.");
        }
        // The RPC acknowledges enqueueing only. Subscribe first so an applied
        // settings notification arriving before that acknowledgement is retained.
        let mut events = self.codex.subscribe();
        self.mutate(
            "thread/settings/update",
            json!({"threadId":thread_id,"effort":level}),
        )
        .await?;
        timeout(SETTINGS_TIMEOUT, async {
            loop {
                let event = events
                    .recv()
                    .await
                    .map_err(|_| UnknownOutcome("Native settings confirmation was lost.".into()))?;
                if event.method == "bridge/disconnected" {
                    return Err(UnknownOutcome(
                        "Disconnected before confirming the effort change.".into(),
                    ));
                }
                if event.id.is_none()
                    && event.method == "thread/settings/updated"
                    && string(&event.params, "threadId") == Some(thread_id)
                    && string(&event.params["threadSettings"], "effort") == Some(level)
                {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| UnknownOutcome("Timed out confirming the effort change.".into()))??;
        self.reply_thread(
            peer,
            thread_id,
            format!("Effort set to {level} for subsequent turns."),
        )
    }

    async fn effort_levels(&self, model: &str) -> Result<Vec<String>> {
        let mut cursor = None;
        let mut seen = HashSet::new();
        loop {
            let mut params = json!({"includeHidden":true,"limit":100});
            if let Some(value) = &cursor {
                params["cursor"] = json!(value);
            }
            let page = self.codex.request("model/list", params).await?;
            let data = page["data"]
                .as_array()
                .ok_or_else(|| anyhow!("Codex returned an invalid model list."))?;
            if let Some(entry) = data.iter().find(|entry| {
                string(entry, "model") == Some(model) || string(entry, "id") == Some(model)
            }) {
                let options = entry["supportedReasoningEfforts"]
                    .as_array()
                    .ok_or_else(|| {
                        anyhow!("Codex did not report supported reasoning efforts for {model}.")
                    })?;
                let mut levels = Vec::new();
                for option in options {
                    let level = string(option, "reasoningEffort")
                        .filter(|level| !level.is_empty())
                        .ok_or_else(|| anyhow!("Codex returned an invalid reasoning effort."))?;
                    if !levels.iter().any(|value| value == level) {
                        levels.push(level.to_owned());
                    }
                }
                if levels.is_empty() {
                    bail!("No reasoning effort levels are available for {model}.");
                }
                return Ok(levels);
            }
            cursor = next_cursor(&page);
            let Some(value) = &cursor else {
                bail!("The current model {model} is absent from Codex's model catalog.");
            };
            if !seen.insert(value.clone()) {
                bail!("Codex returned a repeated model cursor.");
            }
        }
    }

    async fn send_input(&self, entry: &InboxEntry, thread_id: &str) -> Result<()> {
        let input = &entry.message;
        if self
            .store
            .read()?
            .runs
            .values()
            .any(|r| r.thread_id == thread_id && r.active())
        {
            return self.reply_thread(&input.peer, thread_id,"Thread is busy or its status is unknown. Wait, /stop, or select it again with /switch <threadId>. This input was not submitted.");
        }
        let resumed = self.resumed.lock().unwrap().contains(thread_id);
        if !resumed {
            let thread = self.resume(thread_id).await?;
            self.observe_runtime(&input.peer, &thread).await?;
            if self
                .store
                .read()?
                .runs
                .values()
                .any(|r| r.thread_id == thread_id && r.active())
            {
                return self.reply_thread(&input.peer, thread_id,"Thread is not ready for a new turn. This input was not submitted. Check /status or select it again with /switch <threadId>.");
            }
        }
        let run = Run {
            id: Uuid::new_v4().to_string(),
            thread_id: thread_id.into(),
            peer: input.peer.clone(),
            status: "starting".into(),
            started_at_ms: Some(now_ms() as f64),
            ..Default::default()
        };
        let id = run.id.clone();
        self.store.update(|s| {
            s.runs.insert(id.clone(), run);
            Ok(())
        })?;
        let outcome = async {
            let result = self
                .mutate(
                    "turn/start",
                    json!({"threadId":thread_id,"input":[{"type":"text","text":input.text}]}),
                )
                .await?;
            let turn_id = string(&result["turn"], "id")
                .ok_or_else(|| UnknownOutcome("turn/start returned no turn ID".into()))?;
            self.store.update(|s| {
                let current = s.runs.get_mut(&id).unwrap();
                if current.turn_id.as_deref().is_some_and(|v| v != turn_id) {
                    return Err(UnknownOutcome(
                        "Turn ID does not match the received events".into(),
                    )
                    .into());
                }
                current.turn_id = Some(turn_id.into());
                record_start_time(current, &result["turn"]);
                if current.status == "starting" {
                    current.status = "running".into();
                }
                Ok(())
            })
        }
        .await;
        if let Err(error) = outcome {
            let finished = self.store.update(|s| {
                let current = s.runs.get_mut(&id).unwrap();
                if error.is::<CodexRpcError>() && current.turn_id.is_some() {
                    return Ok(false);
                }
                let finished = !current.active();
                if !finished {
                    current.status = if error.is::<UnknownOutcome>() {
                        "unknown"
                    } else {
                        "failed"
                    }
                    .into();
                }
                Ok(finished)
            })?;
            if !finished {
                return Err(error);
            }
        }
        Ok(())
    }

    async fn mutate(&self, method: &str, params: Value) -> Result<Value> {
        self.codex.request(method, params).await.map_err(|e| {
            if e.is::<CodexRpcError>() {
                e
            } else {
                UnknownOutcome(format!("{method}: {e}")).into()
            }
        })
    }

    pub async fn on_codex(&self, message: RpcMessage) -> Result<()> {
        if message.method == "bridge/disconnected" {
            return self.recover();
        }
        if message.id.is_some() {
            return self.receive_request(message).await;
        }
        let params = &message.params;
        if message.method == "serverRequest/resolved" {
            return self.store.update(|s| {
                s.interactions.retain(|_, r| {
                    Some(r.thread_id.as_str()) != string(params, "threadId")
                        || r.rpc_id != params["requestId"]
                });
                Ok(())
            });
        }
        let turn_id = string(params, "turnId").or_else(|| string(&params["turn"], "id"));
        let thread_id = string(params, "threadId");
        let state = self.store.read()?;
        let mut run = state
            .runs
            .values()
            .find(|r| {
                Some(r.thread_id.as_str()) == thread_id
                    && (turn_id.is_some_and(|id| r.turn_id.as_deref() == Some(id))
                        || (r.turn_id.is_none() && r.status == "starting"))
            })
            .cloned();
        if run.is_none()
            && message.method == "turn/started"
            && let (Some(tid), Some(id)) = (thread_id, turn_id)
            && self.resumed.lock().unwrap().contains(tid)
        {
            let adopted = Run {
                id: Uuid::new_v4().to_string(),
                thread_id: tid.into(),
                turn_id: Some(id.into()),
                peer: self.owner_id.clone(),
                status: "running".into(),
                started_at_ms: Some(now_ms() as f64),
                ..Default::default()
            };
            self.store.update(|s| {
                s.runs.insert(adopted.id.clone(), adopted.clone());
                Ok(())
            })?;
            run = Some(adopted);
        }
        let Some(run) = run.filter(|r| r.active()) else {
            return Ok(());
        };
        self.store.update(|s| {
            let mut current = s.runs.shift_remove(&run.id).unwrap();
            if let Some(id) = turn_id {
                current.turn_id = Some(id.into());
            }
            match message.method.as_str() {
                "turn/started" => {
                    record_start_time(&mut current, &params["turn"]);
                    if current.status == "starting" {
                        current.status = "running".into();
                    }
                }
                "item/agentMessage/delta" => {
                    if let (Some(id), Some(delta)) =
                        (string(params, "itemId"), string(params, "delta"))
                    {
                        let item = current.items.entry(id.into()).or_default();
                        if !item.completed {
                            item.text.push_str(delta);
                        }
                    }
                }
                "item/started" | "item/completed" => {
                    let value = &params["item"];
                    if value["type"] == "agentMessage"
                        && let Some(id) = string(value, "id")
                    {
                        let item = current.items.entry(id.into()).or_default();
                        if let Some(text) = string(value, "text")
                            && (message.method == "item/completed" || item.text.is_empty())
                        {
                            item.text = text.into();
                        }
                        if let Some(phase) = string(value, "phase") {
                            item.phase = Some(phase.into());
                        }
                        if message.method == "item/completed" {
                            item.completed = true;
                            queue_agent_message(s, &mut current, id);
                        }
                    }
                }
                "turn/completed" => complete(s, &mut current, &params["turn"], false),
                _ => {}
            }
            s.runs.insert(current.id.clone(), current);
            Ok(())
        })
    }

    async fn receive_request(&self, message: RpcMessage) -> Result<()> {
        let rpc_id = message.id.unwrap();
        if message.method == "currentTime/read" {
            return self
                .codex
                .respond(rpc_id, json!({"currentTimeAt":now_ms()/1000}))
                .await;
        }
        let params = &message.params;
        let state = self.store.read()?;
        let thread_id = string(params, "threadId");
        let run = state.runs.values().find(|r| {
            Some(r.thread_id.as_str()) == thread_id
                && r.active()
                && (params.get("turnId").is_none_or(Value::is_null)
                    || r.turn_id.as_deref() == string(params, "turnId")
                    || (r.turn_id.is_none() && r.status == "starting"))
        });
        let peer = run.map(|r| r.peer.clone()).or_else(|| {
            state
                .bindings
                .get(&self.owner_id)
                .filter(|v| Some(v.as_str()) == thread_id)
                .map(|_| self.owner_id.clone())
        });
        let Some(peer) = peer else { return Ok(()) };
        let Some(thread_id) = thread_id.filter(|id| {
            state
                .bindings
                .get(&peer)
                .is_some_and(|selected| selected == id)
        }) else {
            return Ok(());
        };
        let decisions = params["availableDecisions"].as_array();
        let unsupported = message.method == "item/commandExecution/requestApproval"
            && decisions.is_some_and(|v| {
                !v.iter()
                    .any(|d| matches!(d.as_str(), Some("accept" | "decline" | "cancel")))
            });
        if !is_approval(&message.method)
            && message.method != QUESTIONS
            && message.method != ELICITATION
            || unsupported
            || (message.method == ELICITATION
                && !matches!(
                    string(params, "mode"),
                    Some("form" | "openai/form" | "openaiForm" | "url")
                ))
        {
            return self.reply_thread(
                &peer,
                thread_id,
                format!("Handle {} in the Codex client.", message.method),
            );
        }
        if state
            .interactions
            .values()
            .any(|r| r.rpc_id == rpc_id && r.thread_id == thread_id)
        {
            return Ok(());
        };
        let code = loop {
            let code = format!("{:08x}", rand::random::<u32>());
            if !state.interactions.contains_key(&code) {
                break code;
            }
        };
        let details = if message.method == QUESTIONS {
            question_prompt(&params["questions"], &code)
        } else if message.method == ELICITATION {
            format!(
                "{}\n{}\n{}",
                display_value(params.get("message")),
                params.get("url").map_or_else(
                    || serde_json::to_string_pretty(&params["requestedSchema"]).unwrap(),
                    |v| display_value(Some(v))
                ),
                if params["mode"] == "url" {
                    format!("/approve {code}")
                } else {
                    format!(
                        "/answer {code} {{\"field\":\"value\"}}\nEnter a JSON object that matches the form schema."
                    )
                }
            )
        } else {
            format!(
                "{}{}\n/approve {code}",
                display_value(params.get("command").or_else(|| params.get("reason"))),
                if message.method == PERMISSIONS {
                    format!(
                        "\n{}",
                        serde_json::to_string_pretty(&params["permissions"]).unwrap()
                    )
                } else {
                    String::new()
                }
            )
        };
        let request = Interaction {
            code: code.clone(),
            rpc_id,
            method: message.method.clone(),
            peer: peer.clone(),
            thread_id: thread_id.into(),
            turn_id: string(params, "turnId").map(str::to_owned),
            params: params.clone(),
        };
        self.store.update(|s| {
            // Selection may change while a native request is being handled.
            if s.bindings.get(&peer).is_none_or(|id| id != thread_id) {
                return Ok(());
            }
            if let Some(run) = run
                && let Some(current) = s.runs.get_mut(&run.id)
                && current.turn_id.is_none()
            {
                current.turn_id = request.turn_id.clone();
            }
            s.interactions.insert(code.clone(), request);
            enqueue_thread(
                s,
                &peer,
                thread_id,
                format!(
                    "Interaction {code} ({})\n{details}\n/reject {code}",
                    message.method
                ),
            );
            Ok(())
        })
    }

    async fn answer(&self, peer: &str, command: &str, args: &[&str], text: &str) -> Result<()> {
        let state = self.store.read()?;
        let request = args
            .first()
            .and_then(|c| state.interactions.get(*c))
            .filter(|r| r.peer == peer && state.bindings.get(peer) == Some(&r.thread_id))
            .ok_or_else(|| {
                anyhow!("Invalid or expired interaction code. Use the full code from the prompt.")
            })?
            .clone();
        if command != "/answer" && args.len() != 1 {
            bail!("Usage: /approve <code> or /reject <code>");
        }
        let payload = if request.method == QUESTIONS {
            if command == "/approve" {
                bail!("Use /answer <code> <content>; use a JSON object for multiple questions.");
            }
            if command == "/reject" {
                json!({"answers":{}})
            } else {
                let body = answer_body(text);
                let questions = request.params["questions"].as_array();
                let payload = if let Some(single) = questions
                    .filter(|q| q.len() == 1)
                    .and_then(|q| q.first())
                    .filter(|_| !body.trim_start().starts_with('{'))
                {
                    json!({"answers":{single["id"].as_str().unwrap_or_default():{"answers":[body]}}})
                } else {
                    parse_answer(text, &request.code)?
                };
                if body.trim().is_empty() {
                    bail!("Answer cannot be empty.");
                }
                validate_answers(&payload, &request.params["questions"])?;
                payload
            }
        } else if request.method == ELICITATION {
            if command == "/reject" {
                json!({"action":"decline"})
            } else if request.params["mode"] == "url" {
                if command != "/approve" {
                    bail!("Complete the URL interaction, then use /approve <code>.");
                }
                json!({"action":"accept"})
            } else {
                if command != "/answer" {
                    bail!("Use /answer <code> <JSON> for this form.");
                }
                json!({"action":"accept","content":parse_answer(text,&request.code)?})
            }
        } else if request.method == PERMISSIONS {
            if command == "/answer" {
                bail!("Use /approve <code> or /reject <code> for permissions.");
            }
            json!({"permissions":if command=="/approve"{requested_permissions(&request.params["permissions"])?}else{json!({})},"scope":"turn"})
        } else {
            if command == "/answer" {
                bail!("Use /approve <code> or /reject <code> for approvals.");
            }
            let decisions = request.params["availableDecisions"].as_array();
            let offered = |v: &str| decisions.is_some_and(|d| d.contains(&json!(v)));
            let decision = if command == "/approve" {
                "accept"
            } else if decisions.is_some() && !offered("decline") && offered("cancel") {
                "cancel"
            } else {
                "decline"
            };
            if decisions.is_some() && !offered(decision) {
                bail!(
                    "The native request does not offer {decision}. Handle it in the Codex client."
                );
            }
            json!({"decision":decision})
        };
        self.store.update(|s| {
            s.interactions.shift_remove(&request.code);
            Ok(())
        })?;
        self.codex
            .respond(request.rpc_id, payload)
            .await
            .map_err(|e| UnknownOutcome(format!("Interaction {}: {e}", request.code)))?;
        self.reply_thread(
            peer,
            &request.thread_id,
            format!("Interaction {} submitted.", request.code),
        )
    }

    pub fn recover(&self) -> Result<()> {
        self.resumed.lock().unwrap().clear();
        self.store.update(|state| {
            let mut replies = Vec::new();
            for entry in state.inbox.values_mut().filter(|entry| entry.status == "processing") {
                entry.status = "unknown".into();
                let id = &entry.message.id;
                let text = format!(
                    "Message {id} has an unknown outcome after disconnection. It will not be replayed. Check /status."
                );
                replies.push((entry.message.peer.clone(), entry.thread_id.clone(), text));
            }
            for run in state.runs.values_mut()
                .filter(|run| run.status == "starting" || run.status == "running")
            {
                run.status = "unknown".into();
                let text = "Turn status unknown. Native history will be checked after reconnection. Check /status.".into();
                replies.push((run.peer.clone(), Some(run.thread_id.clone()), text));
            }
            for request in state.interactions.values() {
                let text = format!("Interaction {} expired after disconnection.", request.code);
                replies.push((request.peer.clone(), Some(request.thread_id.clone()), text));
            }
            state.interactions.clear();
            for (peer, thread_id, text) in replies {
                if let Some(id) = thread_id { enqueue_thread(state, &peer, &id, text); }
                else { enqueue(state, &peer, text); }
            }
            Ok(())
        })
    }

    async fn observe_runtime(&self, peer: &str, thread: &Value) -> Result<()> {
        if thread["status"]["type"] == "idle" {
            return Ok(());
        };
        let id = string(thread, "id").unwrap();
        let turns = self.list_turns(id, &HashSet::new(), "notLoaded").await?;
        self.store.update(|s| {
            let live = if thread["status"]["type"] == "active" {
                turns.iter().find(|t| t["status"] == "inProgress")
            } else {
                None
            };
            if let Some(turn) = live {
                adopt_live_run(s, peer, id, turn);
            } else if !s.runs.values().any(|r| r.thread_id == id && r.active()) {
                let run = Run {
                    id: Uuid::new_v4().to_string(),
                    thread_id: id.into(),
                    peer: peer.into(),
                    status: "unknown".into(),
                    ..Default::default()
                };
                s.runs.insert(run.id.clone(), run);
            }
            Ok(())
        })
    }

    async fn list_turns(
        &self,
        thread_id: &str,
        targets: &HashSet<String>,
        items_view: &str,
    ) -> Result<Vec<Value>> {
        let mut turns = IndexMap::new();
        let mut cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = json!({"threadId":thread_id,"limit":100,"sortDirection":"desc","itemsView":items_view});
            if let Some(c) = &cursor {
                params["cursor"] = json!(c);
            }
            let page = self.codex.request("thread/turns/list", params).await?;
            let data = page["data"].as_array().ok_or_else(|| {
                anyhow!("Codex returned invalid turn history. Status remains unknown.")
            })?;
            for turn in data {
                if let Some(id) = string(turn, "id") {
                    turns.entry(id.to_owned()).or_insert_with(|| turn.clone());
                }
            }
            cursor = next_cursor(&page);
            let Some(c) = &cursor else { break };
            if !cursors.insert(c.clone()) {
                bail!("Codex returned a repeated turn cursor. Status remains unknown.");
            }
            if !targets.iter().any(|id| !turns.contains_key(id)) {
                break;
            }
        }
        Ok(turns.into_values().collect())
    }

    async fn turn_items(&self, thread_id: &str, turn: &Value) -> Result<Vec<Value>> {
        let outcome=async {
            let mut items=Vec::new();let mut cursors=HashSet::new();let mut cursor=None;
            loop{
                let mut params=json!({"threadId":thread_id,"turnId":turn["id"],"limit":100,"sortDirection":"asc"});if let Some(c)=&cursor{params["cursor"]=json!(c);}
                let page=self.codex.request("thread/items/list",params).await?;
                let data=page["data"].as_array().ok_or_else(||anyhow!("Codex returned invalid message history. Status remains unknown."))?;
                for entry in data {if entry["turnId"]==turn["id"] && entry["item"].is_object(){items.push(entry["item"].clone());}}
                cursor=next_cursor(&page);let Some(c)=&cursor else{break};if !cursors.insert(c.clone()){bail!("Codex returned a repeated message cursor. Status remains unknown.");}
            }
            Ok::<_,anyhow::Error>(items)
        }.await;
        match outcome {
            Ok(items) => Ok(items),
            Err(error)
                if error
                    .downcast_ref::<CodexRpcError>()
                    .is_some_and(|e| e.code == -32601) =>
            {
                let targets = HashSet::from([string(turn, "id").unwrap().to_owned()]);
                let turns = self.list_turns(thread_id, &targets, "full").await?;
                let full = turns.iter().find(|t| t["id"] == turn["id"]);
                if let Some(items) = full
                    .filter(|t| t["itemsView"] != "notLoaded" && t["itemsView"] != "summary")
                    .and_then(|t| t["items"].as_array())
                {
                    Ok(items.clone())
                } else {
                    bail!(
                        "Full message history is unavailable. Status remains unknown; check the Codex client."
                    )
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn synchronize_runtime(&self, peer: &str, thread: &Value) -> Result<()> {
        let thread_id = string(thread, "id").unwrap();
        if self
            .store
            .read()?
            .runs
            .values()
            .any(|run| run.peer == peer && run.thread_id == thread_id && run.status == "unknown")
        {
            self.reconcile_thread(peer, thread).await
        } else {
            self.observe_runtime(peer, thread).await
        }
    }

    async fn reconcile_thread(&self, peer: &str, thread: &Value) -> Result<()> {
        let thread_id = string(thread, "id").unwrap();
        let unknown = self
            .store
            .read()?
            .runs
            .values()
            .filter(|r| r.peer == peer && r.thread_id == thread_id && r.status == "unknown")
            .cloned()
            .collect::<Vec<_>>();
        let targets = unknown.iter().filter_map(|r| r.turn_id.clone()).collect();
        let turns = self.list_turns(thread_id, &targets, "notLoaded").await?;
        let live = if thread["status"]["type"] == "active" {
            turns.iter().find(|t| t["status"] == "inProgress")
        } else {
            None
        };
        let mut hydrated = HashMap::new();
        for run in &unknown {
            if let Some(turn) = turns
                .iter()
                .find(|t| string(t, "id") == run.turn_id.as_deref())
                .filter(|t| {
                    matches!(
                        string(t, "status"),
                        Some("completed" | "failed" | "interrupted")
                    )
                })
            {
                hydrated.insert(
                    string(turn, "id").unwrap().to_owned(),
                    self.turn_items(thread_id, turn).await?,
                );
            }
        }
        self.store.update(|state| {
            for previous in &unknown {
                let mut run = state.runs.shift_remove(&previous.id).unwrap();
                // A native notification may finish the run while history is being loaded.
                if run.status == "unknown"
                    && let Some(turn) = turns
                        .iter()
                        .find(|turn| string(turn, "id") == run.turn_id.as_deref())
                {
                    if turn["status"] == "inProgress" && thread["status"]["type"] == "active" {
                        record_start_time(&mut run, turn);
                        run.status = "running".into();
                    } else if let Some(items) = string(turn, "id").and_then(|id| hydrated.get(id)) {
                        // Native items arrive ascending, even if the buffered insertion order differs.
                        for item in items {
                            hydrate_item(state, &mut run, item);
                        }
                        complete(state, &mut run, turn, true);
                    }
                }
                state.runs.insert(run.id.clone(), run);
            }
            if let Some(turn) = live {
                adopt_live_run(state, peer, thread_id, turn);
            }
            Ok(())
        })
    }

    fn reply_thread(&self, peer: &str, thread_id: &str, text: impl Into<String>) -> Result<()> {
        self.store.update(|s| {
            enqueue_thread(s, peer, thread_id, text);
            Ok(())
        })
    }

    fn reply(&self, peer: &str, text: impl Into<String>) -> Result<()> {
        self.store.update(|s| {
            enqueue(s, peer, text);
            Ok(())
        })
    }
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}
fn next_cursor(page: &Value) -> Option<String> {
    string(page, "nextCursor")
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}
fn normalize_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    let mut result = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                result.pop();
            }
            _ => result.push(component.as_os_str()),
        }
    }
    result
}
fn is_approval(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" | PERMISSIONS
    )
}
fn display_value(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    }
}
fn nonnegative(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite() && *v >= 0.0)
}
fn record_start_time(run: &mut Run, turn: &Value) {
    if let Some(seconds) = nonnegative(turn.get("startedAt")) {
        run.started_at_ms = Some(seconds * 1000.0);
    }
}
fn format_duration(duration: Option<f64>) -> String {
    let Some(ms) = duration else {
        return "?".into();
    };
    let tenths = (ms / 100.0).round() as u64;
    let minutes = tenths / 600;
    let seconds = (tenths % 600) as f64 / 10.0;
    if minutes > 0 {
        format!("{minutes}m{seconds:.1}s")
    } else {
        format!("{seconds:.1}s")
    }
}
fn queue_agent_message(state: &mut State, run: &mut Run, id: &str) {
    if let Some(item) = run.items.get_mut(id)
        && !item.queued
        && !item.text.is_empty()
    {
        enqueue_thread(state, &run.peer, &run.thread_id, item.text.clone());
        item.queued = true;
    }
}
fn hydrate_item(state: &mut State, run: &mut Run, value: &Value) {
    if value["type"] == "agentMessage"
        && let (Some(id), Some(text)) = (string(value, "id"), string(value, "text"))
    {
        let item = run.items.entry(id.into()).or_default();
        item.text = text.into();
        item.phase = string(value, "phase").map(str::to_owned);
        item.completed = true;
        queue_agent_message(state, run, id);
    }
}
fn complete(state: &mut State, run: &mut Run, turn: &Value, recovered: bool) {
    record_start_time(run, turn);
    let end = nonnegative(turn.get("completedAt"))
        .map(|t| t * 1000.0)
        .or_else(|| {
            if recovered {
                None
            } else {
                Some(now_ms() as f64)
            }
        });
    run.duration_ms = nonnegative(turn.get("durationMs")).or_else(|| {
        run.started_at_ms
            .filter(|t| t.is_finite() && *t >= 0.0)
            .zip(end)
            .filter(|(start, end)| end >= start)
            .map(|(start, end)| end - start)
    });
    run.status = if turn["status"] == "failed"
        || (!turn["error"].is_null() && turn["error"] != false && turn["status"] != "interrupted")
    {
        "failed"
    } else if turn["status"] == "interrupted" {
        "interrupted"
    } else {
        "completed"
    }
    .into();
    if turn["itemsView"] != "summary"
        && turn["itemsView"] != "notLoaded"
        && let Some(items) = turn["items"].as_array()
    {
        for item in items {
            hydrate_item(state, run, item);
        }
    }
    for id in run.items.keys().cloned().collect::<Vec<_>>() {
        queue_agent_message(state, run, &id);
    }
    let status = match run.status.as_str() {
        "failed" => "failed",
        "interrupted" => "stopped",
        _ => "done",
    };
    let detail = if run.status == "failed" {
        format!(
            "\n{}",
            string(&turn["error"], "message").unwrap_or("Unknown error")
        )
    } else {
        String::new()
    };
    enqueue_thread(
        state,
        &run.peer,
        &run.thread_id,
        format!("[{status} in {}]{detail}", format_duration(run.duration_ms)),
    );
    state.interactions.retain(|_, r| {
        r.thread_id != run.thread_id || (r.turn_id.is_some() && r.turn_id != run.turn_id)
    });
}
fn adopt_live_run(state: &mut State, peer: &str, thread_id: &str, turn: &Value) {
    let Some(id) = string(turn, "id").filter(|_| turn["status"] == "inProgress") else {
        return;
    };
    if state
        .runs
        .values()
        .any(|r| r.thread_id == thread_id && r.active())
    {
        return;
    }
    let mut run = Run {
        id: Uuid::new_v4().to_string(),
        thread_id: thread_id.into(),
        turn_id: Some(id.into()),
        peer: peer.into(),
        status: "running".into(),
        ..Default::default()
    };
    record_start_time(&mut run, turn);
    if let Some(items) = turn["items"].as_array() {
        for item in items {
            if item["type"] == "agentMessage"
                && let (Some(id), Some(text)) = (string(item, "id"), string(item, "text"))
            {
                run.items.insert(
                    id.into(),
                    AgentItem {
                        text: text.into(),
                        phase: string(item, "phase").map(str::to_owned),
                        ..Default::default()
                    },
                );
            }
        }
    }
    state.runs.insert(run.id.clone(), run);
}
fn answer_body(text: &str) -> &str {
    let raw = text.trim_start();
    let Some(rest) = raw.strip_prefix("/answer") else {
        return "";
    };
    if !rest.starts_with(char::is_whitespace) {
        return "";
    };
    let rest = rest.trim_start();
    let Some(end) = rest.find(char::is_whitespace) else {
        return "";
    };
    let separator = &rest[end..];
    if let Some(body) = separator.strip_prefix("\r\n") {
        body
    } else if separator.starts_with([' ', '\t', '\n']) {
        &separator[1..]
    } else {
        ""
    }
}
fn parse_answer(text: &str, code: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(answer_body(text))
        .map_err(|_| anyhow!("Usage: /answer {code} <JSON>"))?;
    if !value.is_object() {
        bail!("Answer must be a JSON object.");
    }
    Ok(value)
}
fn question_prompt(value: &Value, code: &str) -> String {
    let empty = Vec::new();
    let list = value.as_array().unwrap_or(&empty);
    let text = list
        .iter()
        .map(|q| {
            let options = q["options"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .enumerate()
                        .map(|(i, o)| {
                            format!(
                                "{}. {}{}",
                                i + 1,
                                display_value(o.get("label")),
                                string(o, "description")
                                    .filter(|v| !v.is_empty())
                                    .map_or_else(String::new, |v| format!(": {v}"))
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            format!(
                "{}{}\n{}{}",
                string(q, "header").unwrap_or("Question"),
                if list.len() > 1 {
                    format!(" ({})", display_value(q.get("id")))
                } else {
                    String::new()
                },
                display_value(q.get("question")),
                if options.is_empty() {
                    String::new()
                } else {
                    format!("\n{options}")
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let example = if list.len() == 1 {
        "<content>".into()
    } else {
        let mut answers = Map::new();
        for q in list {
            answers.insert(
                string(q, "id").unwrap_or_default().into(),
                json!({"answers":["answer"]}),
            );
        }
        json!({"answers":answers}).to_string()
    };
    format!("{text}\n/answer {code} {example}")
}
fn validate_answers(value: &Value, questions: &Value) -> Result<()> {
    let answers = value["answers"].as_object();
    let questions = questions.as_array();
    let (Some(root), Some(answers), Some(questions)) = (value.as_object(), answers, questions)
    else {
        bail!("Answer format: {{\"answers\":{{\"questionId\":{{\"answers\":[\"answer\"]}}}}}}");
    };
    if root.keys().any(|k| k != "answers")
        || answers.keys().any(|id| {
            !questions
                .iter()
                .any(|q| string(q, "id") == Some(id.as_str()))
        })
    {
        bail!("Answer contains an unknown question.");
    }
    for question in questions {
        let id = string(question, "id").unwrap_or_default();
        let answer = answers.get(id);
        if !answer.is_some_and(|a| {
            a.as_object()
                .is_some_and(|o| o.keys().all(|k| k == "answers"))
                && a["answers"]
                    .as_array()
                    .is_some_and(|v| !v.is_empty() && v.iter().all(Value::is_string))
        }) {
            bail!("Question {id} requires a nonempty array of strings.");
        }
    }
    Ok(())
}
fn requested_permissions(value: &Value) -> Result<Value> {
    if !value.is_object() {
        bail!("Invalid native permissions request.");
    }
    let mut result = Map::new();
    if let Some(enabled) = value["network"]["enabled"].as_bool() {
        result.insert("network".into(), json!({"enabled":enabled}));
    }
    if let Some(fs) = value["fileSystem"].as_object() {
        let mut out = Map::new();
        for key in ["read", "write"] {
            if let Some(paths) = fs
                .get(key)
                .and_then(Value::as_array)
                .filter(|v| v.iter().all(Value::is_string))
            {
                out.insert(key.into(), json!(paths));
            }
        }
        if let Some(entries) = fs.get("entries").and_then(Value::as_array) {
            let valid = entries
                .iter()
                .filter(|entry| {
                    matches!(string(entry, "access"), Some("read" | "write" | "deny"))
                        && match string(&entry["path"], "type") {
                            Some("path") => string(&entry["path"], "path").is_some(),
                            Some("glob_pattern") => string(&entry["path"], "pattern").is_some(),
                            Some("special") => matches!(
                                string(&entry["path"]["value"], "kind"),
                                Some("root" | "minimal" | "project_roots" | "tmpdir" | "slash_tmp")
                            ),
                            _ => false,
                        }
                })
                .cloned()
                .collect::<Vec<_>>();
            out.insert("entries".into(), json!(valid));
        }
        if let Some(depth) = fs
            .get("globScanMaxDepth")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0)
        {
            out.insert("globScanMaxDepth".into(), json!(depth));
        }
        if !out.is_empty() {
            result.insert("fileSystem".into(), Value::Object(out));
        }
    }
    Ok(Value::Object(result))
}
