//! A client of the existing native shared App Server, never a model proxy.
use crate::{models::RpcMessage, ports::CodexPort};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::UnixStream,
    sync::{Mutex as AsyncMutex, broadcast, mpsc, oneshot},
    time::timeout,
};
use tokio_tungstenite::{
    client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const FRAME_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct CodexRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

type Reply = oneshot::Sender<Result<Value>>;

struct Connection {
    writer: mpsc::UnboundedSender<Message>,
    cancel: CancellationToken,
    finished: CancellationToken,
}

#[derive(Default)]
struct State {
    generation: u64,
    sequence: u64,
    ready: bool,
    connection: Option<Connection>,
    pending: HashMap<String, Reply>,
}

struct Inner {
    state: Mutex<State>,
    events: broadcast::Sender<RpcMessage>,
}

struct PendingGuard {
    inner: Arc<Inner>,
    generation: u64,
    id: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.generation == self.generation {
            state.pending.remove(&self.id);
        }
    }
}

struct InitializationGuard {
    inner: Arc<Inner>,
    generation: u64,
    finished: CancellationToken,
    actor_started: bool,
    complete: bool,
}

impl Drop for InitializationGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.inner
                .disconnect(self.generation, "Codex initialization cancelled");
        }
        if !self.actor_started {
            self.finished.cancel();
        }
    }
}

/// Cloning shares one connection and one initialization sequence.
#[derive(Clone)]
pub struct CodexRpc {
    path: PathBuf,
    timeout: Duration,
    starting: Arc<AsyncMutex<()>>,
    lifecycle: Arc<AsyncMutex<()>>,
    inner: Arc<Inner>,
}

impl CodexRpc {
    pub fn new(socket_path: PathBuf) -> Self {
        Self::with_timeout(socket_path, Duration::from_secs(30))
    }

    pub fn with_timeout(socket_path: PathBuf, request_timeout: Duration) -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            path: socket_path,
            timeout: request_timeout,
            starting: Arc::new(AsyncMutex::new(())),
            lifecycle: Arc::new(AsyncMutex::new(())),
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                events,
            }),
        }
    }

    async fn send_request(&self, method: &str, params: Value, initializing: bool) -> Result<Value> {
        let (reply, receiver) = oneshot::channel();
        let (id, generation) = {
            let mut state = self.inner.state.lock().unwrap();
            if !initializing && !state.ready {
                bail!("Codex is not initialized");
            }
            state.sequence += 1;
            let id = format!("bridge-{}-{}", state.generation, state.sequence);
            let writer = state
                .connection
                .as_ref()
                .context("Codex is disconnected")?
                .writer
                .clone();
            state.pending.insert(id.clone(), reply);
            if writer
                .send(Message::Text(
                    json!({"id":id,"method":method,"params":params})
                        .to_string()
                        .into(),
                ))
                .is_err()
            {
                state.pending.remove(&id);
                bail!("Codex input stream closed; execution outcome may be unknown");
            }
            (id, state.generation)
        };
        // Cancelled callers also release their pending slot. Requests are never replayed.
        let _pending = PendingGuard {
            inner: self.inner.clone(),
            generation,
            id,
        };
        match timeout(self.timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!(
                "Codex connection closed; execution outcome may be unknown"
            )),
            Err(_) => {
                bail!("Codex RPC timed out: {method}; execution outcome may be unknown")
            }
        }
    }

    fn write(&self, value: Value) -> Result<()> {
        let state = self.inner.state.lock().unwrap();
        state
            .connection
            .as_ref()
            .context("Codex is disconnected")?
            .writer
            .send(Message::Text(value.to_string().into()))
            .map_err(|_| anyhow!("Codex input stream closed"))
    }
}

impl Inner {
    fn disconnect(&self, generation: u64, message: &str) {
        let mut state = self.state.lock().unwrap();
        if generation != state.generation {
            return;
        }
        let Some(connection) = state.connection.take() else {
            return;
        };
        state.ready = false;
        connection.cancel.cancel();
        for (_, reply) in state.pending.drain() {
            let _ = reply.send(Err(anyhow!("{message}; execution outcome may be unknown")));
        }
        let _ = self.events.send(RpcMessage {
            id: None,
            method: "bridge/disconnected".into(),
            params: json!({"message":message}),
        });
    }

    fn receive(&self, generation: u64, text: &str) -> Result<()> {
        let value: Value = serde_json::from_str(text).context("Invalid Codex protocol message")?;
        let object = value.as_object().context("Invalid Codex protocol object")?;
        let mut state = self.state.lock().unwrap();
        if state.generation != generation || state.connection.is_none() {
            return Ok(());
        }
        if let Some(method) = object.get("method").and_then(Value::as_str) {
            let id = object.get("id").cloned();
            if id.as_ref().is_some_and(|id| !valid_id(id)) {
                bail!("Invalid Codex protocol id");
            }
            let _ = self.events.send(RpcMessage {
                id,
                method: method.into(),
                params: object.get("params").cloned().unwrap_or(Value::Null),
            });
            return Ok(());
        }
        let id = object
            .get("id")
            .filter(|id| valid_id(id))
            .context("Invalid Codex protocol response")?;
        // Our own IDs are always strings. Numeric server IDs cannot collide with them.
        let Some(id) = id.as_str() else {
            return Ok(());
        };
        let Some(reply) = state.pending.remove(id) else {
            return Ok(());
        };
        let result = if let Some(error) = object.get("error").and_then(Value::as_object) {
            Err(CodexRpcError {
                code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Codex RPC failed")
                    .into(),
                data: error.get("data").cloned(),
            }
            .into())
        } else {
            object
                .get("result")
                .cloned()
                .context("Invalid Codex RPC response")
        };
        let _ = reply.send(result);
        Ok(())
    }
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

#[async_trait]
impl CodexPort for CodexRpc {
    async fn start(&self) -> Result<()> {
        let _starting = self.starting.lock().await;
        // Close may cancel initialization without waiting on `starting`. A later
        // start registers its new connection only after that close has finished.
        let lifecycle = self.lifecycle.lock().await;
        let cancel = CancellationToken::new();
        let finished = CancellationToken::new();
        let (writer, mut outgoing) = mpsc::unbounded_channel();
        let generation = {
            let mut state = self.inner.state.lock().unwrap();
            if state.ready {
                return Ok(());
            }
            state.generation += 1;
            state.connection = Some(Connection {
                writer,
                cancel: cancel.clone(),
                finished: finished.clone(),
            });
            state.generation
        };
        drop(lifecycle);
        let mut initialization = InitializationGuard {
            inner: self.inner.clone(),
            generation,
            finished: finished.clone(),
            actor_started: false,
            complete: false,
        };
        let connect = async {
            let stream = UnixStream::connect(&self.path).await?;
            let config = WebSocketConfig::default()
                .max_message_size(Some(FRAME_LIMIT))
                .max_frame_size(Some(FRAME_LIMIT));
            let (socket, _) =
                client_async_with_config("ws://localhost/", stream, Some(config)).await?;
            Ok::<_, anyhow::Error>(socket)
        };
        let connected = tokio::select! {
            _ = cancel.cancelled() => Err(anyhow!("Codex client closed during connection")),
            result = timeout(self.timeout, connect) => result.context("Codex connection timed out").and_then(|result| result),
        };
        let mut socket = match connected {
            Ok(socket) => socket,
            Err(error) => {
                self.inner
                    .disconnect(generation, "Shared Codex App Server connection failed");
                finished.cancel();
                return Err(error).with_context(|| {
                    format!(
                        "Cannot connect to existing Codex App Server at {}",
                        self.path.display()
                    )
                });
            }
        };
        let inner = self.inner.clone();
        let actor_cancel = cancel.clone();
        tokio::spawn(async move {
            let message = loop {
                tokio::select! {
                    biased;
                    _ = actor_cancel.cancelled() => {
                        let _ = timeout(Duration::from_millis(500), socket.close(None)).await;
                        break "Codex client closed".to_string();
                    }
                    outgoing = outgoing.recv() => {
                        let Some(outgoing) = outgoing else { break "Codex input stream closed".into(); };
                        let sent = tokio::select! {
                            _ = actor_cancel.cancelled() => break "Codex client closed".into(),
                            sent = socket.send(outgoing) => sent,
                        };
                        if sent.is_err() { break "Codex input stream failed".into(); }
                    }
                    incoming = socket.next() => match incoming {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(error) = inner.receive(generation, &text) { break error.to_string(); }
                        }
                        Some(Ok(Message::Ping(_))) => {
                            let flushed = tokio::select! {
                                _ = actor_cancel.cancelled() => break "Codex client closed".into(),
                                flushed = socket.flush() => flushed,
                            };
                            if flushed.is_err() { break "Codex input stream failed".into(); }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => break "Shared Codex App Server connection closed".into(),
                        Some(Ok(_)) => break "Invalid Codex protocol message: expected a text frame".into(),
                        Some(Err(_)) => break "Shared Codex App Server connection failed".into(),
                    }
                }
            };
            inner.disconnect(generation, &message);
            finished.cancel();
        });
        initialization.actor_started = true;
        let initialized = self.send_request("initialize", json!({
            "clientInfo":{"name":"codex_wechat","title":"Codex WeChat","version":env!("CARGO_PKG_VERSION")},
            "capabilities":{"experimentalApi":true}
        }), true).await;
        if let Err(error) = initialized {
            self.inner
                .disconnect(generation, "Codex initialization failed");
            return Err(error);
        }
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.generation != generation || state.connection.is_none() || cancel.is_cancelled()
            {
                bail!("Codex disconnected during initialization");
            }
            state
                .connection
                .as_ref()
                .unwrap()
                .writer
                .send(Message::Text(
                    json!({"method":"initialized"}).to_string().into(),
                ))
                .map_err(|_| anyhow!("Codex disconnected during initialization"))?;
            state.ready = true;
        }
        initialization.complete = true;
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        self.send_request(method, params, false).await
    }

    async fn respond(&self, id: Value, result: Value) -> Result<()> {
        if !valid_id(&id) {
            bail!("Invalid Codex RPC id");
        }
        self.write(json!({"id":id,"result":result}))
    }

    async fn respond_error(&self, id: Value, code: i64, message: &str) -> Result<()> {
        if !valid_id(&id) {
            bail!("Invalid Codex RPC id");
        }
        self.write(json!({"id":id,"error":{"code":code,"message":message}}))
    }

    fn subscribe(&self) -> broadcast::Receiver<RpcMessage> {
        self.inner.events.subscribe()
    }

    async fn close(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let connection = {
            let state = self.inner.state.lock().unwrap();
            state
                .connection
                .as_ref()
                .map(|connection| (state.generation, connection.finished.clone()))
        };
        if let Some((generation, finished)) = connection {
            self.inner.disconnect(generation, "Codex client closed");
            timeout(Duration::from_secs(1), finished.cancelled())
                .await
                .context("Codex connection did not close")?;
        }
        Ok(())
    }
}
