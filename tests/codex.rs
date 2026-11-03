use codex_wechat::{
    codex::{CodexRpc, CodexRpcError},
    ports::CodexPort,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::UnixListener, time::timeout};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
    },
};
use tokio_util::sync::CancellationToken;

// Tungstenite's handshake callback fixes this error type to an HTTP response.
#[allow(clippy::result_large_err)]
fn inspect_handshake(request: &Request, response: Response) -> Result<Response, ErrorResponse> {
    assert_eq!(request.uri().path(), "/");
    assert!(!request.headers().contains_key("authorization"));
    assert!(!request.headers().contains_key("user-agent"));
    Ok(response)
}

struct Fixture {
    client: CodexRpc,
    _directory: TempDir,
    cancel: CancellationToken,
    messages: Arc<Mutex<Vec<Value>>>,
    connections: Arc<AtomicUsize>,
    initialize: Arc<AtomicBool>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Fixture {
    async fn new(initialize: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("codex.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = CodexRpc::with_timeout(path, Duration::from_millis(300));
        let cancel = CancellationToken::new();
        let messages = Arc::new(Mutex::new(Vec::<Value>::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let initialize = Arc::new(AtomicBool::new(initialize));
        let (server_cancel, server_messages, server_connections, server_initialize) = (
            cancel.clone(),
            messages.clone(),
            connections.clone(),
            initialize.clone(),
        );
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = server_cancel.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let (cancel, messages, connections, initialize) = (
                    server_cancel.clone(),
                    server_messages.clone(),
                    server_connections.clone(),
                    server_initialize.clone(),
                );
                tokio::spawn(async move {
                    let mut socket = accept_hdr_async(accepted.0, inspect_handshake)
                        .await
                        .unwrap();
                    connections.fetch_add(1, Ordering::SeqCst);
                    let mut initialized = false;
                    while let Some(incoming) = tokio::select! {
                        _ = cancel.cancelled() => None,
                        incoming = socket.next() => incoming,
                    } {
                        let Ok(Message::Text(text)) = incoming else {
                            break;
                        };
                        let value: Value = serde_json::from_str(&text).unwrap();
                        messages.lock().unwrap().push(value.clone());
                        let result = match value["method"].as_str() {
                            Some("initialize") if initialize.load(Ordering::SeqCst) => Some(
                                json!({"id":value["id"],"result":{"userAgent":"Codex Desktop/fixture"}}),
                            ),
                            Some("initialized") => {
                                initialized = true;
                                None
                            }
                            Some("echo") => Some(
                                json!({"id":value["id"],"result":{"initialized":initialized,"params":value["params"]}}),
                            ),
                            Some("fail") => Some(
                                json!({"id":value["id"],"error":{"code":-32602,"message":"fixture rejected","data":{"field":"x"}}}),
                            ),
                            Some("collision") => {
                                socket.send(Message::Text(json!({"id":value["id"],"method":"item/tool/requestUserInput","params":{"threadId":"t"}}).to_string().into())).await.unwrap();
                                Some(json!({"id":value["id"],"result":"client result"}))
                            }
                            Some("disconnect") => {
                                let _ = socket.close(None).await;
                                break;
                            }
                            Some("invalid") => {
                                let _ = socket.send(Message::Text("not-json".into())).await;
                                continue;
                            }
                            Some("binary") => {
                                let _ = socket.send(Message::Binary(vec![1, 2, 3].into())).await;
                                continue;
                            }
                            _ => None,
                        };
                        if let Some(result) = result
                            && socket
                                .send(Message::Text(result.to_string().into()))
                                .await
                                .is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        Self {
            client,
            _directory: directory,
            cancel,
            messages,
            connections,
            initialize,
        }
    }
}

#[tokio::test]
async fn initializes_once_without_overriding_native_configuration_and_preserves_text() {
    let fixture = Fixture::new(true).await;
    assert!(
        fixture
            .client
            .request("echo", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("not initialized")
    );
    let (first, second) = tokio::join!(fixture.client.start(), fixture.client.start());
    first.unwrap();
    second.unwrap();
    assert_eq!(fixture.connections.load(Ordering::SeqCst), 1);
    let text = "  你好 🦀\n    print(1)\n";
    assert_eq!(
        fixture
            .client
            .request("echo", json!({"text":text}))
            .await
            .unwrap(),
        json!({"initialized":true,"params":{"text":text}})
    );
    {
        let messages = fixture.messages.lock().unwrap();
        assert_eq!(
            messages[0]["params"],
            json!({"clientInfo":{"name":"codex_wechat","title":"Codex WeChat","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})
        );
    }
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn server_request_id_collision_does_not_consume_client_response() {
    let fixture = Fixture::new(true).await;
    let mut events = fixture.client.subscribe();
    fixture.client.start().await.unwrap();
    assert_eq!(
        fixture
            .client
            .request("collision", json!({}))
            .await
            .unwrap(),
        "client result"
    );
    let request = timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.method, "item/tool/requestUserInput");
    fixture
        .client
        .respond(request.id.unwrap(), json!({"answers":{}}))
        .await
        .unwrap();
    fixture
        .client
        .respond_error(json!(42), -32601, "unsupported")
        .await
        .unwrap();
    fixture.client.request("echo", json!({})).await.unwrap();
    {
        let messages = fixture.messages.lock().unwrap();
        assert!(
            messages
                .iter()
                .any(|value| value["result"] == json!({"answers":{}}))
        );
        assert!(messages.iter().any(
            |value| value == &json!({"id":42,"error":{"code":-32601,"message":"unsupported"}})
        ));
    }
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn rpc_rejections_are_typed_and_timeouts_are_never_replayed() {
    let fixture = Fixture::new(true).await;
    fixture.client.start().await.unwrap();
    let error = fixture.client.request("fail", json!({})).await.unwrap_err();
    let error = error.downcast_ref::<CodexRpcError>().unwrap();
    assert_eq!(error.code, -32602);
    assert_eq!(error.data, Some(json!({"field":"x"})));
    assert!(
        fixture
            .client
            .request("timeout", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("execution outcome may be unknown")
    );
    fixture.client.request("echo", json!({})).await.unwrap();
    assert_eq!(
        fixture
            .messages
            .lock()
            .unwrap()
            .iter()
            .filter(|value| value["method"] == "timeout")
            .count(),
        1
    );
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn disconnect_rejects_all_pending_once_and_allows_explicit_reconnect() {
    let fixture = Fixture::new(true).await;
    let mut events = fixture.client.subscribe();
    fixture.client.start().await.unwrap();
    let (pending, disconnect) = tokio::join!(
        fixture.client.request("timeout", json!({})),
        fixture.client.request("disconnect", json!({}))
    );
    assert!(
        pending
            .unwrap_err()
            .to_string()
            .contains("connection closed")
    );
    assert!(
        disconnect
            .unwrap_err()
            .to_string()
            .contains("connection closed")
    );
    assert_eq!(events.recv().await.unwrap().method, "bridge/disconnected");
    assert!(events.try_recv().is_err());
    fixture.client.start().await.unwrap();
    fixture.client.request("echo", json!({})).await.unwrap();
    assert_eq!(fixture.connections.load(Ordering::SeqCst), 2);
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn malformed_or_binary_messages_disconnect_instead_of_hanging() {
    let fixture = Fixture::new(true).await;
    fixture.client.start().await.unwrap();
    assert!(
        fixture
            .client
            .request("invalid", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("Invalid Codex protocol")
    );
    fixture.client.start().await.unwrap();
    assert!(
        fixture
            .client
            .request("binary", json!({}))
            .await
            .unwrap_err()
            .to_string()
            .contains("expected a text frame")
    );
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn close_cancels_initialization_and_leaves_shared_server_running() {
    let fixture = Fixture::new(false).await;
    let client = fixture.client.clone();
    let starting = tokio::spawn(async move { client.start().await });
    timeout(Duration::from_secs(1), async {
        while fixture.messages.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.client.close().await.unwrap();
    assert!(
        starting
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("closed")
    );
    fixture.initialize.store(true, Ordering::SeqCst);
    fixture.client.start().await.unwrap();
    fixture.client.request("echo", json!({})).await.unwrap();
    fixture.client.close().await.unwrap();
    fixture.client.start().await.unwrap();
    fixture.client.request("echo", json!({})).await.unwrap();
    assert_eq!(fixture.connections.load(Ordering::SeqCst), 3);
    assert!(
        !fixture
            .messages
            .lock()
            .unwrap()
            .iter()
            .any(|value| matches!(value["method"].as_str(), Some("shutdown" | "terminate")))
    );
    fixture.client.close().await.unwrap();
}

#[tokio::test]
async fn missing_socket_does_not_spawn_an_alternative_server() {
    let directory = tempfile::tempdir().unwrap();
    let client = CodexRpc::new(directory.path().join("missing.sock"));
    for _ in 0..2 {
        assert!(
            client
                .start()
                .await
                .unwrap_err()
                .to_string()
                .contains("Cannot connect to existing Codex App Server")
        );
    }
    client.close().await.unwrap();
}

#[tokio::test]
async fn aborting_start_cleans_up_its_connection_before_a_new_attempt() {
    let fixture = Fixture::new(false).await;
    let client = fixture.client.clone();
    let starting = tokio::spawn(async move { client.start().await });
    timeout(Duration::from_secs(1), async {
        while fixture.messages.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    starting.abort();
    assert!(starting.await.unwrap_err().is_cancelled());
    fixture.initialize.store(true, Ordering::SeqCst);
    fixture.client.start().await.unwrap();
    fixture.client.request("echo", json!({})).await.unwrap();
    fixture.client.close().await.unwrap();
}
