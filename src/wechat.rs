//! Tencent iLink transport. Codex-specific behavior belongs to the bridge.
use crate::{
    models::{Inbound, Outbound, PollBatch},
    ports::Channel,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{
    Url,
    header::{HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const LOGIN_ORIGIN: &str = "https://ilinkai.weixin.qq.com";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(40);
const CHUNK_DELAY: Duration = Duration::from_millis(300);

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    pub bot_id: String,
    pub owner_id: String,
    pub token: String,
    pub base_url: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{}", if *code == -14 { "WeChat credentials expired. Stop the service, run login, then restart".to_owned() } else { format!("WeChat API rejected the request: code={code}") })]
pub struct WechatApiError {
    pub code: i64,
}
impl WechatApiError {
    pub fn requires_login(&self) -> bool {
        self.code == -14
    }
}

#[derive(Debug, thiserror::Error)]
#[error("WeChat request timed out")]
struct RequestTimeout;

struct HttpRequest {
    url: Url,
    headers: HeaderMap,
    body: Option<Value>,
}
struct HttpResponse {
    status: u16,
    body: String,
}
#[async_trait]
trait Transport: Send + Sync {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse>;
}
struct HttpTransport(reqwest::Client);
impl HttpTransport {
    fn new() -> Result<Self> {
        Ok(Self(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        ))
    }
}
#[async_trait]
impl Transport for HttpTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse> {
        let mut builder = if request.body.is_some() {
            self.0.post(request.url)
        } else {
            self.0.get(request.url)
        };
        builder = builder.headers(request.headers);
        if let Some(body) = request.body {
            builder = builder.json(&body);
        }
        let response = builder.send().await.context("WeChat HTTP request failed")?;
        let status = response.status().as_u16();
        // The outer deadline remains active until the entire response is read.
        if !(200..300).contains(&status) {
            bail!("WeChat HTTP request failed: {status}");
        }
        Ok(HttpResponse {
            status,
            body: response
                .text()
                .await
                .context("Could not read WeChat response")?,
        })
    }
}

fn api_origin(value: &str) -> Result<Url> {
    let url = Url::parse(value).context("Invalid WeChat API URL")?;
    let trusted = url
        .host_str()
        .and_then(|host| host.strip_suffix(".weixin.qq.com"))
        .is_some_and(|prefix| {
            prefix.starts_with("ilink")
                && prefix
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        });
    if url.scheme() != "https"
        || !trusted
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("WeChat API URL must use a trusted HTTPS iLink domain");
    }
    Ok(url)
}
fn base_info() -> Value {
    json!({"channel_version":"0.1.0", "bot_agent":"CodexWechat/0.1.0"})
}

async fn request(
    transport: &dyn Transport,
    origin: &str,
    endpoint: &str,
    body: Option<Value>,
    token: Option<&str>,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<Value> {
    let origin = api_origin(origin)?;
    let url = origin.join(endpoint)?;
    let mut headers = HeaderMap::new();
    headers.insert("iLink-App-Id", HeaderValue::from_static("bot"));
    headers.insert("iLink-App-ClientVersion", HeaderValue::from_static("256"));
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    if let Some(token) = token {
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("Invalid WeChat credential")?,
        );
        headers.insert(
            "AuthorizationType",
            HeaderValue::from_static("ilink_bot_token"),
        );
        let uin = STANDARD.encode(rand::random::<u32>().to_string());
        headers.insert("X-WECHAT-UIN", HeaderValue::from_str(&uin)?);
    }
    let operation = async {
        let response = transport
            .execute(HttpRequest { url, headers, body })
            .await?;
        if !(200..300).contains(&response.status) {
            bail!("WeChat HTTP request failed: {}", response.status);
        }
        let result: Value =
            serde_json::from_str(&response.body).context("WeChat API returned invalid JSON")?;
        if !result.is_object() {
            bail!("WeChat API returned an invalid JSON object");
        }
        for field in ["ret", "errcode"] {
            if let Some(value) = result.get(field) {
                let code = value
                    .as_i64()
                    .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
                    .or_else(|| {
                        value
                            .as_f64()
                            .filter(|v| {
                                v.is_finite()
                                    && v.fract() == 0.0
                                    && (i64::MIN as f64..i64::MAX as f64).contains(v)
                            })
                            .map(|v| v as i64)
                    })
                    .with_context(|| format!("WeChat API returned an invalid {field}"))?;
                if code != 0 {
                    return Err(WechatApiError { code }.into());
                }
            }
        }
        Ok(result)
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => bail!("WeChat request cancelled"),
        result = tokio::time::timeout(timeout, operation) => result.map_err(|_| anyhow::Error::new(RequestTimeout)).and_then(|r| r),
    }
}

fn split_text(text: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut units = 0;
    for point in text.chars() {
        if units + point.len_utf16() > 3500 {
            chunks.push(std::mem::take(&mut chunk));
            units = 0;
        }
        chunk.push(point);
        units += point.len_utf16();
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push(chunk);
    }
    chunks
}
fn message_id(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        Value::Number(number) => {
            let digits = number.to_string();
            (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then_some(digits)
        }
        _ => None,
    }
}
fn numeric_is(value: Option<&Value>, expected: i64) -> bool {
    value.and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    }) == Some(expected)
}

pub struct WechatClient {
    account: Account,
    transport: Arc<dyn Transport>,
    timeout: Duration,
    chunk_delay: Duration,
}
impl WechatClient {
    pub fn new(account: Account) -> Result<Self> {
        api_origin(&account.base_url)?;
        if account.token.is_empty() || account.bot_id.is_empty() || account.owner_id.is_empty() {
            bail!("WeChat account information is incomplete");
        }
        Ok(Self {
            account,
            transport: Arc::new(HttpTransport::new()?),
            timeout: REQUEST_TIMEOUT,
            chunk_delay: CHUNK_DELAY,
        })
    }
    fn parse_batch(&self, response: Value, cursor: &str) -> Result<PollBatch> {
        let messages = response
            .get("msgs")
            .map(|v| v.as_array().context("Invalid WeChat inbox batch"))
            .transpose()?;
        let mut batch = PollBatch {
            cursor: cursor.to_owned(),
            messages: Vec::new(),
        };
        for message in messages.into_iter().flatten() {
            if message.get("from_user_id").and_then(Value::as_str) != Some(&self.account.owner_id)
                || numeric_is(message.get("message_type"), 2)
            {
                continue;
            }
            let items = message
                .get("item_list")
                .map(|v| {
                    v.as_array().context(
                        "Invalid WeChat message content list; inbox batch not acknowledged",
                    )
                })
                .transpose()?;
            let mut parts = Vec::new();
            let mut unsupported = false;
            for item in items.into_iter().flatten() {
                if numeric_is(item.get("type"), 1)
                    && item
                        .pointer("/text_item/text")
                        .is_some_and(Value::is_string)
                {
                    parts.push(item["text_item"]["text"].as_str().unwrap_or_default());
                } else {
                    unsupported = true;
                }
            }
            if parts.is_empty() && !unsupported {
                continue;
            }
            let id = message.get("message_id").and_then(message_id).or_else(|| {
                items
                    .into_iter()
                    .flatten()
                    .find_map(|i| i.get("msg_id").and_then(message_id))
            });
            let context_token = message
                .get("context_token")
                .map(|v| {
                    v.as_str().context(
                        "WeChat message has an invalid context_token; inbox batch not acknowledged",
                    )
                })
                .transpose()?
                .unwrap_or_default();
            let id = id.context(
                "WeChat message is missing a stable message_id; inbox batch not acknowledged",
            )?;
            batch.messages.push(Inbound { id: format!("{}:{id}", self.account.owner_id), peer: self.account.owner_id.clone(), text: parts.join("\n"), context_token: context_token.to_owned(), notice: unsupported.then(|| "Unsupported attachment. This message was not sent to Codex. Please send text.".to_owned()) });
        }
        if let Some(value) = response.get("get_updates_buf") {
            batch.cursor = value
                .as_str()
                .context("Invalid WeChat inbox cursor")?
                .to_owned();
        }
        Ok(batch)
    }
}
#[async_trait]
impl Channel for WechatClient {
    async fn poll(&self, cursor: &str, cancel: &CancellationToken) -> Result<PollBatch> {
        match request(
            self.transport.as_ref(),
            &self.account.base_url,
            "ilink/bot/getupdates",
            Some(json!({"get_updates_buf": cursor, "base_info": base_info()})),
            Some(&self.account.token),
            self.timeout,
            cancel,
        )
        .await
        {
            Ok(response) => self.parse_batch(response, cursor),
            Err(error) if error.is::<RequestTimeout>() => Ok(PollBatch {
                cursor: cursor.to_owned(),
                messages: vec![],
            }),
            Err(error) => Err(error),
        }
    }
    async fn send(
        &self,
        message: &Outbound,
        context_token: &str,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if context_token.is_empty() {
            bail!("Cannot send without a WeChat context_token");
        }
        let identity = format!("{:x}", Sha256::digest(message.id.as_bytes()));
        for (index, text) in split_text(&message.text).into_iter().enumerate() {
            if index > 0 {
                pause(self.chunk_delay, cancel).await?;
            }
            request(self.transport.as_ref(), &self.account.base_url, "ilink/bot/sendmessage", Some(json!({"base_info":base_info(),"msg":{"from_user_id":"","to_user_id":message.peer,"client_id":format!("codex-wechat-{}-{index}", &identity[..32]),"message_type":2,"message_state":2,"item_list":[{"type":1,"text_item":{"text":text}}],"context_token":context_token}})), Some(&self.account.token), self.timeout, cancel).await?;
        }
        Ok(())
    }
}
async fn pause(duration: Duration, cancel: &CancellationToken) -> Result<()> {
    tokio::select! { biased; _ = cancel.cancelled() => bail!("WeChat request cancelled"), _ = tokio::time::sleep(duration) => Ok(()) }
}

/// Obtain an account without logging credentials. The callback only receives the QR URL.
pub async fn login(cancel: &CancellationToken, on_qr: impl Fn(&str)) -> Result<Account> {
    login_with(&HttpTransport::new()?, cancel, on_qr).await
}
async fn login_with(
    transport: &dyn Transport,
    cancel: &CancellationToken,
    on_qr: impl Fn(&str),
) -> Result<Account> {
    let operation = async {
        for _ in 0..3 {
            let qr = request(
                transport,
                LOGIN_ORIGIN,
                "ilink/bot/get_bot_qrcode?bot_type=3",
                Some(json!({"local_token_list":[]})),
                None,
                Duration::from_secs(15),
                cancel,
            )
            .await?;
            let code = qr["qrcode"]
                .as_str()
                .context("WeChat did not return a valid sign-in QR code")?;
            let qr_url = qr["qrcode_img_content"]
                .as_str()
                .context("WeChat did not return a valid sign-in QR code")?;
            let qr_url = Url::parse(qr_url).context("Invalid WeChat sign-in QR code URL")?;
            if qr_url.scheme() != "https" {
                bail!("WeChat sign-in QR code URL must use HTTPS");
            }
            on_qr(qr_url.as_str());
            let mut origin = LOGIN_ORIGIN.to_owned();
            loop {
                let mut status_url = api_origin(&origin)?.join("ilink/bot/get_qrcode_status")?;
                status_url.query_pairs_mut().append_pair("qrcode", code);
                let endpoint = format!(
                    "{}?{}",
                    status_url.path().trim_start_matches('/'),
                    status_url.query().unwrap_or_default()
                );
                let result = match request(
                    transport,
                    &origin,
                    &endpoint,
                    None,
                    None,
                    REQUEST_TIMEOUT,
                    cancel,
                )
                .await
                {
                    Err(error) if error.is::<RequestTimeout>() => continue,
                    result => result?,
                };
                match result["status"].as_str() {
                    Some("expired") => break,
                    Some("scaned_but_redirect") => {
                        let host = result["redirect_host"]
                            .as_str()
                            .context("WeChat sign-in redirect is missing its domain")?;
                        origin = api_origin(&format!("https://{host}"))?
                            .origin()
                            .ascii_serialization();
                    }
                    Some("confirmed") => {
                        let base_url = match result.get("baseurl") {
                            Some(value) => value
                                .as_str()
                                .context("Invalid WeChat sign-in account fields")?,
                            None => &origin,
                        };
                        let base_url = api_origin(base_url)?.origin().ascii_serialization();
                        let field = |key: &str| -> Result<String> {
                            Ok(result[key].as_str().filter(|s| !s.is_empty()).context("WeChat sign-in result is missing the token, bot ID or owner ID")?.to_owned())
                        };
                        return Ok(Account {
                            bot_id: field("ilink_bot_id")?,
                            owner_id: field("ilink_user_id")?,
                            token: field("bot_token")?,
                            base_url,
                        });
                    }
                    Some("wait" | "scaned") => pause(Duration::from_millis(500), cancel).await?,
                    _ => bail!(
                        "WeChat sign-in requires additional action: {}",
                        result["status"].as_str().unwrap_or("unknown")
                    ),
                }
            }
        }
        bail!("WeChat sign-in QR code expired repeatedly. Sign in again")
    };
    tokio::select! { biased; _ = cancel.cancelled() => bail!("WeChat sign-in cancelled"), result = tokio::time::timeout(Duration::from_secs(8 * 60), operation) => result.context("WeChat sign-in timed out")? }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::Mutex};

    enum Reply {
        Json(Value),
        Raw(&'static str),
        Status(u16),
        Fail,
        Stall,
    }
    #[derive(Default)]
    struct Fake {
        replies: Mutex<VecDeque<Reply>>,
        requests: Mutex<Vec<HttpRequest>>,
    }
    impl Fake {
        fn new(replies: Vec<Reply>) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(replies.into()),
                ..Self::default()
            })
        }
        fn bodies(&self) -> Vec<Value> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.body.clone().unwrap_or(Value::Null))
                .collect()
        }
    }
    #[async_trait]
    impl Transport for Fake {
        async fn execute(&self, request: HttpRequest) -> Result<HttpResponse> {
            self.requests.lock().unwrap().push(request);
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Reply::Json(json!({"ret":0})));
            match reply {
                Reply::Json(value) => Ok(HttpResponse {
                    status: 200,
                    body: value.to_string(),
                }),
                Reply::Raw(body) => Ok(HttpResponse {
                    status: 200,
                    body: body.to_owned(),
                }),
                Reply::Status(status) => Ok(HttpResponse {
                    status,
                    body: String::new(),
                }),
                Reply::Fail => bail!("Connection lost after submission"),
                Reply::Stall => std::future::pending().await,
            }
        }
    }
    fn account() -> Account {
        Account {
            bot_id: "bot".into(),
            owner_id: "owner".into(),
            token: "credential".into(),
            base_url: LOGIN_ORIGIN.into(),
        }
    }
    fn client(fake: Arc<Fake>) -> WechatClient {
        WechatClient {
            account: account(),
            transport: fake,
            timeout: Duration::from_millis(20),
            chunk_delay: Duration::ZERO,
        }
    }
    fn outgoing(text: &str) -> Outbound {
        Outbound {
            id: "outbox-123".into(),
            peer: "owner".into(),
            text: text.into(),
            ..Outbound::default()
        }
    }
    fn qr() -> Reply {
        Reply::Json(
            json!({"qrcode":"opaque & qr", "qrcode_img_content":"https://weixin.qq.com/qr"}),
        )
    }

    // Keep production origin checks intact while exercising reqwest against a local fixture.
    struct LocalTransport {
        inner: HttpTransport,
        target: Url,
    }
    #[async_trait]
    impl Transport for LocalTransport {
        async fn execute(&self, mut request: HttpRequest) -> Result<HttpResponse> {
            request.url = self.target.clone();
            self.inner.execute(request).await
        }
    }
    async fn local_response(
        response: &'static [u8],
        stall: bool,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 8192];
            let _ = stream.read(&mut buffer).await.unwrap();
            stream.write_all(response).await.unwrap();
            if stall {
                std::future::pending::<()>().await;
            }
        });
        (url, task)
    }

    #[tokio::test]
    async fn actual_http_redirects_are_rejected_and_body_deadline_is_enforced() {
        let (target, server) = local_response(
            b"HTTP/1.1 302 Found\r\nLocation: https://evil.test/\r\nContent-Length: 0\r\n\r\n",
            false,
        )
        .await;
        let transport = LocalTransport {
            inner: HttpTransport::new().unwrap(),
            target,
        };
        let error = request(
            &transport,
            LOGIN_ORIGIN,
            "test",
            None,
            None,
            Duration::from_secs(2),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("302"));
        server.await.unwrap();
        let (target, server) =
            local_response(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n{", true).await;
        let transport = LocalTransport {
            inner: HttpTransport::new().unwrap(),
            target,
        };
        let error = request(
            &transport,
            LOGIN_ORIGIN,
            "test",
            None,
            None,
            Duration::from_millis(20),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.is::<RequestTimeout>());
        server.abort();
    }

    #[tokio::test]
    async fn poll_preserves_uint64_whitespace_and_filters_owners_and_echoes() {
        let fake = Fake::new(vec![Reply::Raw(
            r#"{"get_updates_buf":"next","msgs":[{"message_type":1,"message_id":18446744073709551615,"from_user_id":"owner","context_token":"ctx","item_list":[{"type":1,"text_item":{"text":"  line\n\n"}}]},{"message_id":42,"from_user_id":"owner","item_list":[{"type":1,"text_item":{"text":"caption"}},{"type":2}]},{"from_user_id":"other","item_list":"invalid"},{"from_user_id":"owner","message_type":2,"item_list":"invalid"}]}"#,
        )]);
        let batch = client(fake.clone())
            .poll("old", &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(batch.cursor, "next");
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].id, "owner:18446744073709551615");
        assert_eq!(batch.messages[0].text, "  line\n\n");
        assert_eq!(batch.messages[1].text, "caption");
        assert_eq!(batch.messages[1].context_token, "");
        assert!(
            batch.messages[1]
                .notice
                .as_ref()
                .unwrap()
                .contains("Unsupported attachment")
        );
        assert_eq!(fake.bodies()[0]["get_updates_buf"], "old");
    }

    #[tokio::test]
    async fn item_id_fallback_is_lossless_without_context() {
        let fake = Fake::new(vec![Reply::Raw(
            r#"{"msgs":[{"from_user_id":"owner","item_list":[{"msg_id":18446744073709551615,"type":1,"text_item":{"text":"hello"}}]}]}"#,
        )]);
        let batch = client(fake)
            .poll("old", &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(batch.cursor, "old");
        assert_eq!(batch.messages[0].id, "owner:18446744073709551615");
        assert!(batch.messages[0].context_token.is_empty());
    }

    #[tokio::test]
    async fn malformed_batches_fail_before_acknowledgment() {
        for payload in [
            json!({"msgs":{}}),
            json!({"get_updates_buf":123}),
            json!({"msgs":[{"from_user_id":"owner","item_list":123}]}),
            json!({"get_updates_buf":"advanced","msgs":[{"from_user_id":"owner","item_list":[{"type":1,"text_item":{"text":"hello"}}]}]}),
            json!({"msgs":[{"from_user_id":"owner","message_id":1,"context_token":2,"item_list":[{"type":1,"text_item":{"text":"hello"}}]}]}),
        ] {
            assert!(
                client(Fake::new(vec![Reply::Json(payload)]))
                    .poll("old", &CancellationToken::new())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn send_checks_http_and_business_errors_without_exposing_credentials() {
        for payload in [json!({"ret":-14}), json!({"errcode":"-1"})] {
            let code = payload.get("ret").map(|_| -14).unwrap_or(-1);
            for sending in [true, false] {
                let c = client(Fake::new(vec![Reply::Json(payload.clone())]));
                let cancel = CancellationToken::new();
                let err = if sending {
                    c.send(&outgoing("hello"), "ctx", &cancel)
                        .await
                        .unwrap_err()
                } else {
                    c.poll("", &cancel).await.err().unwrap()
                };
                let api = err.downcast_ref::<WechatApiError>().unwrap();
                assert_eq!(api.code, code);
                assert_eq!(api.requires_login(), code == -14);
                assert!(!err.to_string().contains("credential") || code == -14);
            }
        }
        let err = client(Fake::new(vec![Reply::Status(503)]))
            .send(&outgoing("hello"), "ctx", &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("503"));
        for raw in ["[]", "null", "{bad", "{\"ret\":\"bad\"}"] {
            assert!(
                client(Fake::new(vec![Reply::Raw(raw)]))
                    .poll("", &CancellationToken::new())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn partial_send_reuses_chunk_ids_and_preserves_unicode() {
        let fake = Fake::new(vec![Reply::Json(json!({})), Reply::Fail]);
        let c = client(fake.clone());
        let text = format!(" {}😀\n{}\n\n", "x".repeat(3498), "汉".repeat(3500));
        let message = outgoing(&text);
        let cancel = CancellationToken::new();
        assert!(c.send(&message, "ctx", &cancel).await.is_err());
        let mut retried = message.clone();
        retried.attempts = 1;
        c.send(&retried, "new-context", &cancel).await.unwrap();
        let bodies = fake.bodies();
        assert_eq!(bodies[0]["msg"]["client_id"], bodies[2]["msg"]["client_id"]);
        assert_eq!(bodies[1]["msg"]["client_id"], bodies[3]["msg"]["client_id"]);
        let mut reassembled = String::new();
        for body in &bodies[2..] {
            let chunk = body["msg"]["item_list"][0]["text_item"]["text"]
                .as_str()
                .unwrap();
            assert!(chunk.encode_utf16().count() <= 3500);
            assert_eq!(body["msg"]["context_token"], "new-context");
            reassembled.push_str(chunk);
        }
        assert_eq!(reassembled, text);
        assert_eq!(
            bodies[0]["msg"]["client_id"],
            "codex-wechat-a53b2b39e946e930d0b6b3ec2f7f4aac-0"
        );
    }

    #[tokio::test]
    async fn timeout_covers_body_and_only_poll_converts_it_to_empty_batch() {
        let fake = Fake::new(vec![Reply::Stall, Reply::Stall]);
        let c = client(fake);
        let cancel = CancellationToken::new();
        assert!(
            c.send(&outgoing("hello"), "ctx", &cancel)
                .await
                .unwrap_err()
                .is::<RequestTimeout>()
        );
        let batch = c.poll("unchanged", &cancel).await.unwrap();
        assert_eq!(batch.cursor, "unchanged");
        assert!(batch.messages.is_empty());
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_body_and_pacing_and_prevents_fetch() {
        let fake = Fake::new(vec![Reply::Stall]);
        let c = client(fake.clone());
        let cancel = CancellationToken::new();
        let operation = c.poll("", &cancel);
        let abort = async {
            tokio::task::yield_now().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(operation, abort);
        assert!(result.unwrap_err().to_string().contains("cancelled"));
        assert!(c.send(&outgoing("hello"), "ctx", &cancel).await.is_err());
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
        let fake = Fake::new(vec![]);
        let mut c = client(fake.clone());
        c.chunk_delay = Duration::from_secs(60);
        let cancel = CancellationToken::new();
        let message = outgoing(&"x".repeat(3501));
        let operation = c.send(&message, "ctx", &cancel);
        let abort = async {
            tokio::task::yield_now().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(operation, abort);
        assert!(result.is_err());
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_context_fails_without_http_and_auth_headers_are_correct() {
        let fake = Fake::new(vec![]);
        let c = client(fake.clone());
        let cancel = CancellationToken::new();
        assert!(c.send(&outgoing("hello"), "", &cancel).await.is_err());
        assert!(fake.requests.lock().unwrap().is_empty());
        c.send(&outgoing("hello"), "ctx", &cancel).await.unwrap();
        let requests = fake.requests.lock().unwrap();
        let headers = &requests[0].headers;
        assert_eq!(headers["Authorization"], "Bearer credential");
        assert_eq!(headers["AuthorizationType"], "ilink_bot_token");
        let decoded = STANDARD
            .decode(headers["X-WECHAT-UIN"].to_str().unwrap())
            .unwrap();
        assert!(String::from_utf8(decoded).unwrap().parse::<u32>().is_ok());
    }

    #[tokio::test]
    async fn login_validates_redirect_and_final_origin_without_auth_headers() {
        let fake = Fake::new(vec![
            qr(),
            Reply::Json(
                json!({"status":"scaned_but_redirect","redirect_host":"ilinkai2.weixin.qq.com"}),
            ),
            Reply::Json(
                json!({"status":"confirmed","bot_token":"secret","ilink_bot_id":"bot-id","ilink_user_id":"owner-id","baseurl":"https://ilinkai2.weixin.qq.com/"}),
            ),
        ]);
        let shown = Mutex::new(Vec::new());
        let account = login_with(fake.as_ref(), &CancellationToken::new(), |url| {
            shown.lock().unwrap().push(url.to_owned())
        })
        .await
        .unwrap();
        assert_eq!(account.base_url, "https://ilinkai2.weixin.qq.com");
        assert_eq!(account.owner_id, "owner-id");
        assert_eq!(
            shown.lock().unwrap().as_slice(),
            ["https://weixin.qq.com/qr"]
        );
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests[2].url.host_str(), Some("ilinkai2.weixin.qq.com"));
        assert_eq!(
            requests[1]
                .url
                .query_pairs()
                .find(|(k, _)| k == "qrcode")
                .unwrap()
                .1,
            "opaque & qr"
        );
        assert!(
            requests
                .iter()
                .all(|r| !r.headers.contains_key("Authorization"))
        );
    }

    #[tokio::test]
    async fn login_rejects_untrusted_redirect_or_credential_origin_before_third_request() {
        for result in [
            json!({"status":"scaned_but_redirect","redirect_host":"evil.test"}),
            json!({"status":"confirmed","baseurl":"https://evil.test","bot_token":"secret","ilink_bot_id":"bot","ilink_user_id":"owner"}),
        ] {
            let fake = Fake::new(vec![qr(), Reply::Json(result)]);
            let err = login_with(fake.as_ref(), &CancellationToken::new(), |_| {})
                .await
                .err()
                .unwrap();
            assert!(err.to_string().contains("trusted"));
            assert_eq!(fake.requests.lock().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn login_refreshes_expired_qr_only_three_times() {
        let fake = Fake::new(vec![
            qr(),
            Reply::Json(json!({"status":"expired"})),
            qr(),
            Reply::Json(json!({"status":"expired"})),
            qr(),
            Reply::Json(json!({"status":"expired"})),
        ]);
        let error = login_with(fake.as_ref(), &CancellationToken::new(), |_| {})
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("expired repeatedly"));
        assert_eq!(fake.requests.lock().unwrap().len(), 6);
    }
}
