use crate::models::{Outbound, PollBatch, RpcMessage};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait CodexPort: Send + Sync {
    async fn start(&self) -> Result<()>;
    async fn request(&self, method: &str, params: Value) -> Result<Value>;
    async fn respond(&self, id: Value, result: Value) -> Result<()>;
    async fn respond_error(&self, id: Value, code: i64, message: &str) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<RpcMessage>;
    async fn close(&self) -> Result<()>;
}

#[async_trait]
pub trait Channel: Send + Sync {
    async fn poll(&self, cursor: &str, cancel: &CancellationToken) -> Result<PollBatch>;
    async fn send(
        &self,
        message: &Outbound,
        context_token: &str,
        cancel: &CancellationToken,
    ) -> Result<()>;
}
