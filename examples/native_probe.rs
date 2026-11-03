//! Explicit live verification: uses its own persistent thread and no WeChat API.
use anyhow::{Context, Result, bail};
use codex_wechat::{
    bridge::Bridge,
    codex::CodexRpc,
    models::{Inbound, Run},
    ports::CodexPort,
    store::Store,
};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

async fn input(store: &Store, bridge: &Bridge, id: &str, text: &str) -> Result<()> {
    store.accept_batch(
        id,
        vec![Inbound {
            id: id.into(),
            peer: "native-probe".into(),
            text: text.into(),
            context_token: "local-only-no-wechat".into(),
            notice: None,
        }],
    )?;
    bridge.handle(store.read()?.inbox[id].clone()).await?;
    if let Some(error) = store.read()?.inbox[id].error.clone() {
        bail!("{error}");
    }
    Ok(())
}
async fn wait(store: &Store, marker: &str) -> Result<Run> {
    let deadline = Instant::now() + Duration::from_secs(100);
    loop {
        let state = store.read()?;
        if let Some(run) = state.runs.values().last() {
            if run.status == "failed" || run.status == "interrupted" {
                bail!(
                    "Native task failed: {}",
                    state
                        .outbox
                        .last()
                        .map(|m| m.text.as_str())
                        .unwrap_or("unknown")
                );
            }
            if run.status == "completed" && state.outbox.iter().any(|m| m.text.contains(marker)) {
                return Ok(run.clone());
            }
        }
        if Instant::now() >= deadline {
            bail!("Timed out waiting for native task");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cwd = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("Usage: native_probe <workspace>")?
        .canonicalize()?;
    let home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or(PathBuf::from(std::env::var_os("HOME").context("HOME missing")?).join(".codex"));
    let path = home.join("app-server-control/app-server-control.sock");
    let codex = Arc::new(CodexRpc::new(path.clone()));
    let observer = CodexRpc::new(path);
    let store = Arc::new(Store::memory()?);
    let bridge = Arc::new(Bridge::new(
        store.clone(),
        codex.clone(),
        cwd.clone(),
        "native-probe".into(),
    ));
    let cancel = CancellationToken::new();
    let mut rx = codex.subscribe();
    let event_bridge = bridge.clone();
    let event_cancel = cancel.clone();
    let events = tokio::spawn(async move {
        loop {
            tokio::select! {_=event_cancel.cancelled()=>return Ok::<_,anyhow::Error>(()),event=rx.recv()=>{event_bridge.on_codex(event?).await?;}}
        }
    });
    let result=async {
  codex.start().await?;observer.start().await?;bridge.connected().await?;
  input(&store,&bridge,"create","/new").await?;
  let id=store.read()?.bindings["native-probe"].clone();
  input(&store,&bridge,"turn1","This is a codex-wechat Rust shared-runtime check. Reply only CW_RUST_OK. Do not use tools.").await?;
  let first=wait(&store,"CW_RUST_OK").await?;
  observer.request("thread/name/set",json!({"threadId":id,"name":"codex-wechat Rust shared-runtime verification"})).await?;
  let page=observer.request("thread/list",json!({"cwd":cwd,"sortKey":"updated_at","limit":100})).await?;
  let visible=page["data"].as_array().is_some_and(|rows|rows.iter().any(|r|r["id"]==id));
  anyhow::ensure!(visible,"Bridge thread not visible in native session list");
  observer.request("thread/resume",json!({"threadId":id})).await?;
  let history=observer.request("thread/read",json!({"threadId":id,"includeTurns":true})).await?;
  anyhow::ensure!(history.to_string().contains("CW_RUST_OK"),"Native history missing bridge output");
  codex.close().await?;
  tokio::time::sleep(Duration::from_millis(100)).await;
  codex.start().await?;bridge.connected().await?;
  observer.request("turn/start",json!({"threadId":id,"input":[{"type":"text","text":"This turn comes from another native client. Reply only CW_RUST_NATIVE_OK. Do not use tools."}]})).await?;
  let second=wait(&store,"CW_RUST_NATIVE_OK").await?;
  input(&store,&bridge,"list","/list").await?;
  let state=store.read()?;
  let choices=&state.switch_choices["native-probe"];
  let index=choices.iter().position(|c|c.thread_id==id).context("Probe thread missing in numbered list")?;
  input(&store,&bridge,"select",&format!("/switch {}",index+1)).await?;
  // Switching focus must mute a real native turn without interrupting it.
  input(&store,&bridge,"muted-turn","This task verifies background completion. Reply only CW_RUST_MUTED_OK. Do not use tools.").await?;
  let muted=store.read()?.runs.values().last().context("Missing muted probe run")?.clone();
  anyhow::ensure!(muted.active(),"Probe turn finished before focus could switch");
  input(&store,&bridge,"switch-away","/new").await?;
  let selected=store.read()?.bindings["native-probe"].clone();
  anyhow::ensure!(selected!=id,"Focus did not change");
  let deadline=Instant::now()+Duration::from_secs(100);
  loop {
    let state=store.read()?;
    let run=&state.runs[&muted.id];
    if run.status=="completed" { break; }
    anyhow::ensure!(run.active(),"Background probe failed: {}",run.status);
    anyhow::ensure!(Instant::now()<deadline,"Timed out waiting for background completion");
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
  let muted_history=observer.request("thread/read",json!({"threadId":id,"includeTurns":true})).await?;
  anyhow::ensure!(muted_history.to_string().contains("CW_RUST_MUTED_OK"),"Background result missing from native history");
  anyhow::ensure!(store.read()?.outbox.iter().all(|m|!m.text.contains("CW_RUST_MUTED_OK")&&!m.text.starts_with("[done in ")),"Unselected task leaked to WeChat outbox");
  input(&store,&bridge,"return",&format!("/switch {id}")).await?;
  anyhow::ensure!(store.read()?.outbox.iter().all(|m|!m.text.contains("CW_RUST_MUTED_OK")),"Returning replayed muted output");
  println!("{}",serde_json::to_string_pretty(&json!({"threadId":id,"bridgeStatus":first.status,"nativeStatus":second.status,"nativeHistoryVisible":visible,"numericSwitch":true,"switchedAwayTaskCompleted":true,"mutedOutputRemainsInNativeHistory":true,"unselectedOutputSuppressed":true,"realWechatSent":false,"messages":store.read()?.outbox.iter().map(|m|&m.text).collect::<Vec<_>>()}))?);
  Ok::<_,anyhow::Error>(())
 }.await;
    cancel.cancel();
    codex.close().await?;
    observer.close().await?;
    events.await??;
    result
}
