use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use codex_wechat::{
    bridge::Bridge,
    codex::CodexRpc,
    ports::CodexPort,
    runtime::run_bridge,
    store::{InstanceLock, Store, database_path},
    wechat::{Account, WechatClient, login},
};
use qrcode::{QrCode, render::unicode};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "WeChat channel for the native Codex shared runtime")]
struct Cli {
    #[arg(long, global = true, help = "State directory (required; no default)")]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Link your WeChat account by scanning a QR code.
    Login,
    /// Connect the WeChat channel to the existing shared Codex App Server.
    Start {
        #[arg(long)]
        cwd: PathBuf,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Read local channel state while the service is running.
    Status,
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}
fn default_socket() -> Result<PathBuf> {
    Ok(std::env::var_os("CODEX_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or(home()?.join(".codex"))
        .join("app-server-control/app-server-control.sock"))
}
fn private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}
fn save_account(path: &Path, account: &Account) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(account)?)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = execute(Cli::parse()).await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

async fn execute(cli: Cli) -> Result<()> {
    let state_dir = cli.state_dir.context(
        "--state-dir <directory> is required (for example: --state-dir ~/.config/codex-wechat)",
    )?;
    private_dir(&state_dir)?;
    let account_file = state_dir.join("account.json");
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal = tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
        signal_cancel.cancel();
        Ok::<_, std::io::Error>(())
    });
    let result = async {
        let _lock = if matches!(cli.command, Command::Status) { None } else { Some(InstanceLock::acquire(&state_dir.join("instance.sqlite"))?) };
        if matches!(cli.command, Command::Login) {
            let account = login(&cancel, |url| {
                println!("Scan with WeChat to sign in:\n{url}");
                if let Ok(qr) = QrCode::new(url) { println!("{}", qr.render::<unicode::Dense1x2>().quiet_zone(true).build()); }
            }).await?;
            save_account(&account_file, &account)?;
            println!("WeChat signed in. Credentials saved to {}", account_file.display());
            return Ok(());
        }
        if !account_file.exists() {
            if matches!(cli.command, Command::Status) { println!("Not signed in. Run login with the same --state-dir first."); return Ok(()); }
            bail!("Run login with the same --state-dir to link WeChat first");
        }
        let account: Account = serde_json::from_slice(&fs::read(&account_file)?).context("Invalid WeChat account file")?;
        if account.owner_id.is_empty() || account.token.is_empty() || account.bot_id.is_empty() { bail!("WeChat credentials are missing required fields. Sign in again"); }
        let file = database_path(&state_dir, &account.bot_id);
        match cli.command {
            Command::Status => {
                if !file.exists() { println!("Not started. Run login, then enable the user systemd service."); return Ok(()); }
                let state = Store::read_only(&file)?.read()?;
                let pending = state.outbox.first().map(|p| json!({"attempts":p.attempts,"error":p.error,"nextAttemptAt":p.next_attempt_at}));
                println!("{}", serde_json::to_string_pretty(&json!({"bindings":state.bindings,"pendingReplies":state.outbox.len(),"firstPendingReply":pending,"unknownMessages":state.inbox.values().filter(|m| m.status=="unknown").count(),"runs":state.runs.values().map(|r| json!({"threadId":r.thread_id,"turnId":r.turn_id,"status":r.status})).collect::<Vec<_>>()}))?);
                Ok(())
            }
            Command::Start { cwd, socket } => {
                let cwd = fs::canonicalize(cwd).context("Invalid working directory")?;
                if !cwd.is_dir() { bail!("The working directory must be a directory"); }
                let socket = socket.map(Ok).unwrap_or_else(default_socket)?;
                let store = Arc::new(Store::open(&file)?);
                let codex: Arc<dyn CodexPort> = Arc::new(CodexRpc::new(socket));
                let bridge = Arc::new(Bridge::new(store.clone(), codex.clone(), cwd.clone(), account.owner_id.clone()));
                let owner = account.owner_id.clone();
                let channel = Arc::new(WechatClient::new(account)?);
                println!("Starting codex-wechat with the shared Codex App Server. Working directory: {}", cwd.display());
                run_bridge(store, bridge, codex, channel, owner, cancel).await
            }
            Command::Login => unreachable!(),
        }
    }.await;
    signal.abort();
    result
}
