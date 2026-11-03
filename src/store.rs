use crate::models::{Inbound, InboxEntry, Outbound, State};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
};
use tokio::sync::watch;
use uuid::Uuid;

pub struct Store {
    db: Mutex<Connection>,
    binding_changes: watch::Sender<u64>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        Self::initialize(&db)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(Self::from_connection(db))
    }

    pub fn memory() -> Result<Self> {
        let db = Connection::open_in_memory()?;
        Self::initialize(&db)?;
        Ok(Self::from_connection(db))
    }

    pub fn read_only(path: &Path) -> Result<Self> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let result = Self::from_connection(db);
        result.read()?;
        Ok(result)
    }

    fn from_connection(db: Connection) -> Self {
        Self {
            db: Mutex::new(db),
            binding_changes: watch::channel(0).0,
        }
    }

    pub fn subscribe_bindings(&self) -> watch::Receiver<u64> {
        self.binding_changes.subscribe()
    }

    fn initialize(db: &Connection) -> Result<()> {
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS bridge_state (id INTEGER PRIMARY KEY CHECK(id = 1), version INTEGER NOT NULL, payload TEXT NOT NULL);")?;
        db.execute(
            "INSERT OR IGNORE INTO bridge_state VALUES (1, 1, ?)",
            [serde_json::to_string(&State::default())?],
        )?;
        read_state(db)?;
        Ok(())
    }

    pub fn read(&self) -> Result<State> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("State lock poisoned"))?;
        read_state(&db)
    }

    pub fn update<F, T>(&self, update: F) -> Result<T>
    where
        F: FnOnce(&mut State) -> Result<T>,
    {
        let mut db = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("State lock poisoned"))?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut state = read_state(&tx)?;
        let before = state.bindings.clone();
        let result = update(&mut state)?;
        tx.execute(
            "UPDATE bridge_state SET payload=? WHERE id=1",
            params![serde_json::to_string(&state)?],
        )?;
        tx.commit()?;
        if state.bindings != before {
            self.binding_changes
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(result)
    }

    pub fn accept_batch(&self, cursor: &str, messages: Vec<Inbound>) -> Result<()> {
        self.update(|state| {
            for message in messages {
                if message.id.is_empty() || message.peer.is_empty() {
                    bail!("Inbound message missing identity");
                }
                if !message.context_token.is_empty() {
                    state
                        .peers
                        .insert(message.peer.clone(), message.context_token.clone());
                }
                state
                    .inbox
                    .entry(message.id.clone())
                    .or_insert_with(|| InboxEntry {
                        message,
                        status: "pending".into(),
                        thread_id: None,
                        error: None,
                    });
            }
            state.cursor = cursor.to_owned();
            Ok(())
        })
    }
}

fn read_state(db: &Connection) -> Result<State> {
    let (version, payload): (i64, String) = db.query_row(
        "SELECT version, payload FROM bridge_state WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if version != 1 {
        bail!("Unsupported state version: {version}");
    }
    serde_json::from_str(&payload).context("Invalid channel state")
}

pub fn enqueue(state: &mut State, peer: &str, text: impl Into<String>) {
    state.outbox.push(Outbound {
        id: Uuid::new_v4().to_string(),
        peer: peer.to_owned(),
        text: text.into(),
        thread_id: None,
        attempts: 0,
        next_attempt_at: 0,
        error: None,
    });
}

/// Queue native output only for the currently selected thread.
pub fn enqueue_thread(
    state: &mut State,
    peer: &str,
    thread_id: &str,
    text: impl Into<String>,
) -> bool {
    if state.bindings.get(peer).map(String::as_str) != Some(thread_id) {
        return false;
    }
    enqueue(state, peer, text);
    state.outbox.last_mut().unwrap().thread_id = Some(thread_id.to_owned());
    true
}

/// Switch channel focus without interrupting any native Codex task.
pub fn select_thread(state: &mut State, peer: &str, thread_id: &str) {
    state.bindings.insert(peer.to_owned(), thread_id.to_owned());
    state.outbox.retain(|message| {
        message.peer != peer
            || message
                .thread_id
                .as_deref()
                .is_none_or(|id| id == thread_id)
    });
    state
        .interactions
        .retain(|_, request| request.peer != peer || request.thread_id == thread_id);
}

pub fn database_path(state_dir: &Path, bot_id: &str) -> PathBuf {
    let hash = format!("{:x}", Sha256::digest(bot_id.as_bytes()));
    state_dir.join(format!("bridge-{}.sqlite", &hash[..32]))
}

pub struct InstanceLock {
    _db: Connection,
}
impl InstanceLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE;")
            .context("Another instance is using this state directory")?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(Self { _db: db })
    }
}
