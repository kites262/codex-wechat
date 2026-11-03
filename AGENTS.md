# codex-wechat

Rust channel bridge for the existing native Codex shared App Server.

- Read README.md and affected modules before editing. The public deployment guide covers user systemd only.
- Codex owns threads, turns, model access, tools, instructions, approvals and history. Connect to its existing Unix WebSocket; never spawn a replacement server or alter inference headers.
- Channel state belongs to SQLite. Preserve version 1 JSON compatibility, bot namespaces, atomic inbox/cursor and durable outbox writes.
- A run keeps its original peer; only the peer's selected thread may deliver native messages and interaction prompts. A successful /switch or /new drops queued output from other threads without interrupting their native tasks. Muted output is not replayed when switching back; Codex retains history. Failed selection preserves the binding. /list saves the recent per-peer choices; numeric /switch selections use that persisted list, not a refreshed ranking.
- Keep /list as the recent-ten thread list and /switch as explicit selection. Reconcile native state on reconnect and selection without replaying uncertain inputs. /answer accepts plain text for a single question and the native structured format for multiple questions or MCP forms.
- /effort uses the selected thread's native model-supported levels and updates its native settings. Validate against model/list and confirm the matching thread/settings/updated event. Codex owns the setting; keep no separate bridge effort configuration.
- Preserve native assistant text and all phases. Do not prefix messages with thread/turn IDs. Bridge-owned prompts and errors use English.
- Shared requests may be answered by another client. Ignore unrelated requests and expire codes on serverRequest/resolved.
- Keep credentials and local state out of the repository. Use synthetic fixtures and temporary state for tests.
- Run cargo fmt --check, cargo clippy --all-targets -- -D warnings, cargo test and cargo build --release before delivery.
- Use Angular commit format with Chinese titles. Never push directly to the main branch.
