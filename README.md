# codex-wechat

**Your native Codex sessions, from WeChat.**

A small Rust bridge that connects WeChat ClawBot to the Codex shared App Server already running on your machine. Pick a session, send a task, answer an approval, and read the result from your phone.

```text
WeChat ↔ codex-wechat ↔ Codex shared App Server ↔ CLI / desktop
              │                    │
         channel state       threads · tools · models · history
```

Codex keeps the harness: model access, tools, project instructions, skills, permissions and conversation history. The bridge handles messages, session selection and delivery. Model requests and their headers come from the same native process used by your Codex client.

[中文说明](README.zh-CN.md) · [User systemd deployment](docs/deployment.md) · [Architecture](docs/architecture.md)

## Features

- **Native sessions.** Create a thread from WeChat or resume an existing one. Open that same thread in the Codex CLI.
- **Quick switching.** `/list` shows ten recent titles. `/switch 3` selects the third entry from that saved list, even after a restart or changes to the recent-session order.
- **Native reasoning effort.** `/effort` lists the selected thread's model-supported levels. `/effort high` updates the native thread setting for subsequent turns.
- **Every assistant message in the selected thread.** Commentary, partial answers and final answers arrive as individual messages. A separate `[done in 30.1s]` marks the end of a turn. Switching threads stops delivery from the previous thread while its native task continues.
- **Native interactions.** Handle command/file approvals, permission requests, questions and MCP forms using short reply codes. Other Codex clients can resolve the same requests.
- **Durable delivery.** SQLite transactions preserve inbox deduplication, receive cursors, pending replies and fixed task recipients. Only the selected thread delivers to WeChat. Failed sends retry with stable IDs; uncertain task submissions are kept for inspection.
- **One compiled program.** Rust, bundled SQLite and Rustls. Running the bridge requires no Node.js, npm or separate database server.

## Deploy with user systemd

The supported deployment is a **Linux user systemd service**, running as the same user as Codex. The full [deployment guide](docs/deployment.md) covers initial setup, QR login, logs, upgrades and recovery.

You need a working native Codex installation with its shared App Server running, and a WeChat account with ClawBot access. The protocol baseline tested here is Codex **0.162.1**.

For Linux x86_64 or aarch64 with glibc 2.39+, use the [v0.1.0 prebuilt release](https://github.com/kites262/codex-wechat/releases/tag/v0.1.0). Follow the [download and checksum verification steps](docs/deployment.md#2-download-and-install), then continue from the extracted package directory. The archive includes the binary, service files and documentation.

For older GNU/Linux systems or another architecture, build and install a source checkout on the target machine:

```sh
cargo build --locked --release
install -Dm755 target/release/codex-wechat ~/.local/bin/codex-wechat
```

Choose the state directory explicitly; every command requires `--state-dir`. The examples use `~/.config/codex-wechat`:

```sh
install -d -m 700 ~/.config/codex-wechat
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat login
```

Scan the terminal QR code with WeChat and confirm the link. Then install the service files:

```sh
install -Dm600 deploy/service.env.example ~/.config/codex-wechat/service.env
install -Dm644 deploy/codex-wechat.service ~/.config/systemd/user/codex-wechat.service
```

Set `CODEX_WECHAT_CWD` in `~/.config/codex-wechat/service.env` to the absolute project directory, then enable the service:

```sh
systemctl --user daemon-reload
systemctl --user enable --now codex-wechat.service
journalctl --user -u codex-wechat.service -f
```

To keep the service available after logout, enable user lingering as described in the [guide](docs/deployment.md#keep-running-after-logout). Codex's shared service must also remain available. Stopping the bridge disconnects its own client; native Codex tasks continue.

## Chat commands

Start with `/new`, or use `/list` and `/switch 3` to select an existing thread, then send ordinary text. `/switch` requires a number or native thread ID.

| Command | Action |
| --- | --- |
| `/new` | Create and select a native thread in the configured project |
| `/list` | Show up to ten recent titles, numbered 1–10 |
| `/switch 3` | Select entry 3 from the last displayed list |
| `/switch <threadId>` | Resume a thread by its native ID |
| `/status` | Show the selected thread, active turns and interaction codes |
| `/effort [level]` | List supported reasoning-effort levels, or set one for subsequent turns |
| `/stop [threadId]` | Interrupt the selected or specified active turn |
| `/approve <code>` | Accept a native approval or completed URL interaction |
| `/reject <code>` | Decline an interaction |
| `/answer <code> <content>` | Answer a question in plain text, or follow the structured example supplied for multiple questions and forms |
| `/help` | Show available commands |

```text
You: /list

Recent threads:

1. Fix the login redirect

2. Review the release notes *

/switch <number>

You: /switch 1
Switched to: Fix the login redirect

You: Check the failing test and explain the cause.
…native Codex messages…
[done in 30.1s]
```

Use `/effort` after selecting a thread to see its current model's supported levels. The current value is marked with `*` when available. Pass an advertised level, such as `/effort high`, to change it. The bridge validates the level against the native model list and confirms success after Codex reports the updated thread setting. The change applies to subsequent turns; an active turn continues with its existing setting. Codex owns this setting, with no separate effort configuration in the bridge.

For a single question, reply with ordinary text, for example `/answer a1 Use the existing database`. Multiple questions include a JSON example keyed by question ID:

```text
/answer a1 {"answers":{"database":{"answers":["SQLite"]},"region":{"answers":["Tokyo"]}}}
```

For an MCP form, send the requested fields as a JSON object, for example `/answer a2 {"name":"Alice"}`. The bridge wraps those fields in the native form response; you do not add an outer `content` field.

Bridge prompts are in English. Native assistant text keeps its original language and whitespace. Titles and question content keep their original language; list titles are displayed on one line. Thread and turn IDs are omitted from ordinary message prefixes.

## Same session on your computer

Threads created through the bridge are native Codex threads. After the first user message, find one in the CLI session picker or open it directly:

```sh
codex --remote unix:// resume <threadId>
```

When a CLI or desktop client starts another turn on the thread currently selected in WeChat, its assistant messages also reach the phone. A successful `/switch` or `/new` selects the only thread whose messages, turn notices and interaction prompts are delivered. Queued output from other threads is discarded; their native tasks continue. Switching back resumes future delivery without replaying messages muted while away. Open Codex to read that history. A failed switch leaves the selection unchanged. The native client decides when its own history view refreshes or selects a new thread.

## State and recovery

`--state-dir` contains the WeChat account, an instance lock and a bot-specific SQLite database. It stores channel state; Codex owns the conversation history. Existing version 1 bridge databases and account files are compatible.

A numbered selection refers to the most recent successful `/list`. A new `/list` replaces it; an empty list clears the choices.

A lost connection can leave a submitted request's outcome unknown. The bridge keeps the record and does not automatically submit the task again. Use `/status` or the native Codex client to inspect it. Reconnecting or selecting the thread again with `/switch` reconciles native task state; output is delivered only for the selected thread and previously muted messages remain muted. Send retries use stable client IDs, but a lost network acknowledgement can still result in duplicate display.

## Current scope

Single-user private chat, one configured project directory and text messages. Images, voice and files receive an attachment notice; mixed attachment/text messages are not submitted to Codex. Interactions that WeChat cannot present remain available in the native Codex client.

The bridge depends on the native local shared-server protocol and the WeChat iLink service. Account access and service behavior are supplied by those providers. This is an independent integration.

## Development

Build requirements: Rust 1.88 or newer, Cargo and a C compiler for bundled SQLite and TLS dependencies. The release lockfile is committed for reproducible dependency resolution.

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked --release
```

Tests use temporary state, fake Unix WebSocket servers and mock/local HTTP transports. They cover native RPC, sessions, delivery, interactions, Unicode, timeouts, shutdown and TypeScript state compatibility. See [architecture and validation](docs/architecture.md) for implementation boundaries.

## License

[MIT](LICENSE).
