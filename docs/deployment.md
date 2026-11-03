# Deploy with user systemd

Run codex-wechat as a Linux **user service**, under the same account that runs Codex. This guide uses `~/.local/bin/codex-wechat` for the binary and `~/.config/codex-wechat` for state and service settings.

Every CLI command requires an explicit `--state-dir`. Use the same path for login, the service and status checks.

## 1. Prepare native Codex

Install and sign in to the native Codex CLI. Confirm that your intended model can complete a task in Codex before linking WeChat. The App Server protocol baseline tested by this project is 0.162.1.

Use the existing shared service started by the CLI or desktop. Check its version:

```sh
codex app-server daemon version
```

If it has not been started yet, the native CLI provides:

```sh
codex app-server daemon start
```

The bridge connects to `${CODEX_HOME:-$HOME/.codex}/app-server-control/app-server-control.sock`. Its native process supplies model access, credentials, headers and tool execution. A missing socket is reported as a connection error and systemd retries; the bridge does not create a separate runtime.

## 2. Build and install

From a checkout of this repository, using Rust 1.88+ and a C compiler:

```sh
cargo build --locked --release
install -Dm755 target/release/codex-wechat ~/.local/bin/codex-wechat
```

This produces a native executable with bundled SQLite and Rustls. Build on a Linux system compatible with the machine where the service will run.

## 3. Link WeChat

```sh
install -d -m 700 ~/.config/codex-wechat
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat login
```

Scan the terminal QR code using a WeChat account with ClawBot access, and confirm. The command saves `account.json` with mode `0600`. The state directory uses mode `0700`.

If you already have a version 1 bridge state directory, it can be reused with `--state-dir`; a fresh login is unnecessary when the account is still valid. To relocate it, stop the existing service first, copy the complete directory including SQLite WAL files, then point all commands and the unit to the destination. Keep the old directory until the replacement is verified.

## 4. Configure the user service

Install the supplied files:

```sh
install -Dm600 deploy/service.env.example ~/.config/codex-wechat/service.env
install -Dm644 deploy/codex-wechat.service ~/.config/systemd/user/codex-wechat.service
```

Edit `~/.config/codex-wechat/service.env`:

```ini
CODEX_WECHAT_CWD="/absolute/path/to/your/project"
```

Set this to the project directory whose native sessions should be available from WeChat. Paths with spaces are supported. If your native client uses a custom Codex home, set the same path here:

```ini
CODEX_HOME="/absolute/path/to/.codex"
```

The supplied [unit](../deploy/codex-wechat.service) runs:

```ini
ExecStart=%h/.local/bin/codex-wechat --state-dir %h/.config/codex-wechat start --cwd ${CODEX_WECHAT_CWD}
```

`%h` expands to the user's home directory. To choose another state location, update the unit's `--state-dir`, its `EnvironmentFile` path if needed, and the matching login/status commands. The `--socket` option of the `start` subcommand can select another existing native socket when required.

## 5. Enable and verify

```sh
systemctl --user daemon-reload
systemctl --user enable --now codex-wechat.service
systemctl --user status codex-wechat.service
journalctl --user -u codex-wechat.service -f
```

The unit restarts after a failure with a 15-second delay. It uses a private umask and gives the bridge up to 30 seconds to close on stop. The unit file uses the installed binary, so rebuilding a source checkout does not change a running service.

In WeChat, send `/new`, then a text message. For an existing thread, send `/list` and choose an entry using `/switch 1` through `/switch 10`. `/switch` requires a number or native thread ID. Only the selected thread delivers messages and prompts. Switching clears pending output from other threads while their Codex tasks continue; switching back resumes future messages without replaying muted output. Read the full history in Codex.

Inspect local channel state without stopping the service:

```sh
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat status
```

The output includes the selected native thread, pending reply count, the first delivery error and recorded turn statuses. You can open the same thread in the native CLI using `codex --remote unix:// resume <threadId>`.

## Keep running after logout

Check whether user lingering is enabled:

```sh
loginctl show-user "$USER" -p Linger
```

Enable it if you want the user manager to remain available after logout:

```sh
loginctl enable-linger "$USER"
```

Your machine's policy may require an administrator to run this command. Codex's own shared service and the network must also remain available. The bridge unit keeps reconnecting when its socket is temporarily unavailable; managing or upgrading the native Codex service remains part of your Codex installation.

## Upgrade

Build and validate the new checkout:

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked --release
```

Stop the channel, replace the executable and restart:

```sh
systemctl --user stop codex-wechat.service
install -Dm755 target/release/codex-wechat ~/.local/bin/codex-wechat
systemctl --user start codex-wechat.service
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat status
```

Keep the state directory in place. Stopping the bridge closes its own connection, leaving the shared Codex process and its tasks running. The bridge rejoins saved bindings and reconciles unfinished tasks with native state after reconnecting. Selecting a thread again with `/switch` also reconciles its native state. Delivery still follows the current selection; output muted by a thread switch is not replayed.

## Re-link an expired account

A WeChat credentials-expired error requires another QR login. Stop the service before opening the login command:

```sh
systemctl --user stop codex-wechat.service
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat login
systemctl --user start codex-wechat.service
```

A different bot identity gets its own SQLite database. Previous bot state remains in the directory.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `--state-dir ... is required` | Supply the same explicit directory used by the unit |
| Cannot connect to existing Codex App Server | Check `daemon version`, the same user/CODEX_HOME, socket path and journal |
| `403`, provider or model error | Check a task in native Codex; the shared process supplies model requests |
| Unknown turn or submission | Inspect `/status` or the native Codex client; reconnect or select the thread again with `/switch` to reconcile state. Uncertain inputs are not automatically replayed |
| `Another instance is using this state directory` | Stop the existing bridge service before login or starting another instance |
| Pending replies | Inspect local `status` for the first error and retry time; incoming WeChat messages refresh reply credentials |
| Numeric choice is invalid | Send `/list` to save a list, then select one of the displayed numbers with `/switch <number>` |
| New blank thread absent from CLI history | Submit its first message, then refresh the native session picker |

The service journal reports operation failures. Account credentials and channel databases stay in the state directory; they are not part of the source checkout or release archive.
