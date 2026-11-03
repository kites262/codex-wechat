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

## 2. Download and install

The recommended packages are the [v0.1.0 Linux x86_64 binary](https://github.com/kites262/codex-wechat/releases/download/v0.1.0/codex-wechat-v0.1.0-x86_64-unknown-linux-gnu.tar.gz) and [v0.1.0 Linux aarch64 binary](https://github.com/kites262/codex-wechat/releases/download/v0.1.0/codex-wechat-v0.1.0-aarch64-unknown-linux-gnu.tar.gz), with [SHA256SUMS](https://github.com/kites262/codex-wechat/releases/download/v0.1.0/SHA256SUMS). These GNU/Linux packages support **glibc 2.39 or newer**. Each archive includes the executable, README files, license, service files and deployment guide in a single directory.

Run these commands in Bash to select the local architecture, download and verify the package, then enter its directory:

```bash
set -eu
cw_version=v0.1.0
case "$(uname -m)" in
  x86_64) cw_target=x86_64-unknown-linux-gnu ;;
  aarch64|arm64) cw_target=aarch64-unknown-linux-gnu ;;
  *) echo "Unsupported prebuilt architecture: $(uname -m)" >&2; exit 1 ;;
esac
cw_package="codex-wechat-${cw_version}-${cw_target}"
cw_release="https://github.com/kites262/codex-wechat/releases/download/${cw_version}"
cw_download_dir="$(mktemp -d)"
cd "$cw_download_dir"
curl -fL "${cw_release}/${cw_package}.tar.gz" -o "${cw_package}.tar.gz"
curl -fL "${cw_release}/SHA256SUMS" -o SHA256SUMS
awk -v file="${cw_package}.tar.gz" '$2 == file { print; found = 1 } END { if (!found) exit 1 }' SHA256SUMS > SHA256SUMS.selected
sha256sum -c SHA256SUMS.selected
tar -xzf "${cw_package}.tar.gz"
cd "$cw_package"
```

The checksum command verifies only the package selected for this machine. Install the verified executable:

```sh
install -Dm755 codex-wechat ~/.local/bin/codex-wechat
```

Keep this package directory as your working directory for the service-file installation in step 4.

### Build from source

For older GNU/Linux systems or another architecture, build on the machine where the service will run, using Rust 1.88+ and a C compiler. From a checkout of this repository:

```sh
cargo build --locked --release
install -Dm755 target/release/codex-wechat ~/.local/bin/codex-wechat
```

This produces a native executable with bundled SQLite and Rustls. Continue with the same user systemd setup below, using the source checkout as your working directory.

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

Download, verify and extract the new release as in step 2, setting `cw_version` to the desired release tag. From the extracted package directory, stop the channel before replacing its executable:

```sh
systemctl --user stop codex-wechat.service
install -Dm755 codex-wechat ~/.local/bin/codex-wechat
systemctl --user start codex-wechat.service
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat status
```

For a source installation, build the new checkout with `cargo build --locked --release` and use `target/release/codex-wechat` as the source of the `install` command above.

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
