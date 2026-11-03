# Architecture and validation

## Boundaries

```text
WeChat ClawBot ↔ iLink HTTPS ↔ channel adapter ↔ bridge ↔ Unix WebSocket ↔ native Codex
                                               │
                                             SQLite
```

The bridge is a client of the existing shared Codex App Server. It never implements a model/tool loop, starts another inference runtime, or supplies model headers. Native `thread.id` is the session identity used by the CLI, desktop and WeChat.

| State or action | Owner |
| --- | --- |
| Threads, turns, conversation history, tools and model access | Codex |
| Instructions, skills, approval and sandbox configuration | Codex |
| WeChat account and reply credentials | Channel adapter |
| Session selection, numbered list snapshots, immutable task recipient, delivery visibility | Bridge |
| Inbox deduplication, receive cursor, output queue, uncertain outcomes | Bridge SQLite |

## Native protocol

The baseline is Codex 0.162.1 and the [official App Server protocol](https://learn.chatgpt.com/docs/app-server). A WebSocket handshake runs over the local Unix socket; each text frame carries one JSON-RPC message. Initialization identifies this client as `codex_wechat`, with experimental APIs enabled for paginated native history.

Thread creation only supplies the configured working directory. Resume and task submission preserve native model, reasoning, instructions, permission and sandbox settings. Requests have bounded waits. Late replies are discarded and disconnection rejects pending requests; uncertain mutations are not automatically replayed. Closing the client never sends a daemon-shutdown operation.

`/effort` reads the selected thread's native model and reasoning setting, then obtains the supported reasoning-effort levels from `model/list`. It lists those advertised levels and marks the current value when known. `/effort <level>` accepts only an exact advertised level and sends `thread/settings/update` for that thread. Success requires a matching `thread/settings/updated` notification; the RPC acknowledgement alone is insufficient. The update affects subsequent turns and does not interrupt an active turn or submit a model request. Codex owns persistence of the setting; the bridge neither stores a separate effort value nor rewrites global configuration.

A loaded thread subscription can be shared by several native clients. Notifications from CLI/desktop turns on the current WeChat binding are adopted for delivery. Approvals and questions use their original RPC request ID. `serverRequest/resolved` expires a WeChat reply code when another client resolves the interaction. Unrelated requests are ignored; unsupported interactions remain available to a native client.

A single native question accepts plain text through `/answer <code> <content>`. Multiple questions use the native `answers` object keyed by question ID. MCP forms accept a JSON object of field values; the bridge sends it as `{action: "accept", content: <fields>}`. These are different native response schemas, so `content` is a command argument label rather than a shared JSON envelope.

## Delivery and state

The SQLite schema stores a version 1 JSON state snapshot, compatible with the earlier implementation. Camel-case JSON field names, bot-specific SHA-256 database names, interaction IDs, numeric switch snapshots and queued-message markers are retained. Ordered maps preserve native message ordering during reconciliation. Reconnection and thread selection reconcile saved runs against native state without resubmitting uncertain inputs. The instance lock uses an exclusive transaction in a separate SQLite file and is released by the operating system on exit.

An accepted inbox batch and its cursor commit together. Before a mutating RPC, the inbox entry is marked processing. A connection loss preserves an unknown outcome rather than submitting the task again. The outbox is durable and each run keeps its original recipient. Native messages and interaction prompts are deliverable only while that recipient has the run's thread selected. Successful `/switch` and `/new` operations atomically update the selection and discard pending output from other threads. Failed selection preserves the previous binding and its delivery. The sender checks the current selection before delivery and cancels an active send when its output becomes unselected. A message already accepted by WeChat cannot be recalled.

Each completed `agentMessage` for the selected thread is queued immediately, with its full authoritative body. All phases are retained; streamed deltas only buffer unfinished text. A consumed marker and any outbox entry commit in one transaction. Messages muted by selection are marked consumed so switching back or recovering does not replay them; native Codex retains their history. `turn/completed` records completion for every known run and queues a separate duration notice only for the selected thread. Native interaction requests follow the same visibility rule; changing selection does not resolve or reject them. Disconnect outcome notices also reach only the selected thread. Native duration takes precedence, followed by native timestamps and a reliable local start; uncertain historical durations display `?`.

WeChat messages preserve whitespace and split at Unicode scalar boundaries within a 3500 UTF-16-unit limit. Retry IDs match the prior client's SHA-256 namespace and chunk index. Retries use the most recent reply token. Lost network acknowledgements may still cause duplicate display.

The numbered `/list` response commits with its per-peer snapshot. `/switch` requires a number or native thread ID. A numeric selection uses the last stored mapping without fetching a new ordering. A successful new `/list` replaces it. Titles use single-line whitespace. Both list entries and `/help` commands are separated by blank lines for WeChat display.

## Source layout

| File | Responsibility |
| --- | --- |
| `src/main.rs` | Explicit CLI paths, QR login, credential persistence, signals |
| `src/codex.rs` | Shared Unix WebSocket, bidirectional RPC, connection lifecycle |
| `src/wechat.rs` | Trusted iLink endpoints, polling, sending, QR login |
| `src/bridge.rs` | Native session commands, events, interactions and recovery |
| `src/runtime.rs` | Independent polling, inbox, event and send loops |
| `src/store.rs`, `src/models.rs` | Compatible transactional state and instance lock |
| `deploy/` | Official user systemd unit and environment example |

## Validation

The offline suite exercises fake Unix WebSocket servers, synthetic native events, temporary SQLite files, mock iLink responses and localhost HTTP fixtures. It covers connection cancellation, typed errors, no-replay timeouts, shared request identity, Unicode, body deadlines, stable chunk IDs, busy turns, numeric selection persistence, reasoning-effort validation and native update confirmation, approvals, forms, native event ordering and full paginated history recovery.

`tests/fixtures/parity.json` was generated by executing the preceding TypeScript bridge against synthetic cases. Rust independently replays those cases and compares outputs, native RPC parameters and state checkpoints, with explicit adjustments for the `/list` command and selected-thread delivery semantics introduced in Rust. Dedicated tests cover switching during active turns, queued output removal, muted-message recovery and failed selection. Neither TypeScript nor Node is required to run this test suite.

A real Codex probe must create its own test thread, use the existing shared socket, and leave active user threads and the daemon lifecycle intact. Real WeChat QR login and delivery use the owner's account and are checked separately from the offline suite. Native protocol schema support does not imply that every provider will emit every message phase.
