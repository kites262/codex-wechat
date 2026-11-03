# codex-wechat

**在微信里使用你原来的 Codex 会话。**

Rust 编写的微信 ClawBot 消息桥，连接本机已有的 Codex 共享 App Server。模型、工具、技能、项目规则、权限和会话历史由 Codex 原生运行时提供；bridge 负责微信收发、会话绑定与可靠投递。

[English](README.md) · [User systemd 部署教程](docs/deployment.md) · [架构与验证](docs/architecture.md)

## 能做什么

- 微信创建的会话可以在 Codex CLI 恢复，CLI 或桌面发起的同会话任务也能回传微信。
- `/list` 按 1–10 序号显示最近会话标题；`/switch 3` 选择上次列表的第 3 项。列表映射持久保存，重启和排序变化不会改变目标。
- `/effort` 显示当前会话模型支持的推理档位；`/effort high` 修改原生会话设置，对后续任务生效。
- 当前选中会话的每条助手消息完成后发送，保留 commentary、partial_answer、final_answer，最后单独发送 `[done in 30.1s]`。切换后原会话停止向微信投递，原生任务继续执行。
- 审批、权限申请、用户提问和 MCP 表单通过原生请求编号交互，其他 Codex 客户端完成后编号失效。
- SQLite 保存接收游标、去重、待发消息及运行任务的固定收件人。仅当前选中会话向微信投递；发送失败重试，结果未知的任务不会自动重放。
- 编译后运行单个程序，无需 Node.js、npm 或独立数据库服务。

## 部署

官方部署方式是 **Linux 用户级 systemd 服务**，与本机 Codex 使用同一个用户。完整步骤见 [部署教程](docs/deployment.md)。

需要可用的原生 Codex 及正在运行的共享 App Server，以及支持 ClawBot 的微信账号。已验证的 Codex 协议基线为 0.162.1。

从项目源码构建并安装：

```sh
cargo build --locked --release
install -Dm755 target/release/codex-wechat ~/.local/bin/codex-wechat
```

每次调用都必须显式传入 `--state-dir`。示例统一使用 `~/.config/codex-wechat`：

```sh
install -d -m 700 ~/.config/codex-wechat
~/.local/bin/codex-wechat --state-dir ~/.config/codex-wechat login
```

扫码确认后安装配置和用户服务：

```sh
install -Dm600 deploy/service.env.example ~/.config/codex-wechat/service.env
install -Dm644 deploy/codex-wechat.service ~/.config/systemd/user/codex-wechat.service
```

编辑 `~/.config/codex-wechat/service.env`，将 `CODEX_WECHAT_CWD` 设置为项目绝对路径，然后启动：

```sh
systemctl --user daemon-reload
systemctl --user enable --now codex-wechat.service
journalctl --user -u codex-wechat.service -f
```

退出登录后持续运行、升级、日志和凭据失效处理见 [部署教程](docs/deployment.md)。停止 bridge 只断开渠道连接，Codex 服务及其中的任务继续运行。

## 微信命令

| 命令 | 行为 |
| --- | --- |
| `/new` | 新建并选择原生会话，显示会话 ID |
| `/list` | 显示当前项目最近 10 个会话的编号和标题 |
| `/switch 3` | 切换到上次列表的第 3 个会话 |
| `/switch <threadId>` | 按原生 ID 切换 |
| `/status` | 查看绑定、运行状态和待交互编号 |
| `/effort [level]` | 列出模型支持的推理档位，或修改后续任务的档位 |
| `/stop [threadId]` | 中断当前或指定会话的任务 |
| `/approve <code>` | 批准原生交互 |
| `/reject <code>` | 拒绝原生交互 |
| `/answer <code> <content>` | 单个问题直接回答文字；多个问题或表单按提示填写结构化内容 |
| `/help` | 查看命令 |

选择会话后直接发送文字即可。`/switch` 必须提供编号或原生会话 ID；编号对应最近一次成功的 `/list`，重新执行 `/list` 会更新编号映射。Bridge 提示使用英文；模型正文、会话标题与原生问题保留原始语言。普通消息没有会话或任务前缀。

选中会话后，`/effort` 会列出其当前模型支持的档位，并在已知的当前值后标记 `*`。使用列表中的档位，例如 `/effort high`，即可修改。Bridge 根据原生模型列表校验参数，收到 Codex 的会话设置更新通知后才确认成功。新设置对后续任务生效，正在执行的任务沿用原设置。档位由 Codex 保存，bridge 不另存一份配置。

单个问题直接回复文字，例如 `/answer a1 使用现有数据库`。多个问题会给出按问题 ID 填写的 JSON 示例：

```text
/answer a1 {"answers":{"database":{"answers":["SQLite"]},"region":{"answers":["Tokyo"]}}}
```

MCP 表单则提交其要求的字段对象，例如 `/answer a2 {"name":"Alice"}`。Bridge 会封装为原生表单响应，无需额外添加外层 `content` 字段。

成功执行 `/switch` 或 `/new` 后，只有新选中的会话向微信投递正文、任务结束提示和交互提示；其他会话尚未发送的消息会被清除，原生任务继续执行。切回会话后接收后续消息，离开期间静默的消息不补发，可在 Codex 查看历史。切换失败时保留原会话绑定。

已有状态目录中的 account.json 和版本 1 SQLite 数据库可以继续使用。Bridge 保存渠道状态，原生会话历史仍由 Codex 保存。断线期间无法确认的请求不会自动重放，可用 `/status` 或原生 Codex 客户端查看；重新连接或通过 `/switch` 再次选择会话时会自动核对原生任务状态。核对后仍只向当前选中会话投递，已静默的消息不会重放。

当前面向单用户私聊、单个项目目录与文本消息。图片、语音和文件会提示改发文字；不支持的原生交互可在 Codex 客户端处理。本项目是独立集成。

## 开发

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --locked --release
```

源码构建需要 Rust 1.88+、Cargo 和 C 编译器。测试覆盖共享协议、交互路由、事务、恢复、重试、Unicode、超时、关闭流程和旧版状态兼容。

## 许可证

[MIT](LICENSE)。
