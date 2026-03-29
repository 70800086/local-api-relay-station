# Watchdog systemd timer（第二版架构）

目标：用 systemd timer 定时触发巡检，但真正的提醒/处理仍统一走 OpenClaw。

## 组件

- `task_activity.py`：多信号活跃/空转判定（任务板主导 + relay openclaw 辅助）
- `task_watchdog.py`：读快照、跑判定、生成 `TickDecision`
- `task_watchdog_runner.py`：systemd 定时触发入口；在 `TickDecision.action != NO_REPLY` 时调用 `openclaw system event`
- `deploy/systemd/openclaw-watchdog.service`
- `deploy/systemd/openclaw-watchdog.timer`

## 触发链路

1. systemd timer 每 5 分钟触发一次 service
2. service 执行 `task_watchdog_runner.py`
3. runner 读取 `state/tasks.json` / `TASKS.md` 与本地 relay stats
4. runner 复用 `task_watchdog.py` 生成 `TickDecision`
5. 若 `decision.action == NO_REPLY`：静默退出
6. 若需要通知：runner 调 `openclaw system event --mode now --text ...`，把事件交回 OpenClaw 统一处理

## 最小部署

将仓库内模板复制到用户 systemd 目录：

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/openclaw-watchdog.service ~/.config/systemd/user/
cp deploy/systemd/openclaw-watchdog.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now openclaw-watchdog.timer
systemctl --user status openclaw-watchdog.timer
```

## 干跑验证

```bash
python3 task_watchdog_runner.py --dry-run --json
```

验证 `RESUME_TASK` 语义但先不真实拉起 Codex 时，可继续使用默认 service 参数；
若要本地验证 Codex 恢复链路，可显式执行：

```bash
python3 task_watchdog_runner.py --json --resume-codex --codex-workdir /root/.openclaw/workspace
```

## 默认上线建议

当前推荐的 systemd service 默认参数：

- 保持 `task_watchdog_runner.py --json`
- **先不要默认加** `--resume-codex`

原因：

- 现版本已能输出结构化 `RESUME_TASK` / `NOTIFY_ONLY` 事件
- 自动拉起 Codex 已具备能力，但属于更激进的执行策略
- 建议先以“结构化恢复事件 + 人工观察”模式上线，再按需要打开 `--resume-codex`

若后续要开启自动恢复 Codex，建议在 service 中追加：

```bash
--resume-codex --codex-workdir /root/.openclaw/workspace --codex-cooldown-minutes 15
```

其中：

- `--codex-workdir`：指定 Codex 运行目录
- `--codex-cooldown-minutes`：防止 timer 周期内对同一任务重复拉起
- 冷却状态默认写入 `state/watchdog_codex_resume.json`

## 说明

- runner 默认从 `/root/.openclaw/openclaw.json` 读取 gateway token/url
- runner 默认使用 `openclaw system event --mode now` 作为桥接入口
- 当前版本在 `TickDecision.action in {RESUME_TASK, NOTIFY_ONLY}` 时派发系统事件，避免定时器变成新的噪音源
- 当显式开启 `--resume-codex` 时，runner 可直接执行本地 `codex exec --full-auto ...`，并带同任务冷却保护
