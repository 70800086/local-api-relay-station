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

## 说明

- runner 默认从 `/root/.openclaw/openclaw.json` 读取 gateway token/url
- runner 默认使用 `openclaw system event --mode now` 作为桥接入口
- 第一版只在 `TickDecision.action != NO_REPLY` 时派发系统事件，避免定时器变成新的噪音源
