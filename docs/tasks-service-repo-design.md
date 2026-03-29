# tasks-service 分仓设计稿

## 仓库定位

`tasks-service` 是一个面向 agent 的任务状态与调度内核，负责统一管理：

- 任务模型
- 任务状态
- 状态流转
- stale/active 判定
- 恢复决策
- 存储与展示同步

它**不负责**：

- OpenClaw 消息派发
- relay 专属实现
- codex/claude 具体执行命令
- systemd/timer 部署
- 某个业务仓库自己的任务内容

---

## 仓库命名建议

优先推荐：

- `tasks-service`

备选：

- `openclaw-tasks`
- `agent-task-core`
- `task-runtime-core`

建议直接使用：`tasks-service`

原因：

- 中性
- 不绑 OpenClaw
- 以后别的 agent/runtime 也能接

---

## 设计目标

### 目标

把现在分散在：

- `TASKS.md`
- `state/tasks.json`
- `task_watchdog.py`
- `task_activity.py`
- `task_watchdog_runner.py`

里的任务逻辑，收口成一个独立可复用的任务内核。

### 非目标

第一版**不追求**：

- 多租户
- 复杂权限
- 分布式调度
- UI 平台
- 通用工作流引擎
- 大而全插件系统

---

## 核心边界

### 新仓负责

#### 1. 任务领域模型

统一定义：

- Task
- TaskSnapshot
- TickDecision
- ActivityAssessment
- TaskEvent

#### 2. 任务状态服务

统一入口：

- load / save snapshot
- active task 选择
- task 状态流转
- heartbeat/progress/blocker 更新
- resume/switch/complete

#### 3. 任务判定策略

统一规则：

- stale 判定
- active 判定
- relay/外部 activity 作为辅助信号
- next_action 是否过大
- 该 `NO_REPLY` / `RESUME_TASK` / `NOTIFY_ONLY`

#### 4. 存储抽象

支持：

- JSON 真状态
- Markdown 展示/同步

#### 5. CLI

最小命令行：

- inspect
- tick
- render
- sync
- task update

### 当前业务仓继续负责

#### 1. 环境接入

- OpenClaw gateway event
- relay stats 拉取
- codex exec / claude code / acp runtime 执行
- systemd service/timer

#### 2. 业务任务语义

- stock-score 具体任务内容
- 业务仓自己的 TASKS 使用方式
- 任务模板/预设

---

## 推荐架构

### 分层结构

```text
外部环境层
  ├─ OpenClaw / Relay / Codex / Systemd
  ↓
适配层
  ├─ relay activity adapter
  ├─ watchdog adapter
  ├─ executor adapter
  ↓
任务服务层
  ├─ TasksService
  ↓
策略层
  ├─ TaskPolicy
  ↓
存储层
  ├─ JsonTaskStore
  ├─ MarkdownTaskView
  └─ EventLogStore(可后续)
```

---

## 目录结构建议

```text
tasks-service/
├─ README.md
├─ pyproject.toml
├─ src/
│  └─ tasks_service/
│     ├─ __init__.py
│     ├─ models.py
│     ├─ enums.py
│     ├─ service.py
│     ├─ policy.py
│     ├─ signals.py
│     ├─ sync.py
│     ├─ errors.py
│     ├─ cli.py
│     ├─ store/
│     │  ├─ __init__.py
│     │  ├─ base.py
│     │  ├─ json_store.py
│     │  ├─ markdown_view.py
│     │  └─ event_log.py
│     └─ adapters/
│        ├─ __init__.py
│        └─ relay_activity.py
├─ tests/
│  ├─ test_policy.py
│  ├─ test_service.py
│  ├─ test_json_store.py
│  ├─ test_markdown_view.py
│  ├─ test_sync.py
│  └─ fixtures/
└─ docs/
   ├─ architecture.md
   ├─ data-model.md
   └─ migration.md
```

---

## 核心数据模型

### 1) Task

```python
@dataclass
class Task:
    task_id: str
    title: str
    status: str
    progress: int
    priority: str = ""
    task_type: str = ""
    goal: str = ""
    blocker: str = ""
    next_action: str = ""
    done_criteria: str = ""
    depends_on: list[str] = field(default_factory=list)
    last_progress_at: datetime | None = None
    heartbeat_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

### 2) TaskSnapshot

```python
@dataclass
class TaskSnapshot:
    updated_at: datetime
    active_task_id: str
    tasks: list[Task]
    source: str
    version: str = "v1"
```

### 3) ActivitySignal

外部活跃信号不要直接绑 relay，做成通用信号模型：

```python
@dataclass
class ActivitySignal:
    source: str
    last_seen_at: datetime | None
    kind: str
    metadata: dict[str, Any] = field(default_factory=dict)
```

例子：

- relay_recent_request
- executor_running
- editor_save
- git_diff

### 4) ActivityAssessment

```python
@dataclass
class ActivityAssessment:
    is_active: bool
    is_stale: bool
    reason: str
    task_board_active: bool
    external_active: bool
```

### 5) TickDecision

```python
@dataclass
class TickDecision:
    action: str
    reason: str
    changed: bool
    notify_text: str
    snapshot: TaskSnapshot
```

---

## 核心模块职责

### `models.py`

只放数据结构，不放业务逻辑。

### `policy.py`

只负责判定规则，例如：

- `assess_task_activity()`
- `next_action_too_large()`
- `should_resume_task()`
- `should_block_task()`

### `service.py`

只负责状态流转，例如：

- `load_snapshot()`
- `find_active_task()`
- `decide_tick()`
- `resume_task()`
- `block_task()`
- `complete_task()`
- `switch_to_next_task()`

### `store/json_store.py`

负责真实持久化：

- load
- save
- versioning
- schema validation

### `store/markdown_view.py`

负责 Markdown 渲染/解析：

- render snapshot -> TASKS.md
- parse TASKS.md -> snapshot
- 尽量把它定义成**视图层**，而不是唯一真相

### `sync.py`

负责：

- json 与 md 冲突处理
- authoritative source 选择
- resync 策略

---

## 第一版 API 草案

### Service API

```python
class TasksService:
    def __init__(self, store, policy):
        ...

    def load_snapshot(self) -> TaskSnapshot:
        ...

    def save_snapshot(self, snapshot: TaskSnapshot) -> None:
        ...

    def find_active_task(self, snapshot: TaskSnapshot) -> Task | None:
        ...

    def decide_tick(
        self,
        now: datetime,
        *,
        signals: list[ActivitySignal] | None = None,
    ) -> TickDecision:
        ...

    def resume_active_task(self, snapshot: TaskSnapshot, now: datetime) -> TaskSnapshot:
        ...

    def switch_to_next_task(self, snapshot: TaskSnapshot, now: datetime) -> TaskSnapshot:
        ...

    def block_task(self, snapshot: TaskSnapshot, task_id: str, blocker: str, now: datetime) -> TaskSnapshot:
        ...

    def complete_task(self, snapshot: TaskSnapshot, task_id: str, now: datetime) -> TaskSnapshot:
        ...

    def heartbeat_task(self, snapshot: TaskSnapshot, task_id: str, now: datetime) -> TaskSnapshot:
        ...

    def update_progress(self, snapshot: TaskSnapshot, task_id: str, progress: int, note: str, now: datetime) -> TaskSnapshot:
        ...
```

### Policy API

```python
class TaskPolicy:
    def assess_task_activity(
        self,
        task: Task,
        now: datetime,
        *,
        signals: list[ActivitySignal] | None = None,
    ) -> ActivityAssessment:
        ...

    def next_action_too_large(self, next_action: str) -> bool:
        ...

    def shrink_next_action(self, next_action: str) -> str:
        ...
```

---

## 状态真相设计

这是关键。

### 推荐方案

#### 真相源

- **JSON snapshot 是机器真相**
- **Markdown 是人类视图**

#### 为什么

因为 Markdown 同时承担：

- 可读
- 可写
- 可解释

但很难长期承担：

- 强一致结构化状态源

所以第一版即使保留 Markdown 解析，也建议逐步转成：

- `tasks.json` authoritative
- `TASKS.md` rendered view
- 允许人工修改 Markdown，但通过同步流程进入 JSON

---

## 信号模型设计

这次踩坑的核心，其实就是：

- 任务板 heartbeat
- relay activity
- runner 自己刷新 heartbeat

全都耦在一起了。

所以新仓建议统一成**信号系统**：

### 信号来源

- task_board
- relay
- executor
- user_update
- git_activity

### 原则

- 外部信号只能作为辅助依据
- 不能直接偷偷改任务状态
- 状态更新只能通过 service 的显式流转发生

这条特别重要。
否则以后又会出现“某个 signal 悄悄把 task 保活”的问题。

---

## CLI 设计

第一版就做最少：

### 查看

```bash
tasks-service inspect
tasks-service inspect --json
tasks-service task show 2b
```

### 决策

```bash
tasks-service tick
tasks-service tick --json
```

### 状态更新

```bash
tasks-service task heartbeat 2b
tasks-service task progress 2b --progress 20 --note "完成字段盘点"
tasks-service task block 2b --reason "等待外部接口"
tasks-service task done 2b
```

### 同步

```bash
tasks-service sync
tasks-service render
```

---

## 测试策略

### 单元测试

#### policy

- stale_progress
- missing_progress
- next_action_too_large
- external signal only
- stale task board + external signal

#### service

- active task 恢复
- 切换下一个 TODO
- BLOCKED/DONE 状态流转
- heartbeat 更新
- progress 更新

#### store

- json roundtrip
- markdown parse/render
- 冲突处理

### 集成测试

- snapshot -> tick -> state update
- stale task -> RESUME_TASK
- blocker -> NOTIFY_ONLY
- no active task -> switch_task

---

## 和当前仓的集成方式

当前仓以后可以这样接：

### 现在的 `task_watchdog_runner.py`

变成：

- 采集外部 signal（relay、executor）
- 调 `tasks_service.TasksService.decide_tick()`
- 根据 `TickDecision` 决定：
  - 发 system event
  - 拉 codex
  - 记录执行结果

这样当前仓就只做“环境编排”。

---

## 迁移计划

### Phase 0：新仓初始化

- 新建仓库 `tasks-service`
- 建 pyproject
- 建基础目录
- 建 models/policy/service/store skeleton

### Phase 1：搬纯逻辑

从当前仓迁：

- `TaskRecord` → `Task`
- `TaskStateSnapshot` → `TaskSnapshot`
- `TickDecision`
- `find_active_task`
- `select_next_runnable_task`
- `assess_task_activity`
- `next_action_too_large`
- `shrink_next_action`
- `decide_watchdog_tick` 改名成 `decide_tick`

### Phase 2：搬 store

迁：

- JSON 读写
- Markdown parse/render
- 冲突解决逻辑

### Phase 3：当前仓接回新仓

当前仓改成依赖：

- `tasks-service`

然后保留：

- relay adapter
- systemd runner
- codex executor

### Phase 4：削薄当前仓旧文件

- `task_watchdog.py` 只保留适配
- `task_activity.py` 并入新仓 policy/signals

---

## 第一版必须控制住的范围

为了避免分仓后失控，第一版一定要坚持：

### 只做

- 单仓单用户
- 单 active task
- JSON 真相
- Markdown 视图
- 简单 signals
- watchdog tick 决策

### 不做

- 多项目共享数据库
- Web UI
- 复杂 workflow DSL
- 插件市场
- 多执行器编排
- 分布式任务分配

---

## 第一版里程碑

### M1：任务内核可运行

- models/service/policy/store 完成
- 单测通过

### M2：当前仓接入成功

- watchdog 改用新仓 API
- 旧逻辑可删或仅保留兼容层

### M3：Markdown 降为视图层

- JSON authoritative
- Markdown render/sync 清晰

### M4：执行结果记录

- 为后续 watchdog 自动执行历史留接口

---

## README 建议首句

> tasks-service is a lightweight task-state and watchdog decision core for agentic workflows. It manages task snapshots, state transitions, activity/staleness policy, and resume decisions, while leaving execution and environment integration to host applications.

---

## 最终建议

如果采用分仓方案，建议第一版按“任务内核仓”推进，不要把 watchdog runner、relay、systemd 一起搬过去。

最稳的边界是：

- **新仓**：任务内核
- **当前仓**：执行环境与集成

这样以后不管接 OpenClaw、Codex、Claude Code 还是别的 agent runtime，都不用再重新拆一次。
