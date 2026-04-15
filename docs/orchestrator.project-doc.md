# orchestrator 完整项目文档

> **文档状态**：Draft v1
> **版本**：v0.1.1
> **作者**：Codex
> **创建日期**：2026-04-15
> **最后更新**：2026-04-15
> **文档目的**：把 `orchestrator` 子项目从“Dagster 工程”这种容易越界的模糊概念收束为可立项、可拆分、可实现、可验收的正式项目，使其成为主项目中唯一负责日频 cycle 编排、Gate 策略装配、运行时资源注入与失败处理编排的执行控制层。

---

## 变更记录

| 版本 | 日期 | 变更内容 | 作者 |
|------|------|----------|------|
| v0.1 | 2026-04-15 | 初稿 | Codex |
| v0.1.1 | 2026-04-15 | 收紧对 `stream-layer` 的边界措辞，明确本项目不承载 P11 事件流 | Codex |

---

## 1. 一句话定义

`orchestrator` 是主项目中**唯一负责日频 Phase 0-3 cycle 的执行图装配、Dagster definitions、schedule/sensor、Gate 策略、AssetCheck 判定函数、运行时 resource 注入与失败处理编排**的控制层模块，它以“编排拥有顺序与策略但不拥有业务实现”和“日频 cycle 编排与 P11 事件流编排严格分离”为不可协商约束。

它不是主系统业务模块，不是数据平台模块，也不是 Kafka/Flink 事件层。  
它不负责特征计算、图谱传播、formal recommendation 生成、Raw/Canonical/Formal 数据写入。

---

## 2. 文档定位与核心问题

本文解决的问题不是“怎么搭一个 Dagster 项目”，而是：

1. **日频主线装配唯一入口问题**：Phase 0-3 的顺序、依赖、重跑、告警如果没有唯一装配层，会被散落到各业务模块里。
2. **Gate 行为一致性问题**：run failure、部分重跑、`inconclusive`、manifest 补写这些失败处理如果不冻结矩阵，P5 集成期会持续拉扯。
3. **编排与业务边界问题**：Dagster/Temporal 很容易演变成“另一个 main-core”，必须明确编排可以拥有什么、不能拥有什么。

---

## 3. 术语表

| 术语 | 定义 | 备注 |
|------|------|------|
| Daily Cycle | 一次完整日频运行周期，按 Phase 0-3 顺序执行 | 由 `orchestrator` 编排 |
| Phase 0 | 数据准备阶段 | 包含数据到位检查、candidate freeze、`llm_health_check`、Neo4j 一致性校验 |
| Phase 1 | 图谱更新阶段 | 写 graph delta、更新 Neo4j、生成 graph snapshot |
| Phase 2 | 主系统业务链阶段 | L1-L7 主链执行 |
| Phase 3 | 正式发布阶段 | formal object commit + `cycle_publish_manifest` |
| Gate Policy | Gate 判定与失败处理策略配置 | 类型来自 `contracts`，数值归 `orchestrator` |
| AssetCheck Decision | 对某次检查结果的统一分类结论 | 如 `continue` / `fail_run` / `partial_rerun` |
| Partial Rerun | 只重跑允许重跑的失败资产或 phase | 不等于全链重跑 |
| Resource Bundle | 一次运行注入的 Dagster/Temporal 资源集合 | 如 Tushare 限流、LiteLLM client |
| Temporal Gate Flow | P5+ 可选的 Phase 1-3 正式暂停/恢复编排路径 | 仍属于日频 cycle 编排 |

**规则**：
- Gate 的枚举、状态类型、错误分类必须优先服从 `contracts`
- Gate 阈值具体数值、schedule 频率、重跑开关属于 `orchestrator` policy
- P11 事件驱动局部 cycle 不属于 `orchestrator`

---

## 4. 目标与非目标

### 4.1 项目目标

1. **装配日频主线**：把 `data-platform`、`graph-engine`、`main-core`、`audit-eval` 等模块的 assets/checks/resources 组装成统一的 Phase 0-3 执行图。
2. **冻结 Gate 行为矩阵**：把常见失败类型映射到明确处理路径，消除“失败后怎么办”的口径分歧。
3. **提供 Lite 模式运行入口**：在 Dagster 上跑通最小日频 cycle，并支持 schedule、sensor、manual rerun。
4. **控制资源注入边界**：统一装配 Tushare 限流、LiteLLM client、dbt project、Neo4j client 等运行资源，但不自定义其业务协议。
5. **支持部分重跑**：让可恢复失败按矩阵选择性重跑，而不是一律整链重跑。
6. **保留 Temporal 可选扩展位**：P5+ 如果需要正式暂停/恢复语义，可以切到 Phase 1-3 的 Temporal Gate Flow，而不改变业务模块接口。
7. **为集成与影子运行提供可观测入口**：输出 runbook、告警路由和最小运行诊断能力。

### 4.2 非目标

- **不实现业务逻辑**：L1-L8 业务判断、图谱算法、实体解析都归各自模块，因为编排层只负责调用顺序和失败策略。
- **不拥有数据落地逻辑**：Raw/Canonical/Formal/Analytical 的写入归 `data-platform` 和对应业务模块，因为 `orchestrator` 只调度执行。
- **不承载 P11 事件流**：Kafka/Flink/CEP 事件驱动局部 cycle 归 `stream-layer`，否则日频编排与事件流会混在一起。
- **不定义共享 schema**：所有共享对象、枚举、错误码归 `contracts`，编排层只能消费不能改写。
- **不替代 assembly 的部署职责**：容器编排、启动脚本、统一 `.env` 归 `assembly`，`orchestrator` 只定义运行所需对象和装配方式。

---

## 5. 与现有工具的关系定位

### 5.1 架构位置

```text
contracts + policy config + module factories
  -> orchestrator
      ├── Dagster definitions
      ├── jobs / asset groups
      ├── schedules / sensors
      ├── AssetCheck wiring
      ├── resources injection
      └── optional Temporal Gate Flow (P5+)
  -> runtime execution
      ├── data-platform assets
      ├── graph-engine assets
      ├── main-core assets
      ├── audit-eval assets
      └── alerts / run history / reruns
```

### 5.2 上游输入

| 来源 | 提供内容 | 说明 |
|------|----------|------|
| `contracts` | Gate 类型、错误分类、配置 schema、阶段枚举 | `orchestrator` 不能自定义第二套类型 |
| `data-platform` | asset 工厂、resource 工厂、dbt project 入口、Phase 0 基础 assets | 只装配，不改其数据逻辑 |
| `graph-engine` | Phase 1 资产与纯检查函数 | 可调用纯函数，不在编排层实现传播逻辑 |
| `main-core` | Phase 2 / 3 资产、纯检查函数、发布入口 | `orchestrator` 只决定顺序和失败处理 |
| `audit-eval` | 审计、回放、retrospective 相关 assets | 接入执行图，不承载业务定义 |
| `reasoner-runtime` | LiteLLM / health check 资源或接口 | `orchestrator` 只注入，不改 provider 策略 |
| `assembly` | 环境配置、实例连接参数、部署方式 | 运行环境由 `assembly` 提供 |

### 5.3 下游输出

| 目标 | 输出内容 | 消费方式 |
|------|----------|----------|
| Dagster runtime | `Definitions`、jobs、schedules、sensors、resources | Python import |
| 开发 / 运维人员 | Gate policy、runbook、失败处理矩阵 | 配置文件 + 文档 |
| `assembly` | 运行入口、依赖列表、健康检查脚本 | Python CLI / 配置装配 |
| reviewer / 自动化代理 | 明确的编排边界与 rerun 规则 | 文档 + contract test |

### 5.4 核心边界

- **`orchestrator` 只拥有日频 cycle 编排，不拥有业务计算**
- **Gate policy 可以定义数值和处理路径，但类型必须来自 `contracts`**
- **AssetCheck 判定函数只允许调用纯函数，不允许夹带业务 IO**
- **Temporal（P5+ 可选）属于 `orchestrator`，Kafka/Flink 不属于**
- **下游业务模块不反向依赖 `orchestrator`**

---

## 6. 设计哲学

### 6.1 设计原则

#### 原则 1：Wiring-first

编排层的核心职责是把别人的能力按正确顺序连起来，而不是替代别人完成能力本身。  
一旦业务实现进入编排层，所有模块边界都会变脆，后续并行开发会迅速失控。

#### 原则 2：Policy is Allowed, Logic is Not

`orchestrator` 可以拥有 Gate 行为矩阵、schedule、阈值、重跑策略，因为这些是运行控制的一部分。  
但它不能在 asset 函数里写特征计算、图谱传播、选股判断，否则 policy 会变成业务实现的藏身处。

#### 原则 3：Lite-first Execution Reality

P1-P5 的默认执行路径必须先在 Dagster 上真实可跑。  
Temporal 只能是后续增强，不允许反向把当前实现建立在“以后会上 Temporal”的假设上。

#### 原则 4：Failure Semantics Must Be Explicit

每一类失败都必须有明确处理路径。  
不接受“到时候人工判断”的隐式规则，因为影子运行和自动化脚本都需要稳定、可机读的失败语义。

### 6.2 反模式清单

| 反模式 | 为什么危险 |
|--------|-----------|
| 在 Dagster asset 函数里直接写 L4-L7 业务逻辑 | 编排层膨胀成业务层，边界失真 |
| 把 Gate 阈值 hard-code 在 Python 里 | 配置不可审计、难以回溯、自动化代理无法对齐 |
| 把 P11 Kafka/Flink 事件链路塞进 `orchestrator` | 日频 cycle 与事件流混线，模块拆分失效 |
| 为编排单独定义第二套错误码 / 状态枚举 | 与 `contracts` 脱节，集成测试会漂移 |
| 让下游业务模块 import `orchestrator` 的内部实现 | 形成反向依赖，后续重构无法进行 |

---

## 7. 用户与消费方

### 7.1 直接消费方

| 消费方 | 消费内容 | 用途 |
|--------|----------|------|
| Dagster runtime | `Definitions`、jobs、resources | 实际执行日频 cycle |
| 开发者 / 自动化代理 | policy 配置、runbook、phase wiring 约定 | 开发、联调、排障 |
| `assembly` | 运行入口和依赖清单 | 本地 / 集成环境装配 |
| reviewer | 边界、失败矩阵、重跑语义 | 评审实现是否越界 |

### 7.2 间接用户

| 角色 | 关注点 |
|------|--------|
| 主编 / 架构 owner | Phase 0-3 是否严格按设计执行 |
| 运维 / 值班人 | 失败后该停、该补、该重跑什么 |
| 研究分析师 | formal 输出是否按 Gate 质量门发布 |

---

## 8. 总体系统结构

### 8.1 日频 cycle 主线

```text
schedule / manual trigger
  -> load Gate policy + env config
  -> assemble Phase 0 assets/checks/resources
  -> Phase 0 pass
  -> Phase 1 graph update
  -> Phase 2 main-core chain
  -> Phase 3 publish + manifest
  -> alert / run summary / audit hooks
```

### 8.2 Gate 失败处理主线

```text
asset result / dbt test / AssetCheck
  -> normalize failure
  -> classify by Gate matrix
  -> continue / fail_run / partial_rerun / mark_inconclusive / repair_manifest
  -> emit alert and runbook link
```

### 8.3 Temporal 可选扩展主线

```text
Dagster Phase 0
  -> optional handoff to Temporal
  -> Phase 1 / 2 / 3 pause-resume workflow
  -> publish acknowledgement
  -> sync result back to Dagster run summary
```

---

## 9. 领域对象设计

### 9.1 持久层对象

| 对象名 | 职责 | 归属 |
|--------|------|------|
| GatePolicyProfile | 版本化保存 Gate 行为矩阵、阈值、执行 backend 配置 | Git 跟踪的 YAML/TOML |
| PhaseRunRecord | 记录一次 phase 的运行状态、开始/结束时间、失败摘要 | Dagster instance storage |
| AssetCheckDecisionRecord | 记录每次检查的归类结果和处理动作 | Dagster event log / structured log |
| AlertDispatchRecord | 告警下发记录 | 日志 / Webhook 记录 |
| TemporalWorkflowBinding | 记录 cycle 与 Temporal workflow 的绑定关系 | Temporal 历史存储（可选） |

### 9.2 运行时对象

| 对象名 | 职责 | 生命周期 |
|--------|------|----------|
| OrchestrationContext | 一次 cycle 的统一上下文 | 从 run 启动到 run 结束 |
| PhaseExecutionPlan | 某个 phase 的执行计划 | 单个 phase 执行期间 |
| ResourceBundle | 本次运行注入的资源集合 | 单次 run 期间 |
| GateDecision | 某个检查结果的最终处理结论 | 单次检查过程 |
| PartialRerunPlan | 一次失败恢复的重跑选择集 | 单次恢复操作期间 |

### 9.3 核心对象详细设计

#### GatePolicyProfile

**角色**：日频 cycle 运行控制的正式配置对象。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| policy_version | String | policy 版本号 |
| contract_version | String | 对齐的 `contracts` 版本 |
| execution_backend | Enum | `dagster_only` / `dagster_plus_temporal` |
| phase_matrix | Array[Object] | 各 phase 的失败分类与处理路径 |
| thresholds | JSON | 如 L6 整池失败率阈值、单股票容忍阈值 |
| alert_channels | Array[String] | 告警目标 |
| updated_at | Timestamp | 更新时间 |

#### PhaseExecutionPlan

**角色**：某个 phase 在本轮 cycle 中应该执行什么、依赖什么、失败后如何处理。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| cycle_id | String | 当前 cycle |
| phase | Enum | `phase0` / `phase1` / `phase2` / `phase3` |
| asset_selection | Array[String] | 要执行的 asset key / group |
| required_resources | Array[String] | 本 phase 需要的 resources |
| gate_checks | Array[String] | 本 phase 要挂载的检查 |
| rerun_mode | Enum | `none` / `asset_only` / `phase_only` / `repair_only` |

#### GateDecision

**角色**：一次失败或检查结果的统一归类与动作。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| decision_id | String | 唯一标识 |
| cycle_id | String | 当前 cycle |
| phase | Enum | 所属 phase |
| check_name | String | 检查名或失败点 |
| failure_class | Enum | `infra` / `data_quality` / `task_level` / `publish` |
| action | Enum | `continue` / `fail_run` / `partial_rerun` / `mark_inconclusive` / `repair_manifest` |
| reason | String | 决策说明 |

#### ResourceBundle

**角色**：一次运行中被 `orchestrator` 注入的资源视图。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| resource_keys | Array[String] | 如 `tushare_client`、`litellm_client`、`duckdb_conn` |
| source_modules | Array[String] | 资源来源模块 |
| config_ref | String | 运行配置引用 |
| injected_at | Timestamp | 注入时间 |
| read_only | Boolean | 是否只读资源 |

#### PartialRerunPlan

**角色**：允许恢复的失败场景下，最小化重跑范围的选择结果。

**建议字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| run_id | String | 原始 run ID |
| failed_node | String | 失败 asset / check |
| rerun_selection | Array[String] | 需要重跑的最小选择集 |
| requires_manual_ack | Boolean | 是否需要人工确认 |
| generated_at | Timestamp | 生成时间 |

---

## 10. 数据模型设计

### 10.1 模型分层策略

- 版本化运行控制配置 → Git 跟踪的 YAML/TOML policy 文件
- 执行态 run / event / check 记录 → Dagster instance storage
- 可选的 Phase 1-3 暂停/恢复历史 → Temporal 自身持久层
- 告警与运行摘要 → 日志、Webhook 或 `assembly` 提供的外部接收器

### 10.2 存储方案

| 存储用途 | 技术选型 | 理由 |
|----------|----------|------|
| 编排配置 | YAML/TOML + Git | 可审计、可版本化、便于代理修改 |
| 运行记录 | Dagster instance storage（Lite 默认为 PostgreSQL） | 与执行引擎天然一致 |
| 事件历史 | Dagster event log | 便于回溯和选择性重跑 |
| 可选暂停/恢复历史 | Temporal persistence | 只在启用 Temporal 时需要 |
| 告警输出 | Python logging / Webhook | Lite 模式足够简单 |

### 10.3 关系模型

- `GateDecision.phase + cycle_id -> PhaseRunRecord.phase + cycle_id`
- `PartialRerunPlan.run_id -> PhaseRunRecord.run_id`
- `TemporalWorkflowBinding.cycle_id -> PhaseRunRecord.cycle_id`
- policy 中的 phase / action / failure_class 必须与 `contracts` 的枚举一致

---

## 11. 核心计算/算法设计

### 11.1 执行图编译算法

**输入**：各模块提供的 asset/resource/check 工厂，当前 policy 配置。

**输出**：可执行的 Dagster `Definitions`。

**处理流程**：

```text
读取 Gate policy
  -> 收集各模块导出的 factories
  -> 按 Phase 0/1/2/3 分组 assets
  -> 注入 resources 与 AssetCheck
  -> 校验依赖图和反向依赖约束
  -> 生成 Definitions / jobs / schedules / sensors
```

### 11.2 Gate 分类算法

**输入**：某次检查结果、失败事件、phase、policy matrix。

**输出**：`GateDecision`。

**处理流程**：

```text
标准化失败事件
  -> 判断 failure_class
  -> 在 phase matrix 中查找处理路径
  -> 生成 action
  -> 写 event log / alert
  -> 决定继续、停止、标 inconclusive 或部分重跑
```

### 11.3 部分重跑选择算法

**输入**：失败 asset、运行历史、policy 中的 rerun 规则。

**输出**：`PartialRerunPlan`。

**处理流程**：

```text
确认失败类型允许重跑
  -> 计算失败节点的最小依赖闭包
  -> 排除已确认不可重放的上游 phase
  -> 生成 rerun_selection
  -> 标记是否需要人工确认
```

### 11.4 Temporal 可选切换算法

**输入**：`execution_backend`、cycle 上下文、Phase 1-3 执行计划。

**输出**：Dagster-only 或 Dagster+Temporal 的运行路径。

**处理流程**：

```text
读取 execution_backend
  -> 若为 dagster_only，则 Dagster 执行 Phase 0-3
  -> 若为 dagster_plus_temporal，则 Dagster 完成 Phase 0
  -> 启动 Temporal workflow 执行 Phase 1-3
  -> 写回运行摘要和发布状态
```

---

## 12. 触发/驱动引擎设计

### 12.1 触发源类型

| 类型 | 来源 | 示例 |
|------|------|------|
| 定时触发 | schedule | 交易日收盘后启动日频 cycle |
| 手动触发 | human / automation | 人工 rerun、影子运行演练 |
| 状态感知触发 | sensor | 数据到位、上一 phase 成功、需要 manifest 补写 |
| 可选恢复触发 | Temporal signal | P5+ 正式暂停/恢复 |

### 12.2 关键触发流程

```text
schedule / manual trigger
  -> 构建 OrchestrationContext
  -> 编译 PhaseExecutionPlan
  -> 按 phase 执行
  -> 遇到 Gate 决策
  -> 继续 / fail / rerun / 补写
```

### 12.3 Gate 行为矩阵基线

| 阶段 | 失败类型 | 处理路径 | 是否允许部分重跑 |
|------|---------|---------|-----------------|
| Phase 0 | 数据延迟 / 上游数据未到位 | run 失败 + 告警，待补齐后重跑 Phase 0 | 否 |
| Phase 0 | `llm_health_check` 失败 | 硬停，不进入 Phase 1 | 否 |
| Phase 0 | dbt test fail | run 失败 + 告警 + 修复后重跑失败 asset group | 是 |
| Phase 1 | 图谱 promotion / snapshot 异常 | run 失败，保持上一轮 ready graph | 否 |
| Phase 2 | 单股票任务级 LLM 失败且未超阈值 | 标记 `inconclusive`，继续其他股票 | 该股票 |
| Phase 2 | 整池失败率超过阈值 | run 失败 + 告警 | 否 |
| Phase 3 | formal 单表 commit 失败 | Phase 3 失败，不写 manifest | 否 |
| Phase 3 | manifest 写入失败 | 告警 + 人工补写 manifest | 是（repair only） |
| 任意 | PG / Iceberg / Neo4j 等基础设施不可用 | 硬停 | 否 |

---

## 13. 输出产物设计

### 13.1 Dagster Definitions Bundle

**面向**：Dagster runtime、`assembly`

**结构**：

```text
{
  jobs: Array[String]
  asset_groups: Array[String]
  schedules: Array[String]
  sensors: Array[String]
  resources: Array[String]
}
```

### 13.2 Gate Policy Artifact

**面向**：开发者、reviewer、自动化代理

**结构**：

```text
{
  policy_version: String
  execution_backend: String
  phase_matrix: Array[Object]
  thresholds: Object
  alert_channels: Array[String]
}
```

### 13.3 Runbook / Alert Payload

**面向**：值班人、影子运行检查者

**结构**：

```text
{
  cycle_id: String
  phase: String
  status: String
  failed_node: String | null
  action: String
  summary: String
}
```

---

## 14. 系统模块拆分

**组织模式**：monorepo 下的独立 Python package，以 Dagster 为默认执行 backend。

| 模块名 | 语言 | 运行位置 | 职责 |
|--------|------|----------|------|
| `orchestrator.definitions` | Python | 库 | 汇总并导出 `Definitions` |
| `orchestrator.jobs` | Python | 库 | Phase 0-3 jobs / asset groups |
| `orchestrator.schedules` | Python | 库 | 交易日 schedule |
| `orchestrator.sensors` | Python | 库 | readiness / repair / rerun sensors |
| `orchestrator.resources` | Python | 库 | 资源装配与注入 |
| `orchestrator.policy` | Python + YAML/TOML | 库 + 配置 | Gate matrix、阈值、backend 选择 |
| `orchestrator.checks` | Python | 库 | AssetCheck wiring、纯判定函数封装 |
| `orchestrator.alerting` | Python | 库 | 告警与 runbook 输出 |
| `orchestrator.temporal` | Python | 可选库 | P5+ 的 Phase 1-3 Temporal workflow |

**关键设计决策**：

- `orchestrator` 在主项目中的角色是**日频 cycle 的唯一编排控制层**
- 它与其他子项目的关系是**只依赖上游模块导出的工厂/纯函数，不复制实现**
- 它必须独立成子项目，因为所有跨模块集成都在这里汇合
- `data-platform` 只提供 asset/resource 工厂，不在本项目内重写数据逻辑
- 本项目不承载 P11 事件流，Kafka/Flink/CEP 统一归 `stream-layer`

---

## 15. 存储与技术路线

| 用途 | 技术选型 | 理由 |
|------|----------|------|
| 日频编排 | Dagster | Lite 模式默认执行引擎，已与主文档对齐 |
| dbt 装配 | dagster-dbt | 把 dbt model 接入 Phase 0 资产图 |
| 运行配置 | YAML/TOML | 可审计、可版本管理 |
| 运行记录 | Dagster instance storage | 与 Dagster 原生一致 |
| 可选 Gate 暂停/恢复 | Temporal | 仅 P5+ 需要正式 pause/resume 时启用 |
| 告警输出 | logging / Webhook | Lite 模式最小可用 |

最低要求：

- Python 3.12+
- Dagster
- dagster-dbt
- PostgreSQL（Dagster instance storage 由 `assembly` 装配）
- 可选 Temporal（默认不启用）

---

## 16. API 与接口合同

### 16.1 Python 接口

| 名称 | 功能 | 参数 |
|------|------|------|
| `build_definitions()` | 组装完整 Dagster `Definitions` | 模块工厂集合、policy 路径 |
| `build_daily_cycle_jobs()` | 构建日频 cycle jobs / asset groups | phase 配置 |
| `load_gate_policy()` | 读取并校验 Gate policy | policy 文件路径 |
| `classify_gate_result()` | 把失败归类为统一动作 | phase、事件、policy |
| `plan_partial_rerun()` | 生成最小化重跑选择集 | run_id、failed_node |
| `build_resource_bundle()` | 组装运行时 resources | 环境配置、模块资源工厂 |

### 16.2 协议 / 配置接口

| 名称 | 功能 | 参数 |
|------|------|------|
| `GatePolicySchema` | Gate 配置 schema | 由 `contracts` 定义 |
| `AssetFactoryProvider` | 模块向 `orchestrator` 暴露 asset 工厂 | `get_assets / get_checks / get_resources` |
| `PureCheckProvider` | 纯判定函数提供协议 | 无副作用检查函数集合 |
| `ExecutionBackend` | 日频执行 backend 选择 | `dagster_only` / `dagster_plus_temporal` |

### 16.3 版本与兼容策略

- 所有 Gate 类型、错误分类、阶段枚举以 `contracts` 为准
- `orchestrator` 只消费各模块暴露的稳定工厂接口，不直接读取其内部实现
- 切换到 Temporal 时，不允许改写 `data-platform`、`main-core`、`graph-engine` 的对外接口

---

## 18. 测试与验证策略

### 18.1 单元测试

- Gate policy 文件加载与 schema 校验
- `classify_gate_result()` 的失败归类逻辑
- `plan_partial_rerun()` 的选择算法
- resource bundle 注入与只读边界
- `AssetFactoryProvider` 接口校验

### 18.2 集成测试

| 场景 | 验证目标 |
|------|----------|
| 1 API + 1 dbt model + 1 个 Phase 0 check | 验证 P1a 最小 Dagster 骨架 |
| Phase 0-3 四阶段串联跑通 | 验证日频主线装配完整性 |
| `llm_health_check` 失败 | 验证 Phase 0 硬停语义 |
| Phase 2 单股票失败未超阈值 | 验证 `inconclusive` 路径 |
| Phase 3 manifest 写入失败 | 验证 repair-only 路径 |

### 18.3 协议 / 契约测试

- Gate policy 的 phase / action / failure_class 与 `contracts` 枚举一致
- `AssetFactoryProvider` / `PureCheckProvider` 实现符合约定
- `orchestrator` 不引入 Kafka/Flink 事件流接口

### 18.4 边界与回归测试

- 编排层不包含业务计算函数的静态检查
- 下游业务模块不反向 import `orchestrator` 的依赖检查
- Dagster-only 与 Dagster+Temporal 两条路径的 phase 语义一致性测试（可选）

---

## 19. 关键评价指标

### 19.1 性能指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 最小日频 cycle 装配耗时 | `< 30 秒` | 本地 Lite 环境 |
| 失败后生成 rerun plan 耗时 | `< 5 秒` | 单次恢复决策 |
| Phase 0 Gate 判定额外开销 | `< 3 秒` | 不含上游业务执行时间 |

### 19.2 质量指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| Gate 矩阵覆盖率 | `100%` | 文档列出的失败类型必须有明确动作 |
| 编排层业务逻辑越界率 | `0` | 不允许在 `orchestrator` 中实现业务算法 |
| 反向依赖发生率 | `0` | 下游模块不得依赖 `orchestrator` 内部 |
| Phase 3 半发布可见错误率 | `0` | manifest 语义必须守住 |

---

## 20. 项目交付物清单

### 20.1 编排骨架

- Dagster `Definitions`
- Phase 0-3 jobs / asset groups
- schedules / sensors / manual rerun 入口

### 20.2 策略与配置

- Gate 行为矩阵
- policy schema 校验器
- backend 选择配置（Dagster-only / Temporal 可选）

### 20.3 运行与排障

- resource bundle 组装层
- 告警输出与 runbook
- 集成测试与边界检查脚本

---

## 21. 实施路线图

### 阶段 0：P1a Dagster 骨架（1-2 周）

**阶段目标**：建立最小 Dagster 工程与 1 条 Phase 0 骨架。

**交付**：
- `Definitions`
- 1 个 schedule
- 1 个 dbt asset group
- 1 个最小 Gate check

**退出条件**：Dagster UI 中可看到最小依赖图并成功执行。

### 阶段 1：P1b-P1c Phase 0 完整化（2-3 周）

**阶段目标**：把数据准备、candidate freeze、基础检查完整接入。

**交付**：
- Phase 0 assets/checks
- Gate policy 初版
- 手动 rerun 与告警通路

**退出条件**：Phase 0 失败可以按矩阵停止或重跑。

### 阶段 2：P2-P3 主链接入（2-4 周）

**阶段目标**：接入 Phase 1 图谱链与 Phase 2/3 主系统链。

**交付**：
- Phase 1 / Phase 2 / Phase 3 wiring
- `llm_health_check`
- Phase 3 manifest repair-only 路径

**退出条件**：四阶段串联最小闭环跑通。

### 阶段 3：P5 集成与影子运行（2-3 周）

**阶段目标**：冻结 Gate 行为矩阵并支撑影子运行。

**交付**：
- 完整 Gate matrix
- runbook
- 失败注入测试

**退出条件**：P5 影子运行期间不再因失败处理口径反复改边界。

### 阶段 4：P5+ Temporal 可选扩展（按需）

**阶段目标**：在不改业务模块接口的前提下引入 Phase 1-3 pause/resume。

**交付**：
- `orchestrator.temporal`
- backend 切换配置
- parity test

**退出条件**：Dagster-only 与 Dagster+Temporal 行为一致。

---

## 22. 主要风险

| 风险 | 影响 | 应对措施 |
|------|------|----------|
| `orchestrator` 膨胀成业务容器 | 模块边界失效、后续返工极大 | 静态检查禁止业务逻辑进入编排层 |
| Gate 行为矩阵缺失或含糊 | P5 集成持续争议 | 在本模块中先冻结 matrix，再允许联调 |
| Dagster 与 Temporal 双路径漂移 | 运行语义不一致 | Temporal 只做 Phase 1-3 包装，不改模块接口 |
| 编排配置分散在多个仓位 | 审计与回放困难 | policy 配置统一纳入本模块版本控制 |

---

## 23. 验收标准

项目完成的最低标准：

1. `orchestrator` 能在不复制业务实现的前提下装配 `data-platform`、`graph-engine`、`main-core`、`audit-eval` 的最小日频执行图
2. Phase 0-3 的顺序、Gate 行为矩阵、失败处理路径都有正式文档与可运行实现
3. `llm_health_check`、dbt test fail、Phase 2 `inconclusive`、Phase 3 manifest 补写四类关键场景都可验证
4. 编排层不包含 Kafka/Flink 事件驱动职责，也不包含业务算法实现
5. 文档中定义的主项目角色、OWN/BAN/EDGE 与主项目 `12 + N` 模块边界一致

---

## 24. 一句话结论

`orchestrator` 子项目不是“放 Dagster 代码的地方”，而是主项目中唯一负责日频 cycle 执行图、Gate 策略和失败处理编排的正式控制层。  
它的边界是否足够硬，直接决定后续集成会是模块协作，还是重新长出一个隐形主系统。

---

## 25. 自动化开发对接

### 25.1 自动化输入契约

| 项 | 规则 |
|----|------|
| `module_id` | `orchestrator` |
| 脚本先读章节 | `§1` `§4` `§5.4` `§8` `§12` `§14` `§16` `§18` `§21` `§23` |
| 默认 issue 粒度 | 一次只实现一个 phase 纯函数、一个 resource bundle、一个 asset / check 组，或一条调度路径 |
| 默认写入范围 | 当前 repo 的 Dagster definitions、resource / policy wiring、检查逻辑、测试、文档和配置 |
| 内部命名基线 | 以 `§8` 的主线名、`§12` 的触发名和 `§14` 的内部模块名为准 |
| 禁止越界 | 不写业务实现、不承载 P11 事件流、不替代 `assembly` 做部署装配 |
| 完成判定 | 同时满足 `§18`、`§21` 当前阶段退出条件和 `§23` 对应条目 |

### 25.2 推荐自动化任务顺序

1. 先落 definitions、resource skeleton 和 phase 基础记录对象
2. 再落 Gate policy、AssetCheck、failure classification 和 schedule / sensor
3. 再落最小 cycle 主线和回归测试
4. `Temporal` 仅在前述主线稳定后，按 `§21` 后置阶段单独推进

补充规则：

- 单个 issue 默认只覆盖一个 phase 或一种编排能力，不跨多个 phase 混做
- 任何会影响模块公共接口的改动，必须先有契约测试再实现

### 25.3 Blocker 升级条件

- 需要 import 其他子项目私有内部而不是公开入口
- 需要把 Kafka / Flink / CEP 放进本项目
- 需要把业务判断、存储写入或部署职责拉进编排层
- 无法给出可重复运行的最小 cycle 或 Gate 验证路径
