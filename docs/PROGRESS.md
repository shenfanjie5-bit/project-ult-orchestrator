# orchestrator 项目进度

> 源文档：`docs/orchestrator.project-doc.md` §21 实施路线图
> 任务拆解：`docs/TASK_BREAKDOWN.md`
> 最后更新：2026-04-16

---

## 里程碑状态总览

| 里程碑 | 标题 | 对应文档阶段 | Issue 数 | 预计工期 | 状态 | 退出条件（摘要） |
|--------|------|--------------|----------|----------|------|------------------|
| milestone-0 | P1a Dagster 骨架 | §21 阶段 0 | 10 | 1-2 周 | 🟡 进行中 | Dagster UI 可看到最小依赖图并成功执行 |
| milestone-1 | P1b-P1c Phase 0 完整化 | §21 阶段 1 | 8 | 2-3 周 | ⚪ 待启动 | Phase 0 失败可按矩阵停止或重跑 |
| milestone-2 | P2-P3 主链接入 | §21 阶段 2 | 9 | 2-4 周 | ⚪ 待启动 | 四阶段串联最小闭环跑通 |
| milestone-3 | P5 集成与影子运行 | §21 阶段 3 | 7 | 2-3 周 | ⚪ 待启动 | 影子运行期间失败处理口径不再反复 |
| milestone-4 | P5+ Temporal 可选扩展 | §21 阶段 4 | 3 | 按需 | ⚪ 待启动 | Dagster-only 与 Dagster+Temporal 行为一致 |

**图例**：⚪ 待启动 · 🟡 进行中 · 🟢 已完成 · 🔴 阻塞

---

## milestone-0 详细进度（阶段 0：P1a Dagster 骨架）

| ISSUE | 标题 | 优先级 | 状态 | 关键依赖 |
|-------|------|--------|------|----------|
| ISSUE-001 | 初始化 orchestrator Python 包结构与依赖基线 | P0 | ⚪ | 无 |
| ISSUE-002 | 定义 contracts 消费适配层（禁止自定义类型） | P0 | ⚪ | #001 |
| ISSUE-003 | Gate Policy schema 与 YAML 加载器 | P0 | ⚪ | #002 |
| ISSUE-004 | ResourceBundle 装配骨架与只读注入边界 | P0 | ⚪ | #001 |
| ISSUE-005 | 最小 AssetCheck 与 classify_gate_result 纯函数 | P0 | ⚪ | #003 |
| ISSUE-006 | Phase 0 最小 asset group 与 dbt 装配骨架 | P0 | ⚪ | #001 |
| ISSUE-007 | 交易日 schedule 与最小 sensor 骨架 | P0 | ⚪ | #008 |
| ISSUE-008 | build_definitions() 入口与 daily_cycle_job 装配 | P0 | ⚪ | #004 #005 #006 #007 |
| ISSUE-009 | alerting 骨架（logging 通道 + runbook payload） | P1 | ⚪ | #001 |
| ISSUE-010 | P1a 端到端集成测试 | P0 | ⚪ | #008 #005 #006 |

---

## milestone-1 ~ milestone-4 概览

详见 `docs/TASK_BREAKDOWN.md`。阶段 1+ 的 issue 当前仅有骨架（标题 + labels + 摘要 + 依赖），系统会在该 milestone 开始前由 Codex 读取实际代码后自动补全完整 8 段 body。

| 里程碑 | 覆盖 §12.3 矩阵行 | 关键交付 |
|--------|-------------------|----------|
| milestone-1 | 第 1-3 行（数据延迟 / llm_health_check / dbt test fail） | Phase 0 失败矩阵完整，partial rerun 可用 |
| milestone-2 | 第 4-8 行（Phase 1/2/3 主链 + inconclusive + manifest repair） | 四阶段闭环与 formal/manifest 发布 |
| milestone-3 | 第 9 行 + 全矩阵冻结 | 多通道告警、失败注入、静态越界检查 |
| milestone-4 | 无新增矩阵条目 | Temporal pause/resume 与 parity test |

---

## 风险与 Blocker

当前无 Blocker。

**Blocker 升级条件（来自 CLAUDE.md）**：
- 需要 import 其他子项目私有内部（而非公开入口）
- 需要把 Kafka / Flink / CEP 放进本项目
- 需要把业务判断、存储写入或部署职责拉进编排层
- 无法给出可重复运行的最小 cycle 或 Gate 验证路径

---

## 验收标准追踪（§23）

| 验收条目 | 对应里程碑 | 状态 |
|----------|------------|------|
| 不复制业务实现完成装配 | milestone-0 ~ 4 全程守 | ⚪ 持续验证中 |
| Phase 0-3 顺序 + Gate 矩阵 + 失败路径有文档与可运行实现 | milestone-3 完成 | ⚪ |
| llm_health_check / dbt test fail / inconclusive / manifest 补写四类关键场景可验证 | milestone-1 + milestone-2 | ⚪ |
| 不包含 Kafka/Flink 事件驱动与业务算法 | milestone-3 静态检查兜底 | ⚪ |
| 主项目角色与 `12 + N` 模块边界一致 | milestone-3 完成 | ⚪ |
