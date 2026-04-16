# P5 Gate Runbook

本 runbook 面向 P5 影子运行值班处理。告警中的 `runbook_url` 使用仓库内相对路径和锚点，锚点由 `phase/failure_class/action` 组合生成。

### 通用处理原则

1. 先确认告警 payload 的 `cycle_id`、`phase`、`failed_node`、`failure_class`、`action`。
2. 只在对应上游模块或运行环境中排查业务原因；`orchestrator` 只负责装配、分类、告警和 rerun/repair 请求。
3. 处理后用本节给出的验证命令或对应测试确认路径恢复。

<a id="phase0-data_quality-fail_run"></a>
## phase0-data_quality-fail_run

场景：`phase0_data_readiness_delayed`

适用：Phase 0 数据 readiness 失败，市场数据延迟或不可用。

处理路径：

1. 查看 payload 的 `failed_node`，确认失败来自 data readiness provider。
2. 到 `data-platform` 或上游数据源检查数据到达、水位、交易日配置。
3. 数据恢复后重新触发日频 cycle，不在 `orchestrator` 中补写业务数据。

验证：

```bash
python3 -m pytest tests/sensors/test_data_readiness.py tests/integration/test_phase0_failure_matrix.py -q
```

<a id="phase0-infra-fail_run"></a>
## phase0-infra-fail_run

场景：`phase0_llm_health_check_failed`

适用：Phase 0 LLM health check 失败，需要在进入 Phase 1 前硬停。

处理路径：

1. 确认 `reasoner-runtime` health check 返回的 provider、错误摘要和凭据状态。
2. 修复 provider 配置、网络出口或限流问题。
3. health check 通过后重新触发 cycle。

验证：

```bash
python3 -m pytest tests/checks/test_llm_health_check.py -q
```

<a id="phase0-task_level-partial_rerun"></a>
## phase0-task_level-partial_rerun

场景：`phase0_dbt_test_failed`

适用：Phase 0 dbt test 失败，Gate action 为 `partial_rerun`。

处理路径：

1. 查看 alert summary 中的 dbt test 名称、asset key、rerun request 写入状态。
2. 修复 dbt model 或上游输入后，使用生成的 rerun request 选择最小 asset selection。
3. 不扩大 rerun 到整个 phase，除非 request 生成失败且人工确认需要更大范围。

验证：

```bash
python3 -m pytest tests/checks/test_dbt_events.py -q
```

<a id="phase1-publish-fail_run"></a>
## phase1-publish-fail_run

场景：`phase1_graph_promotion_snapshot_failed`

适用：Phase 1 graph promotion 或 snapshot 失败，保留上一版 ready graph。

处理路径：

1. 确认失败节点是 graph promotion 或 snapshot asset。
2. 到 `graph-engine` 检查纯检查函数输出和 graph artifact 完整性。
3. 修复后重新执行 Phase 1；不要在编排层复制 graph 业务逻辑。

验证：

```bash
python3 -m pytest tests/checks/test_phase1_gate.py tests/integration/test_phase1_graph_gate.py -q
```

<a id="phase2-task_level-mark_inconclusive"></a>
## phase2-task_level-mark_inconclusive

场景：`phase2_single_stock_task_failed`

适用：Phase 2 单股票任务失败但未超过容忍阈值，标记为 `mark_inconclusive` 并继续其他股票。

处理路径：

1. 记录 payload 中的股票、失败节点和 failure rate。
2. 在 `main-core` 检查单股票任务输入、LLM 响应和输出校验。
3. 仅对受影响股票做后续补跑或人工审阅；不要阻塞整个 pool，除非 pool gate 后续失败。

验证：

```bash
python3 -m pytest tests/checks/test_phase2_single_stock_gate.py tests/integration/test_phase2_inconclusive_path.py -q
```

<a id="phase2-data_quality-fail_run"></a>
## phase2-data_quality-fail_run

场景：`phase2_pool_failure_rate_exceeded`

适用：Phase 2 pool failure rate 超过 policy 阈值，整轮失败并告警。

处理路径：

1. 查看 payload 的 `failed_node` 列表和 summary 中的失败比例。
2. 到 `main-core` 检查 pool 级输入、任务分布和失败类别。
3. 修复后重新触发 cycle 或按事故处理流程重跑 Phase 2。

验证：

```bash
python3 -m pytest tests/checks/test_phase2_pool_gate.py tests/integration/test_phase2_pool_failure_gate.py -q
```

<a id="phase3-publish-fail_run"></a>
## phase3-publish-fail_run

场景：`phase3_formal_commit_failed`

适用：Phase 3 formal table commit 失败，manifest 写入前硬停。

处理路径：

1. 确认失败发生在 formal objects commit，而不是 manifest repair 路径。
2. 到 `main-core` 发布入口检查事务、目标表 schema、权限和幂等状态。
3. commit 成功前不要手工写 manifest。

验证：

```bash
python3 -m pytest tests/checks/test_phase3_publish_gate.py tests/integration/test_phase3_publish_wiring.py -q
```

<a id="phase3-infra-repair_manifest"></a>
## phase3-infra-repair_manifest

场景：`phase3_manifest_write_failed`

适用：Phase 3 manifest 写入失败，Gate action 为 `repair_manifest`。

处理路径：

1. 确认 payload 的 `failed_node` 是 `cycle_publish_manifest`。
2. 使用 repair-only rerun request，只选择注册的 manifest repair asset。
3. repair 成功后核对 manifest 内容指向已提交的 formal objects，不重新提交业务表。

验证：

```bash
python3 -m pytest tests/checks/test_phase3_manifest_repair.py tests/integration/test_phase3_manifest_repair_flow.py -q
```

<a id="phase2-infra-fail_run"></a>
## phase2-infra-fail_run

场景：`infra_unavailable_hard_stop`

适用：核心存储、Dagster instance、graph backend 或其他基础设施不可用。矩阵入口记录为 `phase2/infra/fail_run`，但 `applies_to_phases` 覆盖 Phase 0 到 Phase 3。

处理路径：

1. 确认 payload 中的 phase 和 failed node，判断是存储、实例连接、graph backend 还是资源装配失败。
2. 修复基础设施或 assembly 提供的环境配置。
3. 基础设施恢复后重新触发受影响 cycle；不要在业务模块中绕过 gate。

验证：

```bash
python3 -m pytest tests/resources/test_bundle.py tests/policy/test_gate_matrix_coverage.py -q
```
