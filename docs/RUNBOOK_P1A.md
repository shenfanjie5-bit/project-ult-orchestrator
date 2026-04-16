# P1a 最小骨架 Runbook

### 本地启动

1. 安装开发依赖：
   ```bash
   python3 -m pip install -e ".[dev]"
   ```
2. 编译占位 dbt project：
   ```bash
   make dbt-compile
   ```
3. 跑 P1a 端到端集成测试：
   ```bash
   make integration-test
   ```
4. 需要看 Dagster UI 时再启动：
   ```bash
   make dagster-dev
   ```

### 常见错误

- `dagster is not installed`：当前环境没有安装开发依赖，重新执行 `python3 -m pip install -e ".[dev]"`。
- `dbt CLI is not installed`：确认 dev extra 中的 `dbt-core` 与 `dbt-duckdb` 已安装。
- `manifest.json` 缺失：先执行 `make dbt-compile`，或删除 `dbt_stub/target` 后重新编译。
- DuckDB 文件无法创建：确认工作区可写，`dbt_stub/dagster_home` 可由当前用户创建。

### 替换点

- `dbt_stub/` 只用于 P1a 骨架；接入 `data-platform` 后替换为其公开 dbt project 入口。
- `phase0_readiness_ping` 是 API 占位 asset；接入真实 readiness API 时保持 orchestrator 只装配，不复制业务逻辑。
- `candidate_freeze` 属于 Phase 0 asset group，但业务实现由 `data-platform` 的 `AssetFactoryProvider.get_assets()` 提供；orchestrator 只校验 asset key 与 `phase0` group 并纳入 `daily_cycle_job` 选择。
- `phase0_ping_check` 只做 Gate policy wiring；真实检查函数应由上游 `PureCheckProvider` 暴露。
- `GatePolicyResource` 当前读取 `config/policy/gate_policy.lite.yaml`；环境装配交给 `assembly` 后只替换 policy 路径。

### 回滚步骤

1. 删除 `tests/integration/` 与 `docs/RUNBOOK_P1A.md`。
2. 从 `Makefile` 移除 `integration-test` 目标。
3. 如不再需要本地 dbt 骨架，移除 dev extra 中的 dbt 依赖。
