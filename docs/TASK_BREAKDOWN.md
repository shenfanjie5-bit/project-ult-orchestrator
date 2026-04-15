# 项目任务拆解

> 源文档：`docs/orchestrator.project-doc.md`
> 依据章节：§1 / §4 / §5.4 / §8 / §12 / §14 / §16 / §18 / §21 / §23
> 里程碑映射：§21 的阶段 0-4 直接对应 milestone-0 ~ milestone-4

---

## 阶段 0：P1a Dagster 骨架

**目标**：建立最小 Dagster 工程骨架，能在 UI 中看到最小依赖图并成功执行一次最小 cycle。
**前置依赖**：无

### ISSUE-001: 初始化 orchestrator Python 包结构与依赖基线
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §14 与 §15，`orchestrator` 是 monorepo 下独立的 Python package，默认执行引擎为 Dagster，Python 版本要求 ≥ 3.12。当前仓库仅有空 `pyproject.toml`，需要先搭建 src-layout 包骨架、依赖声明与 pytest 配置，为后续所有模块提供落脚点。本 issue 不做任何业务功能，只确保 `pip install -e .` 与 `pytest` 可跑通。

#### 所属模块
`orchestrator` / 仓库根（pyproject、src-layout、tests 基线）

#### 实现范围
- 修改 `pyproject.toml`：`requires-python = ">=3.12"`；在 `[project.dependencies]` 中加入 `dagster`、`dagster-dbt`、`pydantic`、`pyyaml`；在 `[project.optional-dependencies]` 中加入 `dev = ["pytest", "pytest-asyncio", "mypy"]`。
- 创建 src-layout：`src/orchestrator/__init__.py`（仅包含 `__version__ = "0.1.0"` 与 docstring），并在 `[tool.setuptools.packages.find]` 中声明 `where = ["src"]`。
- 创建 9 个子包的占位目录（每个目录下仅 `__init__.py`）：`definitions/`、`jobs/`、`schedules/`、`sensors/`、`resources/`、`policy/`、`checks/`、`alerting/`、`temporal/`（§14 列出的 9 个模块）。
- 创建 `tests/` 目录与 `tests/__init__.py`、`tests/conftest.py`（空 fixture）。
- 在仓库根新增 `Makefile`，目标至少包含 `install`、`test`、`lint`、`dagster-dev`。
- 更新 `.gitignore`，忽略 `.venv/`、`dagster_home/`、`__pycache__/`、`*.egg-info/`。

#### 不在本次范围
- 不实现任何 Dagster asset / job / resource。
- 不引入 Kafka / Flink / Temporal 依赖。
- 不接入 `contracts` 或下游模块的真实 import。
- 不创建 Gate policy 配置文件。

#### 关键交付物
- `pyproject.toml` 更新后 `pip install -e ".[dev]"` 成功且锁定 Python ≥ 3.12。
- `src/orchestrator/__init__.py` 导出 `__version__: str`。
- 9 个子模块目录均存在且 `import orchestrator.<sub>` 不报错。
- `tests/test_smoke.py` 包含 `def test_import_orchestrator(): import orchestrator; assert orchestrator.__version__ == "0.1.0"`。
- `Makefile` 中 `make test` 等价于 `pytest -q`，`make dagster-dev` 等价于 `DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions`。
- `.gitignore` 覆盖 Python 与 Dagster 临时目录。

#### 验收标准
- [ ] `pip install -e ".[dev]"` 在干净虚拟环境中 30 秒内完成且无错误。
- [ ] `python -c "import orchestrator; print(orchestrator.__version__)"` 输出 `0.1.0`。
- [ ] 9 个子模块 `import orchestrator.definitions/jobs/schedules/sensors/resources/policy/checks/alerting/temporal` 全部成功。
- [ ] `pytest -q` 通过且至少执行 1 个测试（test_smoke）。
- [ ] `pyproject.toml` 中 `requires-python` 为 `>=3.12`。
- [ ] `make dagster-dev` 的命令串中包含 `-m orchestrator.definitions`。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -c "import orchestrator; print(orchestrator.__version__)"
python -c "import orchestrator.definitions, orchestrator.jobs, orchestrator.schedules, orchestrator.sensors, orchestrator.resources, orchestrator.policy, orchestrator.checks, orchestrator.alerting, orchestrator.temporal"
pytest -q
```

#### 依赖
无前置依赖

---

### ISSUE-002: 定义 contracts 消费适配层（禁止自定义类型）
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §5.4 与 CLAUDE.md 核心约束 3，Gate 类型、错误分类、阶段枚举必须来自 `contracts`，`orchestrator` 只能消费不能自定义第二套。由于当前 monorepo 中 `contracts` 仍在建设，本 issue 在 `orchestrator.policy` 内建立**适配层**，用 `typing.Protocol` 或可替换 stub 定义 `PhaseEnum`、`FailureClassEnum`、`ActionEnum` 等类型别名，在 `contracts` 就绪后一行替换即可切换。目的是提前冻结"只能从单一来源取类型"的依赖约束。

#### 所属模块
`orchestrator.policy` / `orchestrator.policy.contracts_adapter`

#### 实现范围
- 创建 `src/orchestrator/policy/contracts_adapter.py`：定义 `PhaseEnum`（`PHASE0`/`PHASE1`/`PHASE2`/`PHASE3`）、`FailureClass`（`INFRA`/`DATA_QUALITY`/`TASK_LEVEL`/`PUBLISH`）、`GateAction`（`CONTINUE`/`FAIL_RUN`/`PARTIAL_RERUN`/`MARK_INCONCLUSIVE`/`REPAIR_MANIFEST`）。
- 在文件头注释中显式标注 `# TODO(contracts): replace local Enum with `from contracts.gate import ...` once contracts package is published.`。
- 提供 `CONTRACTS_VERSION: str = "stub-0.1"` 常量，并导出 `ALL_PHASE_NAMES`、`ALL_FAILURE_CLASSES`、`ALL_ACTIONS` 元组便于校验。
- 编写 `tests/policy/test_contracts_adapter.py`：断言 4 个 phase、4 个 failure class、5 个 action 数量与名称完全匹配 §9.3 / §12.3 表格。
- 在 `src/orchestrator/policy/__init__.py` 中 re-export 三个 Enum 与 `CONTRACTS_VERSION`。

#### 不在本次范围
- 不 import 真正的 `contracts` package（尚未就绪）。
- 不定义 Gate 策略具体数值、阈值或 YAML schema（见 ISSUE-003）。
- 不在 asset/check 代码中直接使用这些 enum（见 ISSUE-005）。

#### 关键交付物
- `PhaseEnum(Enum)`：成员 `PHASE0 = "phase0"`、`PHASE1 = "phase1"`、`PHASE2 = "phase2"`、`PHASE3 = "phase3"`。
- `FailureClass(Enum)`：4 个成员，值为 snake_case 字符串。
- `GateAction(Enum)`：5 个成员，值与 §9.3 `GateDecision.action` 字段枚举一致。
- `ALL_PHASE_NAMES: tuple[str, ...] = ("phase0", "phase1", "phase2", "phase3")`。
- `CONTRACTS_VERSION` 字符串常量，供 policy 文件在加载时做版本对齐校验。
- Adapter 文件顶部必须有 `TODO(contracts)` 注释指明替换点。

#### 验收标准
- [ ] `from orchestrator.policy import PhaseEnum, FailureClass, GateAction, CONTRACTS_VERSION` 成功。
- [ ] `len(PhaseEnum) == 4 and len(FailureClass) == 4 and len(GateAction) == 5`。
- [ ] `PhaseEnum.PHASE0.value == "phase0"`、`GateAction.MARK_INCONCLUSIVE.value == "mark_inconclusive"`。
- [ ] `tests/policy/test_contracts_adapter.py` 全部通过。
- [ ] Grep `rg "class.*Enum" src/orchestrator/` 只在 `contracts_adapter.py` 中命中（防止散落）。
- [ ] 文件顶部含有 `TODO(contracts)` 替换标记。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/policy/test_contracts_adapter.py -v
python -c "from orchestrator.policy import PhaseEnum, FailureClass, GateAction; print(len(PhaseEnum), len(FailureClass), len(GateAction))"
rg "class.*\(Enum\)" src/orchestrator/ --type py
```

#### 依赖
依赖 #ISSUE-001（需要包结构与 pytest 运行环境）

---

### ISSUE-003: Gate Policy schema 与 YAML 加载器
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §9.3 `GatePolicyProfile`、§12.3 Gate 行为矩阵与 §16.1 `load_gate_policy()`，Gate 行为矩阵、阈值、执行 backend 必须保存为可审计的 YAML/TOML 文件。本 issue 只实现**加载器骨架**：定义 Pydantic schema、读取 YAML、与 contracts adapter 做 enum 校验，不实现 classify 逻辑（见 ISSUE-005）。所有后续 issue 都会从这里消费 policy 对象。

#### 所属模块
`orchestrator.policy` / `orchestrator.policy.loader` / `orchestrator.policy.schema`

#### 实现范围
- 创建 `src/orchestrator/policy/schema.py`，用 Pydantic v2 定义 `GatePolicyProfile`，字段与 §9.3 一致：`policy_version: str`、`contract_version: str`、`execution_backend: Literal["dagster_only", "dagster_plus_temporal"]`、`phase_matrix: list[PhaseMatrixEntry]`、`thresholds: dict[str, float]`、`alert_channels: list[str]`、`updated_at: datetime`。
- `PhaseMatrixEntry`：`phase: PhaseEnum`、`failure_class: FailureClass`、`action: GateAction`、`allow_partial_rerun: bool`、`description: str`。
- 创建 `src/orchestrator/policy/loader.py`，实现 `load_gate_policy(path: Path | str) -> GatePolicyProfile`：读取 YAML，用 Pydantic 校验，若 `contract_version != CONTRACTS_VERSION` 记录 WARNING 但不失败（P1a 宽松模式）。
- 创建 `config/policy/gate_policy.lite.yaml`：包含 §12.3 全部 9 条矩阵条目，阈值部分填最小集（`phase2_pool_failure_rate: 0.30`、`phase2_single_stock_tolerance: 0.50`）。
- 编写 `tests/policy/test_loader.py`：用例覆盖加载成功、字段缺失报错、非法 enum 值报错、contract_version 不匹配时的 warning。
- 在 `src/orchestrator/policy/__init__.py` re-export `GatePolicyProfile`、`load_gate_policy`。

#### 不在本次范围
- 不实现 `classify_gate_result()`（ISSUE-005）。
- 不实现 `plan_partial_rerun()`（阶段 1）。
- 不支持 TOML（默认仅 YAML）。
- 不支持 policy 热加载或多版本切换。

#### 关键交付物
- `GatePolicyProfile` Pydantic 模型，支持 `.model_validate(...)`。
- `load_gate_policy(path) -> GatePolicyProfile`：签名、异常类型（`FileNotFoundError` / `pydantic.ValidationError`）。
- `config/policy/gate_policy.lite.yaml` 覆盖 §12.3 全部 9 条失败类型。
- Policy 中 `phase` / `failure_class` / `action` 字段必须能反序列化为 contracts adapter 的 Enum。
- 至少 5 个单元测试覆盖加载正常、文件缺失、字段缺失、enum 非法、version mismatch warning。

#### 验收标准
- [ ] `load_gate_policy("config/policy/gate_policy.lite.yaml")` 返回非空 `GatePolicyProfile`。
- [ ] `profile.phase_matrix` 长度为 9（覆盖 §12.3 全部条目）。
- [ ] 删除 YAML 中 `execution_backend` 字段后加载抛 `ValidationError`。
- [ ] 把任一 `action` 改成非法值（如 `"ignore"`）后加载抛 `ValidationError`。
- [ ] `profile.execution_backend` 只接受 `dagster_only` / `dagster_plus_temporal` 两个值。
- [ ] `pytest tests/policy/test_loader.py -v` 全绿，至少 5 条用例。
- [ ] YAML 文件中阈值 hard-code 数字位于 `thresholds:` 键下，Python 源码中搜索不到这些数值。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/policy/test_loader.py -v
python -c "from orchestrator.policy import load_gate_policy; p = load_gate_policy('config/policy/gate_policy.lite.yaml'); print(p.policy_version, len(p.phase_matrix))"
rg "0\.30|0\.50" src/orchestrator/ --type py
```

#### 依赖
依赖 #ISSUE-002（需要 PhaseEnum / FailureClass / GateAction）

---

### ISSUE-004: ResourceBundle 装配骨架与只读注入边界
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §9.3 `ResourceBundle` 与 §16.1 `build_resource_bundle()`，`orchestrator` 需要统一装配 `tushare_client`、`litellm_client`、`duckdb_conn` 等资源并注入给 Dagster。本 issue 搭建 **resource 装配容器与 Provider 协议**，不实现具体 resource 的业务 IO，真实资源工厂来自 `data-platform` / `reasoner-runtime`（在阶段 1 接入）。P1a 用 `stub` resource（返回常量字符串）跑通骨架即可。

#### 所属模块
`orchestrator.resources` / `orchestrator.resources.bundle` / `orchestrator.resources.providers`

#### 实现范围
- 创建 `src/orchestrator/resources/bundle.py`：定义 dataclass `ResourceBundle`（字段见 §9.3：`resource_keys: tuple[str, ...]`、`source_modules: tuple[str, ...]`、`config_ref: str`、`injected_at: datetime`、`read_only: bool`）。
- 定义 `build_resource_bundle(config_ref: str, providers: Iterable[AssetFactoryProvider]) -> dict[str, dagster.ResourceDefinition]`：返回 Dagster `resources` 字典。
- 在 `src/orchestrator/resources/providers.py` 定义 `AssetFactoryProvider` Protocol（见 §16.2），方法：`get_assets() -> Sequence`、`get_checks() -> Sequence`、`get_resources() -> Mapping[str, ResourceDefinition]`。
- 创建 `src/orchestrator/resources/_stub_provider.py`：实现 `StubProvider`，`get_resources()` 返回 1 个 `ResourceDefinition`，key 为 `orchestration_context_stub`，value 是返回 `{"mode": "stub"}` 的 ConfigurableResource。
- 编写 `tests/resources/test_bundle.py`：至少 3 个用例——stub provider 注入成功、重复 resource key 抛 `ValueError`、`read_only=True` 时修改 resource 抛异常。
- 在 `src/orchestrator/resources/__init__.py` re-export `ResourceBundle`、`build_resource_bundle`、`AssetFactoryProvider`。

#### 不在本次范围
- 不实现真正的 `tushare_client` / `litellm_client`（阶段 1）。
- 不连接 PostgreSQL / Neo4j（阶段 2）。
- 不做 Temporal resource（阶段 4）。
- 不实现配置动态刷新。

#### 关键交付物
- `ResourceBundle` dataclass，5 个字段与 §9.3 严格一致。
- `build_resource_bundle(config_ref, providers)` 函数签名及行为：收集所有 provider 的 `get_resources()`，键冲突立即抛 `ValueError("duplicate resource key: X")`。
- `AssetFactoryProvider` Protocol（使用 `typing.runtime_checkable`）。
- `StubProvider` 仅供 P1a 骨架使用，必须在 docstring 标注 `# only for P1a bootstrap; replaced by data-platform providers in milestone-1`。
- 测试覆盖：resource key 冲突、read-only 违约、stub provider 成功注入。

#### 验收标准
- [ ] `from orchestrator.resources import ResourceBundle, build_resource_bundle, AssetFactoryProvider` 成功。
- [ ] `StubProvider().get_resources()` 返回非空字典且 key 为 `orchestration_context_stub`。
- [ ] 传入两个带同名 resource key 的 provider 时 `build_resource_bundle` 抛 `ValueError`，消息包含冲突的 key 名。
- [ ] `ResourceBundle(read_only=True)` 实例在 `dataclasses.replace(...)` 之外修改字段抛 `FrozenInstanceError` 或等价异常。
- [ ] `pytest tests/resources/test_bundle.py -v` 全绿，≥3 条用例。
- [ ] `AssetFactoryProvider` 是 `@runtime_checkable` Protocol，`isinstance(StubProvider(), AssetFactoryProvider)` 返回 `True`。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/resources/test_bundle.py -v
python -c "from orchestrator.resources import build_resource_bundle; from orchestrator.resources._stub_provider import StubProvider; print(list(build_resource_bundle('lite', [StubProvider()]).keys()))"
```

#### 依赖
依赖 #ISSUE-001（包结构）

---

### ISSUE-005: 最小 AssetCheck 与 classify_gate_result 纯函数
**labels**: P0, algorithm, milestone-0, ready

#### 背景与目标
根据 §11.2 Gate 分类算法与 §16.1 `classify_gate_result()`，编排层需要把"某次失败事件 + phase + policy"映射为 `GateDecision`。本 issue 先实现**纯函数版本**（无 Dagster 依赖），再用一个最小 Dagster `@asset_check` 包一层，让 Phase 0 骨架能在 Dagster UI 中看到 1 个检查节点并得到 `continue` / `fail_run` 结论。分类表严格查 policy，不写任何业务判断逻辑。

#### 所属模块
`orchestrator.checks` / `orchestrator.checks.classifier` / `orchestrator.checks.asset_checks`

#### 实现范围
- 创建 `src/orchestrator/checks/classifier.py`，实现纯函数 `classify_gate_result(phase: PhaseEnum, failure_class: FailureClass | None, policy: GatePolicyProfile) -> GateDecision`：若 `failure_class is None` 返回 `action=CONTINUE`；否则查 `policy.phase_matrix` 中 `(phase, failure_class)` 对应条目，返回 `GateDecision(phase=..., failure_class=..., action=entry.action, reason=entry.description)`；未命中抛 `UnknownGateFailure` 异常。
- 创建 `src/orchestrator/checks/models.py`：定义 `GateDecision` dataclass（字段见 §9.3，不含 `decision_id` / `cycle_id`——由调用方填入）。
- 创建 `src/orchestrator/checks/asset_checks.py`：用 `@asset_check(asset="phase0_readiness_ping")` 装饰一个函数 `phase0_ping_check`，内部调用 `classify_gate_result(PhaseEnum.PHASE0, None, policy)`，返回 `AssetCheckResult(passed=True)`。policy 通过 Dagster resource 注入（key: `gate_policy`）。
- 创建 `src/orchestrator/checks/resources.py`：定义 `GatePolicyResource(ConfigurableResource)`，字段 `policy_path: str`，`setup_for_execution` 中调用 `load_gate_policy`。
- 编写 `tests/checks/test_classifier.py`：至少 6 条用例——`(PHASE0, None)` → continue；`(PHASE0, INFRA)` → fail_run；`(PHASE2, TASK_LEVEL)` → mark_inconclusive；未知组合抛 `UnknownGateFailure`；policy 中同时存在两条相同 `(phase, failure_class)` 时加载阶段应已拒绝（回归测试）。
- 在 `src/orchestrator/checks/__init__.py` re-export `classify_gate_result`、`GateDecision`、`UnknownGateFailure`、`phase0_ping_check`、`GatePolicyResource`。

#### 不在本次范围
- 不实现 `plan_partial_rerun()`（阶段 1）。
- 不写告警下发（ISSUE-008）。
- 不做多 phase 并发分类。
- `AssetCheck` 不做真实数据校验（只做 policy 查表）。

#### 关键交付物
- `classify_gate_result` 签名与行为见上；**纯函数，无 IO**。
- `GateDecision` dataclass，字段严格对齐 §9.3。
- `UnknownGateFailure(Exception)` 自定义异常，消息包含 `phase` 与 `failure_class`。
- `GatePolicyResource` Dagster `ConfigurableResource`，`policy_path` 指向 YAML。
- `phase0_ping_check` 是合法 `AssetChecksDefinition`，被 ISSUE-008 的 `build_definitions()` 收集。
- 测试覆盖 continue / fail_run / mark_inconclusive / 未知失败 4 条主路径，总用例 ≥6。

#### 验收标准
- [ ] `classify_gate_result(PhaseEnum.PHASE0, FailureClass.INFRA, policy).action == GateAction.FAIL_RUN`。
- [ ] `classify_gate_result(PhaseEnum.PHASE2, FailureClass.TASK_LEVEL, policy).action == GateAction.MARK_INCONCLUSIVE`。
- [ ] `classify_gate_result(PhaseEnum.PHASE0, None, policy).action == GateAction.CONTINUE`。
- [ ] 未命中 policy 的 `(phase, failure_class)` 组合抛 `UnknownGateFailure`。
- [ ] `classify_gate_result` 无任何文件 IO、网络调用（通过 `grep -E "open\(|requests\.|urlopen"` 确认）。
- [ ] `phase0_ping_check` 可被 `dagster.Definitions(asset_checks=[phase0_ping_check])` 接收。
- [ ] `pytest tests/checks/ -v` 全绿，≥6 条用例。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/checks/ -v
python -c "from orchestrator.checks import classify_gate_result, GateAction; from orchestrator.policy import PhaseEnum, FailureClass, load_gate_policy; p = load_gate_policy('config/policy/gate_policy.lite.yaml'); print(classify_gate_result(PhaseEnum.PHASE0, FailureClass.INFRA, p))"
rg "open\(|requests\.|urlopen" src/orchestrator/checks/classifier.py
```

#### 依赖
依赖 #ISSUE-003（需要 `GatePolicyProfile` / `load_gate_policy`）

---

### ISSUE-006: Phase 0 最小 asset group 与 dbt 装配骨架
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §14 与 §21 阶段 0 退出条件"1 个 dbt asset group"，需要在 `orchestrator.jobs` 中装配一个最小 Phase 0 asset group，使用 `dagster-dbt` 把占位 dbt project 挂到执行图。由于真正的 dbt model 来自 `data-platform`，本 issue 只创建最小 stub dbt project（1 个 source + 1 个 model + 1 个 test），证明 `dagster-dbt` 集成通路可用，阶段 1 再替换为真实 dbt project 入口。

#### 所属模块
`orchestrator.jobs` / `orchestrator.jobs.phase0` / `dbt_stub/`（仓库新增顶层目录）

#### 实现范围
- 在仓库根创建 `dbt_stub/` 目录：`dbt_project.yml`（project name `orchestrator_stub`，profile `orchestrator_stub`）、`profiles.yml`（使用 `duckdb` target 写入 `./dagster_home/stub.duckdb`）、`models/phase0/heartbeat.sql`（`select 1 as heartbeat`）、`models/phase0/schema.yml`（`not_null` test on `heartbeat`）。
- 创建 `src/orchestrator/jobs/phase0.py`：使用 `@dbt_assets(manifest=...)` 装饰器生成 Phase 0 dbt asset group；增加一个纯 Python `@asset(group_name="phase0")` 叫 `phase0_readiness_ping`，返回字符串 `"ok"`（用于被 ISSUE-005 的 `phase0_ping_check` 勾上）。
- 创建 `src/orchestrator/jobs/__init__.py` 导出 `phase0_readiness_ping` 与 `dbt_phase0_assets`（dbt asset group）。
- 创建 `Makefile` 目标 `dbt-compile`：`cd dbt_stub && dbt compile --profiles-dir . --project-dir .`。
- 编写 `tests/jobs/test_phase0.py`：断言 `phase0_readiness_ping` 存在、`group_name == "phase0"`、`AssetKey(["phase0_readiness_ping"])` 可寻址。
- 在 `dbt_stub/README.md` 中写明"P1a 骨架占位 dbt project，阶段 1 替换为 `data-platform/dbt` 入口"。

#### 不在本次范围
- 不实现 `data-platform` 的真实 dbt model（本 issue 只提供骨架）。
- 不做 Phase 1/2/3 的 asset group（阶段 1-2）。
- 不写候选池 freeze 逻辑（业务，归 `data-platform`）。
- 不集成真实 `llm_health_check`（阶段 2）。

#### 关键交付物
- `dbt_stub/dbt_project.yml` 与 `profiles.yml` 配置 DuckDB 本地 target，可被 `dbt compile` 识别。
- `phase0_readiness_ping` asset：`group_name="phase0"`、返回值字符串、无外部依赖。
- `dbt_phase0_assets`：通过 `@dbt_assets(manifest=PROJECT_DIR / "target" / "manifest.json")` 定义。
- `DBT_PROJECT_DIR` / `DBT_PROFILES_DIR` 常量在 `src/orchestrator/jobs/phase0.py` 顶部定义，默认值为相对于仓库根的 `dbt_stub`，允许环境变量 `ORCHESTRATOR_DBT_PROJECT_DIR` 覆盖（`assembly` 后续使用）。
- Makefile `dbt-compile` 目标可生成 `dbt_stub/target/manifest.json`。

#### 验收标准
- [ ] `cd dbt_stub && dbt compile` 成功并生成 `target/manifest.json`。
- [ ] `python -c "from orchestrator.jobs import phase0_readiness_ping; print(phase0_readiness_ping.key)"` 输出 `AssetKey(['phase0_readiness_ping'])`。
- [ ] `python -c "from orchestrator.jobs.phase0 import dbt_phase0_assets; print(len(list(dbt_phase0_assets.keys)) > 0)"` 输出 `True`。
- [ ] `phase0_readiness_ping` 的 `group_name` 属性为 `"phase0"`。
- [ ] `pytest tests/jobs/test_phase0.py -v` 全绿。
- [ ] `dbt_stub/` 中无任何业务字段或真实数据源（仅 1 个 `select 1` 模型）。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pip install dbt-duckdb
cd dbt_stub && dbt compile --profiles-dir . --project-dir . && cd ..
pytest tests/jobs/test_phase0.py -v
python -c "from orchestrator.jobs import phase0_readiness_ping; print(phase0_readiness_ping.key, phase0_readiness_ping.group_names_by_key)"
```

#### 依赖
依赖 #ISSUE-001（包结构）

---

### ISSUE-007: 交易日 schedule 与最小 sensor 骨架
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §12.1 触发源类型与 §21 阶段 0 退出条件"1 个 schedule"，需要定义 1 条日频 schedule 与 1 个状态感知 sensor 骨架。schedule 固定每个交易日 17:00 触发（只用 cron 表达式，不做交易日历过滤——交易日过滤逻辑属于业务）。sensor 只挂一个 `data_readiness_sensor` 骨架，永远返回 `SkipReason("not wired in P1a")`，阶段 1 再接入真实 readiness 信号。

#### 所属模块
`orchestrator.schedules` / `orchestrator.sensors`

#### 实现范围
- 创建 `src/orchestrator/schedules/daily_cycle.py`：`daily_cycle_schedule = ScheduleDefinition(job=daily_cycle_job, cron_schedule="0 17 * * 1-5", execution_timezone="Asia/Shanghai", name="daily_cycle_schedule")`；job 引用见 ISSUE-008。
- 创建 `src/orchestrator/sensors/data_readiness.py`：`@sensor(job=daily_cycle_job, name="data_readiness_sensor")` 装饰的函数，永远返回 `SkipReason("readiness wiring deferred to milestone-1")`。
- 在 `src/orchestrator/schedules/__init__.py` / `sensors/__init__.py` 分别 re-export。
- 编写 `tests/schedules/test_daily_cycle.py`：断言 `cron_schedule == "0 17 * * 1-5"`、`execution_timezone == "Asia/Shanghai"`、`name == "daily_cycle_schedule"`。
- 编写 `tests/sensors/test_data_readiness.py`：调用 sensor 的 `evaluation_fn` 后断言返回 `SkipReason`。

#### 不在本次范围
- 不实现交易日历过滤（业务逻辑，不在编排层）。
- 不接真实 readiness 信号（阶段 1）。
- 不做 manual rerun sensor（阶段 1）。
- 不做 Temporal signal sensor（阶段 4）。

#### 关键交付物
- `daily_cycle_schedule: ScheduleDefinition`，cron 严格为 `"0 17 * * 1-5"`，timezone `Asia/Shanghai`。
- `data_readiness_sensor: SensorDefinition`，`evaluation_fn` 固定返回 `SkipReason`。
- 两个 `name` 字段在 Dagster 命名空间中唯一且与文件名一致。
- 单元测试直接调用 sensor 函数无需 Dagster instance。

#### 验收标准
- [ ] `daily_cycle_schedule.cron_schedule == "0 17 * * 1-5"` 且 `execution_timezone == "Asia/Shanghai"`。
- [ ] `data_readiness_sensor` 的评估结果是 `SkipReason` 类型，消息包含 `"milestone-1"`。
- [ ] `pytest tests/schedules/ tests/sensors/ -v` 全绿。
- [ ] 两个定义可被 `Definitions(schedules=[daily_cycle_schedule], sensors=[data_readiness_sensor])` 收集且 `dagster dev` 不抛错。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/schedules/ tests/sensors/ -v
python -c "from orchestrator.schedules import daily_cycle_schedule; print(daily_cycle_schedule.cron_schedule, daily_cycle_schedule.execution_timezone)"
```

#### 依赖
依赖 #ISSUE-008（schedule 引用的 `daily_cycle_job` 需要先定义）。**实施顺序**：先实现 ISSUE-008 的 job 骨架再回填本 issue 的 schedule/sensor；或两者同一 PR 合入。

---

### ISSUE-008: build_definitions() 入口与 daily_cycle_job 装配
**labels**: P0, infrastructure, milestone-0, ready

#### 背景与目标
根据 §11.1 执行图编译算法与 §16.1 `build_definitions()`，需要把 asset / check / resource / schedule / sensor 汇总为一个 `Definitions` 对象，供 `dagster dev -m orchestrator.definitions` 加载。本 issue 是 P1a 骨架的**汇总入口**，也是整个阶段 0 退出条件的核心（Dagster UI 能看到依赖图）。同时定义 `daily_cycle_job` 作为 Phase 0 最小 job。

#### 所属模块
`orchestrator.definitions` / `orchestrator.jobs.cycle`

#### 实现范围
- 创建 `src/orchestrator/jobs/cycle.py`：`daily_cycle_job = define_asset_job(name="daily_cycle_job", selection=AssetSelection.groups("phase0"))`。
- 创建 `src/orchestrator/definitions.py`（顶层模块文件，不是目录里的 `__init__.py`）：
  - 函数 `build_definitions(policy_path: str | None = None, providers: Iterable[AssetFactoryProvider] | None = None) -> Definitions`。
  - 默认 `policy_path = os.environ.get("ORCHESTRATOR_POLICY_PATH", "config/policy/gate_policy.lite.yaml")`。
  - 默认 `providers = [StubProvider()]`。
  - 收集：`phase0_readiness_ping` + `dbt_phase0_assets` + `phase0_ping_check` + `daily_cycle_job` + `daily_cycle_schedule` + `data_readiness_sensor` + `GatePolicyResource(policy_path=policy_path)` + `build_resource_bundle(...)` 的结果。
  - 返回 `Definitions(assets=[...], asset_checks=[...], jobs=[...], schedules=[...], sensors=[...], resources={...})`。
- 在模块底部暴露 `defs = build_definitions()`，供 `dagster dev -m orchestrator.definitions` 识别。
- 编写 `tests/test_definitions.py`：加载 `build_definitions()`、断言 `len(defs.jobs) == 1`、`len(defs.schedules) == 1`、`len(defs.sensors) == 1`、`"gate_policy" in defs.resources`、`phase0_readiness_ping` asset 在 job selection 中。
- 在 `src/orchestrator/__init__.py` re-export `build_definitions`。
- 在 `README.md` 末尾加一节"本地运行 Dagster UI"，给出 `dagster dev -m orchestrator.definitions` 命令。

#### 不在本次范围
- 不汇入 Phase 1/2/3 asset / check / job（阶段 1-2）。
- 不做 Temporal backend 分支（阶段 4）。
- 不做运行时 policy reload（永久后置）。
- 不对接告警 Webhook（ISSUE-009）。

#### 关键交付物
- `build_definitions(policy_path, providers) -> Definitions` 签名。
- 模块级 `defs` 变量可被 `dagster dev -m orchestrator.definitions` 加载。
- 默认 policy 路径可通过 `ORCHESTRATOR_POLICY_PATH` 环境变量覆盖。
- 装配出的 `Definitions` 中至少包含：2 个 asset（`phase0_readiness_ping` + 1 个 dbt model）、1 个 asset_check、1 个 job、1 个 schedule、1 个 sensor、2 个 resource（`gate_policy` + `orchestration_context_stub`）。
- 集成测试验证 Dagster 内部一致性校验通过（无 cycle、无 missing dep）。

#### 验收标准
- [ ] `python -c "from orchestrator.definitions import defs; print(len(defs.jobs), len(defs.schedules))"` 输出 `1 1`。
- [ ] `DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions` 启动后 UI 可访问且未报 `DagsterInvalidDefinitionError`。
- [ ] `build_definitions()` 返回对象中 `"gate_policy" in resources` 且 `"orchestration_context_stub" in resources`。
- [ ] `pytest tests/test_definitions.py -v` 全绿。
- [ ] Dagster UI 中 `daily_cycle_job` 显示 ≥2 个 asset 节点与 ≥1 个 asset_check 节点。
- [ ] 手动从 UI 触发 `daily_cycle_job` 运行成功（状态 `SUCCESS`）。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/test_definitions.py -v
DAGSTER_HOME=$(pwd)/dagster_home dagster dev -m orchestrator.definitions &
DAGSTER_PID=$!
sleep 5
curl -s http://localhost:3000/server_info | head -c 200
kill $DAGSTER_PID
```

#### 依赖
依赖 #ISSUE-004（ResourceBundle）, #ISSUE-005（AssetCheck）, #ISSUE-006（phase0 assets）, #ISSUE-007（schedule/sensor；同 PR 合入亦可）

---

### ISSUE-009: alerting 骨架（logging 通道 + runbook payload）
**labels**: P1, infrastructure, milestone-0, ready

#### 背景与目标
根据 §13.3 Runbook / Alert Payload 与 §15 "告警输出 logging / Webhook（Lite 模式足够简单）"，Phase 0 骨架需要有一条最简告警路径，以便 ISSUE-005 的 `classify_gate_result` 在产出 `fail_run` 时有去向。本 issue 只实现 **logging 通道**（不做 Webhook、Slack），并冻结 `AlertPayload` dataclass 结构，阶段 3 再落多通道 dispatcher。

#### 所属模块
`orchestrator.alerting`

#### 实现范围
- 创建 `src/orchestrator/alerting/payload.py`：dataclass `AlertPayload`（字段见 §13.3：`cycle_id: str`、`phase: str`、`status: str`、`failed_node: str | None`、`action: str`、`summary: str`、`runbook_url: str | None = None`）。
- 创建 `src/orchestrator/alerting/dispatcher.py`：`dispatch_alert(payload: AlertPayload, channels: Iterable[str] = ("logging",)) -> None`。仅实现 `logging` 通道：`logger.warning(json.dumps(dataclasses.asdict(payload)))`。未知通道打 warning 但不 raise（P1a 宽松模式）。
- 在 `src/orchestrator/alerting/__init__.py` re-export `AlertPayload`、`dispatch_alert`。
- 编写 `tests/alerting/test_dispatcher.py`：用 `caplog` 验证 `dispatch_alert` 在 `logging` 通道下写出包含 `cycle_id` 与 `action` 字段的 JSON；未知通道时产生 `logger.warning("unknown alert channel: X")`。
- 在 `src/orchestrator/checks/asset_checks.py` 中不直接调 `dispatch_alert`（避免 AssetCheck 带副作用），只在 `GateDecision.action == FAIL_RUN` 时通过 op-level hook 触发（本 issue 只做 dispatcher 与 payload；hook 在阶段 1 接入）。

#### 不在本次范围
- 不实现 Slack / Webhook / PagerDuty 通道（阶段 3）。
- 不做 runbook URL 自动渲染（阶段 3）。
- 不做告警去重与节流（阶段 3）。
- 不做 Dagster op-level hook 连线（阶段 1）。

#### 关键交付物
- `AlertPayload` dataclass，7 字段与 §13.3 对齐。
- `dispatch_alert(payload, channels)`：默认 `("logging",)` 通道。
- JSON 序列化输出可被 `json.loads` 反序列化。
- 至少 3 条单元测试：logging 通道写出、未知通道 warning、空 payload 字段校验。

#### 验收标准
- [ ] `dispatch_alert(AlertPayload(cycle_id="c1", phase="phase0", status="failed", failed_node="phase0_readiness_ping", action="fail_run", summary="test"))` 在 `caplog` 中产生 WARNING 级记录且 JSON 可解析。
- [ ] 调用 `dispatch_alert(payload, channels=("unknown",))` 不抛异常，但 warning 日志包含 `"unknown alert channel"`。
- [ ] `AlertPayload` 的 `failed_node` / `runbook_url` 允许为 `None`，其余字段必填。
- [ ] `pytest tests/alerting/ -v` 全绿，≥3 条用例。
- [ ] `AlertPayload` 字段集合 == §13.3 字段集合（除允许新增 `runbook_url`）。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/alerting/ -v
python -c "from orchestrator.alerting import AlertPayload, dispatch_alert; import logging; logging.basicConfig(level=logging.WARNING); dispatch_alert(AlertPayload(cycle_id='c1', phase='phase0', status='failed', failed_node='x', action='fail_run', summary='demo'))"
```

#### 依赖
依赖 #ISSUE-001（包结构）

---

### ISSUE-010: P1a 端到端集成测试（1 API 占位 + 1 dbt model + 1 check）
**labels**: P0, testing, integration, milestone-0, ready

#### 背景与目标
根据 §18.2 集成测试表首行"1 API + 1 dbt model + 1 个 Phase 0 check"与 §21 阶段 0 退出条件"Dagster UI 中可看到最小依赖图并成功执行"，本 issue 提供**可重复运行的端到端骨架验证**：用 Dagster 的 `materialize` 与 `execute_in_process` API 在单次 pytest 中跑通 Phase 0 最小 cycle，断言 asset 物化成功、asset_check 通过、告警未被错误触发。同时作为阶段 0 与阶段 1 之间的 regression baseline。

#### 所属模块
`tests/integration`（仓库新增目录）

#### 实现范围
- 创建 `tests/integration/__init__.py`、`tests/integration/conftest.py`：定义 `dagster_instance` fixture（`DagsterInstance.ephemeral()`）、`stub_policy_path` fixture（指向 `config/policy/gate_policy.lite.yaml`）、`tmp_dbt_project` fixture（复制 `dbt_stub/` 到 `tmp_path` 并运行 `dbt compile`）。
- 创建 `tests/integration/test_phase0_minimal_cycle.py`：
  - 用例 1 `test_materialize_phase0_readiness_ping`：`materialize([phase0_readiness_ping], resources={"gate_policy": GatePolicyResource(policy_path=stub_policy_path)})`，断言 `result.success is True`。
  - 用例 2 `test_asset_check_passes`：对 `phase0_ping_check` 执行 `execute_asset_check`，断言 `result.passed is True`。
  - 用例 3 `test_definitions_loads_without_errors`：`build_definitions(policy_path=stub_policy_path)` 不抛异常，返回 `Definitions` 实例。
  - 用例 4 `test_daily_cycle_job_executes_in_process`：`daily_cycle_job.execute_in_process(resources={...})`，断言 `result.success` 且至少物化 1 个 asset。
  - 用例 5 `test_no_business_imports`：遍历 `src/orchestrator/` 所有 `.py` 文件，断言没有 `import`/`from` 引用 `graph_engine.` / `main_core.` / `data_platform.` 的内部子模块（只允许公开入口）。
- 在 `Makefile` 增加目标 `integration-test`：`pytest tests/integration -v --tb=short`。
- 在 `docs/` 下新增 `docs/RUNBOOK_P1A.md`，写清"如何本地跑通 P1a 骨架、常见报错、依赖 contracts/data-platform 的替换点"（不超过 100 行）。

#### 不在本次范围
- 不做 Phase 1/2/3 端到端测试（阶段 1-2）。
- 不做失败注入测试（阶段 3）。
- 不接 CI（阶段 3 或由 `assembly` 负责）。
- 不对 dbt 做真实数据断言（只验 compile + 1 model）。

#### 关键交付物
- 5 条集成测试用例，全部可在本机 `pytest tests/integration -v` 通过。
- `dagster_instance` fixture 确保测试隔离（每个用例用独立 ephemeral instance）。
- `test_no_business_imports` 用 `ast.parse` 扫描 `src/orchestrator/`，拒绝任何 `graph_engine.internal.*` / `main_core.internal.*` / `data_platform.internal.*` 形式的 import（正则 `^(graph_engine|main_core|data_platform|audit_eval)\.` 命中即失败）。
- `docs/RUNBOOK_P1A.md` 包含本地启动、验证命令、回滚步骤 3 节。
- `Makefile` `integration-test` 目标在无 Dagster UI 启动的情况下 60 秒内完成。

#### 验收标准
- [ ] `pytest tests/integration -v` 全部通过，用例数 ≥5。
- [ ] `test_no_business_imports` 扫描结果为 0 条违规（当前 P1a 骨架不应触碰下游私有内部）。
- [ ] `daily_cycle_job.execute_in_process(...)` 返回对象 `result.success is True`。
- [ ] `docs/RUNBOOK_P1A.md` 存在且包含"本地启动"、"常见错误"、"替换点"三级标题。
- [ ] `make integration-test` 在 60 秒内结束（本机 Lite 环境）。
- [ ] 集成测试未引入 Kafka / Flink / Temporal 相关 import（`rg "kafka|flink|temporal" tests/integration/ src/orchestrator/` 无命中，除 `temporal/` 占位目录本身的 `__init__.py` 空文件）。
- [ ] 覆盖 §23 验收标准条目 1（不复制业务实现完成装配）与条目 4（无 Kafka/Flink/业务算法）。

#### 验证命令
```bash
cd /Users/fanjie/Desktop/Cowork/project-ult/orchestrator
pytest tests/integration -v --tb=short
rg "^from (graph_engine|main_core|data_platform|audit_eval)\." src/orchestrator/ --type py
rg -i "kafka|flink" src/orchestrator/ tests/integration/ --type py
test -f docs/RUNBOOK_P1A.md && echo "runbook ok"
```

#### 依赖
依赖 #ISSUE-008（build_definitions），#ISSUE-005（AssetCheck），#ISSUE-006（Phase 0 assets）

---

## 阶段 1：P1b-P1c Phase 0 完整化

**目标**：把数据准备、candidate freeze、基础检查完整接入；Phase 0 失败可以按矩阵停止或重跑。
**前置依赖**：阶段 0 全部完成

### ISSUE-011: 接入 data-platform 真实 asset 工厂（替换 StubProvider）
**labels**: P0, integration, milestone-1, ready
**摘要**: 通过 `AssetFactoryProvider` 协议接入 `data-platform` 暴露的 Phase 0 asset / resource 工厂，替换 P1a 的 `StubProvider`；禁止 import 其内部实现。
**依赖**: #ISSUE-010（P1a 骨架稳定）

---

### ISSUE-012: Phase 0 数据到位检查（readiness sensor 真实化）
**labels**: P0, feature, milestone-1, ready
**摘要**: 把 `data_readiness_sensor` 从 SkipReason 替换为查询 `data-platform` readiness signal，满足条件触发 `daily_cycle_job`；失败按矩阵 `fail_run + alert`。
**依赖**: #ISSUE-011

---

### ISSUE-013: candidate freeze asset 与 Phase 0 asset group 扩展
**labels**: P1, feature, milestone-1, ready
**摘要**: 把 candidate freeze（由 `data-platform` 提供工厂）挂入 Phase 0 asset group，补齐 dbt test fail 的失败 asset 级重跑路径。
**依赖**: #ISSUE-011

---

### ISSUE-014: llm_health_check AssetCheck（硬停语义）
**labels**: P0, feature, milestone-1, ready
**摘要**: 接入 `reasoner-runtime` 暴露的 health probe，若失败则 `classify_gate_result` 返回 `FAIL_RUN` 并阻止 Phase 1 触发；覆盖 §12.3 矩阵第 2 行。
**依赖**: #ISSUE-011

---

### ISSUE-015: dbt test fail → partial rerun 路径
**labels**: P0, feature, milestone-1, ready
**摘要**: 当 dbt test 标记失败时，生成仅重跑失败 asset group 的 `PartialRerunPlan`，落地 §12.3 矩阵第 3 行。
**依赖**: #ISSUE-013

---

### ISSUE-016: plan_partial_rerun 纯函数与单元测试
**labels**: P1, algorithm, milestone-1, ready
**摘要**: 实现 §11.3 部分重跑选择算法（纯函数），输入 `failed_node + run history + policy`，输出 `PartialRerunPlan`；覆盖 repair_only / asset_only / phase_only 三种模式。
**依赖**: #ISSUE-003

---

### ISSUE-017: manual rerun 触发入口与 Dagster job 关联
**labels**: P1, feature, milestone-1, ready
**摘要**: 暴露 CLI / sensor 允许人工对单个 cycle 重跑特定 asset group，消费 `PartialRerunPlan`；不做 UI。
**依赖**: #ISSUE-016

---

### ISSUE-018: Phase 0 完整化集成测试与失败矩阵对齐
**labels**: P0, testing, milestone-1, ready
**摘要**: 扩展 `tests/integration/`，覆盖 §12.3 前三行全部失败类型与恢复路径；达到阶段 1 退出条件。
**依赖**: #ISSUE-012, #ISSUE-014, #ISSUE-015

---

## 阶段 2：P2-P3 主链接入

**目标**：Phase 1 图谱链与 Phase 2/3 主系统链接入，四阶段串联最小闭环跑通。
**前置依赖**：阶段 1 全部完成

### ISSUE-019: Phase 1 graph-engine asset 接入
**labels**: P0, integration, milestone-2, ready
**摘要**: 通过 `AssetFactoryProvider` 装配 `graph-engine` 暴露的图谱 promotion / snapshot assets，生成 `phase1` asset group。
**依赖**: #ISSUE-018

---

### ISSUE-020: Phase 1 graph promotion 失败 Gate（保留上一轮 ready graph）
**labels**: P0, feature, milestone-2, ready
**摘要**: 落地 §12.3 矩阵第 4 行：图谱 promotion/snapshot 异常 → `FAIL_RUN`，sensor 不滚动最新 ready graph 指针。
**依赖**: #ISSUE-019

---

### ISSUE-021: Phase 2 main-core 主链 asset 接入
**labels**: P0, integration, milestone-2, ready
**摘要**: 装配 `main-core` 暴露的 L1-L7 主链 assets 与纯检查函数，生成 `phase2` asset group；不在编排层实现任何业务算法。
**依赖**: #ISSUE-020

---

### ISSUE-022: Phase 2 单股票 inconclusive 路径
**labels**: P0, feature, milestone-2, ready
**摘要**: 落地 §12.3 矩阵第 5 行：单股票 LLM 任务失败且未超阈值 → `MARK_INCONCLUSIVE`，继续其他股票；阈值读 policy。
**依赖**: #ISSUE-021

---

### ISSUE-023: Phase 2 整池失败率阈值 Gate
**labels**: P0, feature, milestone-2, ready
**摘要**: 落地 §12.3 矩阵第 6 行：整池失败率超过 `thresholds.phase2_pool_failure_rate` → `FAIL_RUN + alert`。
**依赖**: #ISSUE-022

---

### ISSUE-024: Phase 3 formal commit + manifest 发布
**labels**: P0, integration, milestone-2, ready
**摘要**: 装配 `main-core` formal commit asset 与 `cycle_publish_manifest` 写入 asset；单表 commit 失败 → Phase 3 失败不写 manifest。
**依赖**: #ISSUE-023

---

### ISSUE-025: Phase 3 manifest 补写 repair-only 路径
**labels**: P0, feature, milestone-2, ready
**摘要**: 落地 §12.3 矩阵第 8 行：manifest 写入失败 → 告警 + 暴露 repair-only rerun 入口；不回滚已 commit 的 formal 表。
**依赖**: #ISSUE-024

---

### ISSUE-026: audit-eval asset 接入与 retrospective hook
**labels**: P1, integration, milestone-2, ready
**摘要**: 装配 `audit-eval` 的审计 / 回放 assets，挂接到 Phase 3 完成之后；不在编排层写任何 retrospective 逻辑。
**依赖**: #ISSUE-024

---

### ISSUE-027: 四阶段端到端最小闭环集成测试
**labels**: P0, testing, milestone-2, ready
**摘要**: 从 schedule 触发开始，Phase 0→1→2→3 全链跑通；覆盖 §18.2 第 2 行 "四阶段串联"。
**依赖**: #ISSUE-025, #ISSUE-026

---

## 阶段 3：P5 集成与影子运行

**目标**：冻结完整 Gate 行为矩阵、产出 runbook、支持失败注入测试，P5 影子运行期间不再因失败处理口径反复改边界。
**前置依赖**：阶段 2 全部完成

### ISSUE-028: 完整 Gate 行为矩阵冻结与 schema 校验
**labels**: P0, feature, milestone-3, ready
**摘要**: 补齐 §12.3 全部 9 行失败类型、阈值、alert_channels；加入 schema fuzz 测试确保矩阵覆盖率 100%。
**依赖**: #ISSUE-027

---

### ISSUE-029: 基础设施不可用（PG/Iceberg/Neo4j）硬停路径
**labels**: P0, feature, milestone-3, ready
**摘要**: 落地 §12.3 矩阵末行：infra 不可用 → 任何 phase 立即硬停；通过 resource 初始化异常短路。
**依赖**: #ISSUE-028

---

### ISSUE-030: 多通道告警 dispatcher（Slack / Webhook）
**labels**: P1, feature, milestone-3, ready
**摘要**: 扩展 ISSUE-009 的 `dispatch_alert`，新增 Slack / 通用 Webhook 通道；通道选择来自 policy `alert_channels`。
**依赖**: #ISSUE-028

---

### ISSUE-031: Runbook 生成与 alert payload 关联
**labels**: P1, feature, milestone-3, ready
**摘要**: 在 `alerting.payload` 中自动填入 runbook_url，指向 `docs/RUNBOOK_*.md` 锚点；每个 phase / failure_class 组合一条。
**依赖**: #ISSUE-030

---

### ISSUE-032: 失败注入测试框架与矩阵回归用例
**labels**: P0, testing, milestone-3, ready
**摘要**: 基于 pytest fixture 构造"人为让某个 asset 抛错/dbt test fail/llm_health_check 返回 503"等场景，确保每条矩阵条目都有端到端可重放测试。
**依赖**: #ISSUE-029, #ISSUE-030

---

### ISSUE-033: 影子运行观测与运行诊断脚本
**labels**: P1, feature, milestone-3, ready
**摘要**: 暴露 `orchestrator diag run <cycle_id>` CLI，导出 run summary、GateDecision 列表、rerun plan 建议；供值班人使用。
**依赖**: #ISSUE-032

---

### ISSUE-034: 编排层越界静态检查与反向依赖扫描
**labels**: P0, testing, milestone-3, ready
**摘要**: 增加 AST-based 静态检查（CI 钩子），禁止编排层出现业务算法关键词（如 `pagerank` / `embedding` / `feature_`），并扫描下游模块有无反向 import `orchestrator.*` 内部。
**依赖**: #ISSUE-027

---

## 阶段 4：P5+ Temporal 可选扩展

**目标**：在不改业务模块接口的前提下引入 Phase 1-3 pause/resume；Dagster-only 与 Dagster+Temporal 行为一致。
**前置依赖**：阶段 3 全部完成；仅在业务需要正式 pause/resume 时启动

### ISSUE-035: orchestrator.temporal workflow 骨架
**labels**: P2, feature, milestone-4, ready
**摘要**: 实现 Phase 1-3 包装为 Temporal workflow，不改变 `main-core` / `graph-engine` 对外接口；`execution_backend == dagster_only` 时不加载。
**依赖**: #ISSUE-034

---

### ISSUE-036: execution_backend 切换配置与 handoff
**labels**: P2, feature, milestone-4, ready
**摘要**: 根据 policy `execution_backend` 决定 Phase 0 结束后是否 handoff Temporal；failover 到 Dagster-only 路径。
**依赖**: #ISSUE-035

---

### ISSUE-037: Dagster-only vs Dagster+Temporal parity test
**labels**: P2, testing, milestone-4, ready
**摘要**: 两条路径跑同一 cycle，断言 phase 状态机、GateDecision、最终 manifest 字段完全一致；阶段 4 退出条件。
**依赖**: #ISSUE-036

---
