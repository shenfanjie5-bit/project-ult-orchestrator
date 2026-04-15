# orchestrator

This module is scaffold-only.

Source of truth:

- `docs/orchestrator.project-doc.md`

Current workspace state:

- `docs/` keeps the source project doc
- `pyproject.toml` is placeholder project metadata
- implementation directories are created only when real work starts

Execution rule:

1. read the project doc first
2. keep work inside this module unless the issue explicitly targets shared contracts
3. do not treat this scaffold as finished implementation

## 本地运行 Dagster UI

如尚未生成 dbt manifest，先运行：

```bash
make dbt-compile
```

启动本地 Dagster UI：

```bash
DAGSTER_HOME=./dagster_home dagster dev -m orchestrator.definitions
```
