"""Dagster orchestration package for project-ult."""

__version__ = "0.1.0"


def __getattr__(name: str) -> object:
    if name == "build_definitions":
        from orchestrator.definitions import build_definitions

        return build_definitions
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["__version__", "build_definitions"]
