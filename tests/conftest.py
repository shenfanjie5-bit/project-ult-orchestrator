import pytest


def _patch_pendulum_compat() -> None:
    try:
        import pendulum
    except ModuleNotFoundError:
        return

    if not hasattr(pendulum, "Pendulum") and hasattr(pendulum, "DateTime"):
        pendulum.Pendulum = pendulum.DateTime  # type: ignore[attr-defined]


_patch_pendulum_compat()


@pytest.fixture
def empty_fixture() -> None:
    return None
