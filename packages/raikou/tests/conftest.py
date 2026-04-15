from __future__ import annotations

import pytest

from raikou import connect


@pytest.fixture(scope="session")
def spark():
    try:
        s = connect(app_name="raikou-tests", master="local[2]")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark is not available: {exc!r}")
    yield s
    s.stop()

