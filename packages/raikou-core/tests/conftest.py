from __future__ import annotations

import pytest

from raikou_core.engine import SparkExecutionEngine
from raikou_core.session import get_or_create_spark


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for integration tests."""
    s = get_or_create_spark(app_name="raikou-core-tests")
    yield s
    s.stop()


@pytest.fixture()
def eng() -> SparkExecutionEngine:
    return SparkExecutionEngine()

