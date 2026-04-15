from __future__ import annotations

import pytest

from raikou_core.engine import SparkExecutionEngine


def test_capabilities_smoke() -> None:
    eng = SparkExecutionEngine()
    caps = eng.capabilities
    assert caps.backend == "custom"
    assert caps.has_execute_plan is True
    assert caps.has_async_execute_plan is False


def test_expr_global_default_alias_requires_marker() -> None:
    eng = SparkExecutionEngine()
    with pytest.raises(TypeError, match="expects a global-agg expr marker"):
        eng.expr_global_default_alias(("nope", 1))


def test_expr_is_global_agg_marker_shape() -> None:
    eng = SparkExecutionEngine()
    assert eng.expr_is_global_agg(("_global_agg", object())) is True
    assert eng.expr_is_global_agg(("_global_agg",)) is False
    assert eng.expr_is_global_agg("x") is False

