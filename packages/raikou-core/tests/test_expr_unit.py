from __future__ import annotations

import pytest

from raikou_core.expr import (
    BinaryOp,
    Col,
    InList,
    Lit,
    When,
    make_binary,
    make_in_list,
    make_when,
    parse_op_symbol,
    to_column,
)


def test_parse_op_symbol_strips() -> None:
    assert parse_op_symbol("  + ") == "+"


def test_make_binary_uses_parsed_symbol() -> None:
    ex = make_binary("  * ", 1, 2)
    assert isinstance(ex, BinaryOp)
    assert ex.op == "*"


def test_binaryop_unsupported_operator_raises() -> None:
    ex = BinaryOp("^", 1, 2)
    with pytest.raises(TypeError, match="Unsupported binary op"):
        ex.to_column(df=None)


def test_in_list_normalizes_lit_values() -> None:
    ex = InList(inner=Col("x"), values=(Lit(1), 2, Lit("a")))

    class FakeCol:
        def __init__(self):
            self.seen = None

        def isin(self, raw):
            self.seen = list(raw)
            return ("isin", tuple(raw))

    class FakeDF(dict):
        pass

    df = FakeDF(x=FakeCol())
    out = ex.to_column(df)
    assert out[0] == "isin"
    assert df["x"].seen == [1, 2, "a"]


@pytest.mark.spark
def test_when_without_else_builds_when_only(spark) -> None:
    # Minimal Spark integration with expression builder (reuse shared session).
    from pyspark.sql import functions as F

    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    expr = When(condition=(F.col("x") > 1), then_value=Lit("yes"))
    col = expr.to_column(df)
    out = [r[0] for r in df.select(col.alias("v")).orderBy("x").collect()]
    assert out == [None, "yes"]


def test_to_column_passthrough_non_sparkexpr() -> None:
    obj = object()
    assert to_column(obj, df=None) is obj


def test_make_when_smoke() -> None:
    w = make_when(condition=True, then_value=1, else_value=2)
    assert isinstance(w, When)


def test_make_in_list_smoke() -> None:
    ex = make_in_list("x", [1, 2, 3])
    assert isinstance(ex, InList)
