from __future__ import annotations

import pytest

from raikou_core.expr import (
    Between,
    Cast,
    Coalesce,
    Col,
    IsNotNull,
    IsNull,
    Lit,
    Not,
    When,
)


@pytest.mark.spark
def test_expr_nodes_end_to_end(spark) -> None:
    df = spark.createDataFrame([(None, 1), (2, None)], schema="a long, b long")

    # Coalesce + Cast
    c = Coalesce((Col("a"), Col("b"), Lit(0))).to_column(df).alias("c")
    s = Cast(c, "string").to_column(df).alias("s")

    # Between + null checks + not
    cond_between = Between(Col("b"), Lit(1), Lit(1)).to_column(df)
    cond_null = IsNull(Col("a")).to_column(df)
    cond_not_null = IsNotNull(Col("b")).to_column(df)
    cond = (cond_between & cond_not_null) | Not(cond_null).to_column(df)

    # When with else branch
    w = When(condition=cond, then_value=Lit("y"), else_value=Lit("n")).to_column(df).alias("w")

    rows = df.select(c, s, w).orderBy("a").collect()
    assert len(rows) == 2
