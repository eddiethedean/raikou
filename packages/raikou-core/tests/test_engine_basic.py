from __future__ import annotations

import pytest

from raikou_core.engine import SparkExecutionEngine
from raikou_core.expr import BinaryOp, Col, Lit
from raikou_core.roots import SparkRoot


@pytest.mark.spark
def test_select_filter_with_columns_collect(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
    plan = eng.make_plan(field_types={})
    plan = eng.plan_with_columns(plan, {"z": BinaryOp("+", Col("x"), Lit(10))})
    plan = eng.plan_filter(plan, BinaryOp(">", Col("z"), Lit(11)))
    plan = eng.plan_select(plan, ["y", "z"])

    out = eng.execute_plan(plan, SparkRoot(df))
    assert out == {"y": ["b"], "z": [12]}


@pytest.mark.spark
def test_sort_unique_slice(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 2}, {"x": 1}, {"x": 1}])
    plan = eng.make_plan(field_types={})
    plan = eng.plan_sort(plan, keys=["x"], desc=[False], nulls_last=[True], maintain_order=False)
    plan = eng.plan_unique(plan, subset=["x"], keep="first", maintain_order=False)
    plan = eng.plan_slice(plan, offset=0, length=2)

    out = eng.execute_plan(plan, SparkRoot(df))
    assert out["x"] == [1, 2]


@pytest.mark.spark
def test_unnest_struct(spark, eng: SparkExecutionEngine) -> None:
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField(
                "addr",
                T.StructType([T.StructField("street", T.StringType(), nullable=True)]),
                nullable=True,
            ),
        ]
    )
    df = spark.createDataFrame([(1, ("x",))], schema=schema)
    plan = eng.make_plan(field_types={})
    root, descriptors = eng.execute_unnest(plan, SparkRoot(df), columns=["addr"])
    out = eng.execute_plan(eng.make_plan({}), root)
    assert out["id"] == [1]
    assert out["addr_street"] == ["x"]
    assert "addr" not in out
    assert "addr_street" in descriptors


@pytest.mark.spark
def test_explode_list(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"id": 1, "tags": [1, 2]}, {"id": 2, "tags": [3]}])
    plan = eng.make_plan(field_types={})
    root, _ = eng.execute_explode(plan, SparkRoot(df), columns=["tags"])
    out = eng.execute_plan(eng.make_plan({}), root)
    assert out["id"] == [1, 1, 2]
    assert out["tags"] == [1, 2, 3]


@pytest.mark.spark
def test_join_and_groupby(spark, eng: SparkExecutionEngine) -> None:
    left = spark.createDataFrame([{"k": 1, "v": 10}, {"k": 2, "v": 20}])
    right = spark.createDataFrame([{"k": 1, "w": 7}, {"k": 1, "w": 8}])

    lplan = eng.make_plan(field_types={})
    rplan = eng.make_plan(field_types={})
    joined_root, _ = eng.execute_join(
        lplan,
        SparkRoot(left),
        rplan,
        SparkRoot(right),
        left_on=["k"],
        right_on=["k"],
        how="inner",
        suffix="_right",
    )
    joined = joined_root.df
    assert set(joined.columns) == {"k", "v", "k_right", "w"}

    # group by k and sum w
    plan = eng.make_plan(field_types={})
    root, _ = eng.execute_groupby_agg(
        plan,
        joined_root,
        by=["k"],
        aggregations={"w_sum": ("sum", "w")},
    )
    out = eng.execute_plan(eng.make_plan({}), root)
    assert out["k"] == [1]
    assert out["w_sum"] == [15]
