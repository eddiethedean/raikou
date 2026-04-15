from __future__ import annotations

from pathlib import Path

import pytest

from pydantable_protocol.exceptions import UnsupportedEngineOperationError

from raikou_core.engine import SparkExecutionEngine
from raikou_core.roots import SparkRoot
from raikou_core.schema import spark_schema_to_descriptors


def test_plan_schema_descriptors_tracks_select_drop_rename(eng: SparkExecutionEngine) -> None:
    plan = eng.make_plan({"x": int, "y": str | None})
    assert set(plan.schema_descriptors()) == {"x", "y"}
    plan2 = eng.plan_rename(plan, {"y": "yy"})
    assert set(plan2.schema_descriptors()) == {"x", "yy"}
    plan3 = eng.plan_drop(plan2, ["x"])
    assert set(plan3.schema_descriptors()) == {"yy"}
    plan4 = eng.plan_select(plan2, ["x"])
    assert set(plan4.schema_descriptors()) == {"x"}


@pytest.mark.spark
def test_schema_to_descriptors_smoke(spark) -> None:
    df = spark.createDataFrame([{"x": 1, "y": "a"}])
    desc = spark_schema_to_descriptors(df.schema)
    assert set(desc) == {"x", "y"}
    assert desc["x"]["base"] in ("int", "unknown")


@pytest.mark.spark
def test_execute_concat_except_intersect_all(spark, eng: SparkExecutionEngine) -> None:
    left_df = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 2}])
    right_df = spark.createDataFrame([{"x": 2}])
    lp = eng.make_plan({})
    rp = eng.make_plan({})

    root, _ = eng.execute_concat(lp, SparkRoot(left_df), rp, SparkRoot(right_df), how="vertical")
    out = eng.execute_plan(eng.make_plan({}), root)
    assert out["x"].count(2) == 3

    root2, _ = eng.execute_except_all(lp, SparkRoot(left_df), rp, SparkRoot(right_df))
    out2 = eng.execute_plan(eng.make_plan({}), root2)
    assert out2["x"] == [1, 2]

    root3, _ = eng.execute_intersect_all(lp, SparkRoot(left_df), rp, SparkRoot(right_df))
    out3 = eng.execute_plan(eng.make_plan({}), root3)
    assert out3["x"] == [2]


@pytest.mark.spark
def test_posexplode(spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"id": 1, "tags": [5, 6]}])
    plan = eng.make_plan({})
    root, _ = eng.execute_posexplode(
        plan, SparkRoot(df), list_column="tags", pos_name="pos", value_name="val"
    )
    out = eng.execute_plan(eng.make_plan({}), root)
    assert out["pos"] == [0, 1]
    assert out["val"] == [5, 6]


@pytest.mark.spark
def test_sinks_parquet_csv_json(tmp_path: Path, spark, eng: SparkExecutionEngine) -> None:
    df = spark.createDataFrame([{"x": 1, "y": "a"}])
    root = SparkRoot(df)
    plan = eng.make_plan({})

    pq = tmp_path / "out.parquet"
    eng.write_parquet(plan, root, str(pq), mkdir=True)
    assert pq.exists()

    csv_dir = tmp_path / "csv_out"
    eng.write_csv(plan, root, str(csv_dir))
    assert csv_dir.exists()

    js_dir = tmp_path / "json_out"
    eng.write_ndjson(plan, root, str(js_dir))
    assert js_dir.exists()


def test_unimplemented_paths_raise(eng: SparkExecutionEngine) -> None:
    plan = eng.make_plan({})
    with pytest.raises(UnsupportedEngineOperationError):
        eng.plan_duplicate_mask(plan, subset=None, keep="first")
    with pytest.raises(UnsupportedEngineOperationError):
        eng.plan_drop_duplicate_groups(plan, subset=None)
    with pytest.raises(UnsupportedEngineOperationError):
        eng.plan_rolling_agg(plan, column="x", window_size=3, min_periods=1, op="sum", out_name="o")
    with pytest.raises(UnsupportedEngineOperationError):
        eng.plan_melt(plan, id_vars=["x"], value_vars=["y"], variable_name="v", value_name="val")
    with pytest.raises(UnsupportedEngineOperationError):
        eng.plan_pivot(plan, index=["x"], columns="c", values=["v"], aggregate_function="sum")
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_melt()
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_pivot()
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_rolling_agg()
    with pytest.raises(UnsupportedEngineOperationError):
        eng.execute_groupby_dynamic_agg()
    with pytest.raises(UnsupportedEngineOperationError):
        eng.write_ipc(plan, root_data=None, path="x")
