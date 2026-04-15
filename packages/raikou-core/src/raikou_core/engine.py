from __future__ import annotations

from typing import Any, Mapping, Sequence

from pydantable_protocol.exceptions import UnsupportedEngineOperationError
from pydantable_protocol.protocols import EngineCapabilities

from . import expr as sx
from .plan import (
    OpDrop,
    OpDropNulls,
    OpFillNull,
    OpFilter,
    OpRename,
    OpSelect,
    OpSlice,
    OpSort,
    OpUnique,
    OpWithColumns,
    OpWithRowCount,
    SparkPlan,
    field_types_to_descriptors,
)
from .roots import SparkRoot
from .schema import spark_schema_to_descriptors


def _capabilities() -> EngineCapabilities:
    return EngineCapabilities(
        backend="custom",
        extension_loaded=True,
        has_execute_plan=True,
        has_async_execute_plan=False,
        has_async_collect_plan_batches=False,
        has_sink_parquet=False,
        has_sink_csv=False,
        has_sink_ipc=False,
        has_sink_ndjson=False,
        has_collect_plan_batches=False,
        has_execute_join=True,
        has_execute_groupby_agg=True,
    )


def _rows_to_column_dict(rows: list[Any], columns: list[str]) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {c: [] for c in columns}
    for r in rows:
        as_dict = r.asDict(recursive=True)
        for c in columns:
            out[c].append(as_dict.get(c))
    return out


def _ensure_spark_df(data: Any) -> Any:
    if isinstance(data, SparkRoot):
        return data.df
    # Allow passing a raw pyspark DataFrame as root_data.
    return data


class SparkExecutionEngine:
    """
    Spark-backed engine implementing `pydantable-protocol`'s `ExecutionEngine`.

    Plans are represented by `raikou_core.plan.SparkPlan`; expressions are `raikou_core.expr.SparkExpr`
    or raw `pyspark.sql.Column`.
    """

    __slots__ = ()

    @property
    def capabilities(self) -> EngineCapabilities:
        return _capabilities()

    def has_async_execute_plan(self) -> bool:
        return False

    def has_async_collect_plan_batches(self) -> bool:
        return False

    def make_plan(self, field_types: Any) -> Any:
        try:
            desc = field_types_to_descriptors(dict(field_types))
        except Exception:
            desc = {}
        return SparkPlan(field_types=field_types, _schema_descriptors=desc)

    def make_literal(self, *, value: Any) -> Any:
        return sx.Lit(value)

    def plan_with_columns(self, plan: Any, columns: dict[str, Any]) -> Any:
        desc = dict(plan.schema_descriptors())
        for name in columns:
            desc[str(name)] = {"base": "unknown", "nullable": True}
        return plan.add(OpWithColumns(columns=columns)).with_schema_descriptors(desc)

    def expr_is_global_agg(self, expr: Any) -> bool:
        # MVP: treat anything tagged as ("_global_agg", spark_col) as global agg.
        return isinstance(expr, tuple) and len(expr) == 2 and expr[0] == "_global_agg"

    def expr_global_default_alias(self, expr: Any) -> Any:
        if not self.expr_is_global_agg(expr):
            raise TypeError("expr_global_default_alias expects a global-agg expr marker.")
        return "agg"

    def plan_global_select(self, plan: Any, items: list[tuple[str, Any]]) -> Any:
        # Select aggregation expressions; Spark will reduce to one row.
        out_desc = {name: {"base": "unknown", "nullable": True} for name, _ in items}
        return plan.add(("_global_select", tuple(items))).with_schema_descriptors(out_desc)

    def plan_select(self, plan: Any, projects: list[str]) -> Any:
        desc = plan.schema_descriptors()
        out = {k: desc[k] for k in projects if k in desc}
        return plan.add(OpSelect(projects=projects)).with_schema_descriptors(out)

    def plan_filter(self, plan: Any, condition_expr: Any) -> Any:
        return plan.add(OpFilter(condition=condition_expr))

    def plan_sort(
        self,
        plan: Any,
        keys: list[str],
        desc: list[bool],
        nulls_last: list[bool],
        maintain_order: bool,
    ) -> Any:
        return plan.add(
            OpSort(keys=keys, desc=desc, nulls_last=nulls_last, maintain_order=maintain_order)
        )

    def plan_unique(
        self,
        plan: Any,
        subset: list[str] | None,
        keep: str,
        maintain_order: bool,
    ) -> Any:
        return plan.add(OpUnique(subset=subset, keep=keep, maintain_order=maintain_order))

    def plan_duplicate_mask(self, plan: Any, subset: list[str] | None, keep: str) -> Any:
        raise UnsupportedEngineOperationError("duplicate_mask is not implemented for Spark yet.")

    def plan_drop_duplicate_groups(self, plan: Any, subset: list[str] | None) -> Any:
        raise UnsupportedEngineOperationError(
            "drop_duplicate_groups is not implemented for Spark yet."
        )

    def plan_drop(self, plan: Any, columns: list[str]) -> Any:
        desc = dict(plan.schema_descriptors())
        for c in columns:
            desc.pop(c, None)
        return plan.add(OpDrop(columns=columns)).with_schema_descriptors(desc)

    def plan_rename(self, plan: Any, rename_map: Mapping[str, str]) -> Any:
        desc = dict(plan.schema_descriptors())
        for old, new in rename_map.items():
            if old in desc:
                desc[new] = desc.pop(old)
        return plan.add(OpRename(rename_map=rename_map)).with_schema_descriptors(desc)

    def plan_slice(self, plan: Any, offset: int, length: int) -> Any:
        return plan.add(OpSlice(offset=offset, length=length))

    def plan_with_row_count(self, plan: Any, name: str, offset: int) -> Any:
        return plan.add(OpWithRowCount(name=name, offset=offset))

    def plan_fill_null(
        self,
        plan: Any,
        subset: list[str] | None,
        value: Any,
        strategy: str | None,
    ) -> Any:
        return plan.add(OpFillNull(subset=subset, value=value, strategy=strategy))

    def plan_drop_nulls(
        self,
        plan: Any,
        subset: list[str] | None,
        how: str,
        threshold: int | None,
    ) -> Any:
        return plan.add(OpDropNulls(subset=subset, how=how, threshold=threshold))

    def plan_rolling_agg(
        self,
        plan: Any,
        column: str,
        window_size: int,
        min_periods: int,
        op: str,
        out_name: str,
        partition_by: Sequence[str] | None = None,
    ) -> Any:
        raise UnsupportedEngineOperationError("rolling_agg is not implemented for Spark yet.")

    def plan_melt(
        self,
        plan: Any,
        id_vars: Sequence[str],
        value_vars: Sequence[str] | None,
        variable_name: str,
        value_name: str,
    ) -> Any:
        raise UnsupportedEngineOperationError("melt is not implemented for Spark yet.")

    def plan_pivot(
        self,
        plan: Any,
        index: Sequence[str],
        columns: str,
        values: Sequence[str],
        aggregate_function: str,
        *,
        pivot_values: Sequence[Any] | None = None,
        sort_columns: bool = False,
        separator: str = "_",
    ) -> Any:
        raise UnsupportedEngineOperationError("pivot is not implemented for Spark yet.")

    def plan_explode(self, plan: Any, columns: Sequence[str], *, outer: bool = False) -> Any:
        # Implement as execute-time reshape; keep as op for plan.
        return plan.add(("_explode", tuple(columns), bool(outer)))

    def plan_posexplode(
        self,
        plan: Any,
        list_column: str,
        pos_name: str,
        value_name: str,
        *,
        outer: bool = False,
    ) -> Any:
        return plan.add(("_posexplode", list_column, pos_name, value_name, bool(outer)))

    def plan_unnest(self, plan: Any, columns: Sequence[str]) -> Any:
        return plan.add(("_unnest", tuple(columns)))

    def execute_plan(
        self,
        plan: Any,
        data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> Any:
        df = self._apply_plan(_ensure_spark_df(data), plan)
        cols = df.columns
        rows = df.collect()
        return _rows_to_column_dict(rows, cols)

    async def async_execute_plan(self, *a: Any, **k: Any) -> Any:
        raise UnsupportedEngineOperationError("SparkExecutionEngine is sync-only.")

    async def async_collect_plan_batches(self, *a: Any, **k: Any) -> list[Any]:
        raise UnsupportedEngineOperationError(
            "SparkExecutionEngine batch streaming is not implemented."
        )

    def collect_batches(self, *a: Any, **k: Any) -> list[Any]:
        raise UnsupportedEngineOperationError(
            "SparkExecutionEngine batch streaming is not implemented."
        )

    # ---- sinks ----
    def write_parquet(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
        partition_by: list[str] | tuple[str, ...] | None = None,
        mkdir: bool = True,
    ) -> None:
        from pathlib import Path

        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        p = Path(path)
        if mkdir and p.parent and not p.parent.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
        writer = df.write
        if partition_by:
            writer = writer.partitionBy(*list(partition_by))
        opts = write_kwargs or {}
        writer.options(**{str(k): v for k, v in opts.items()}).parquet(str(p))

    def write_csv(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        separator: int = ord(","),
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        opts = {"sep": chr(int(separator))}
        if write_kwargs:
            opts |= {str(k): v for k, v in write_kwargs.items()}
        df.write.options(**opts).csv(path)

    def write_ipc(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        compression: str | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        raise UnsupportedEngineOperationError("Spark does not support Arrow IPC sink here.")

    def write_ndjson(
        self,
        plan: Any,
        root_data: Any,
        path: str,
        *,
        streaming: bool = False,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        # Spark writes JSON as newline-delimited JSON by default.
        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        opts = write_kwargs or {}
        df.write.options(**{str(k): v for k, v in opts.items()}).json(path)

    # ---- join / groupby / set ops (execute-time) ----
    def execute_join(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        left_on: Sequence[str],
        right_on: Sequence[str],
        how: str,
        suffix: str,
        *,
        validate: str | None = None,
        coalesce: bool | None = None,
        join_nulls: bool | None = None,
        maintain_order: str | None = None,
        allow_parallel: bool | None = None,
        force_parallel: bool | None = None,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        ldf0 = self._apply_plan(_ensure_spark_df(left_root_data), left_plan)
        rdf0 = self._apply_plan(_ensure_spark_df(right_root_data), right_plan)
        ldf = ldf0.alias("l")
        rdf = rdf0.alias("r")
        if len(left_on) != len(right_on):
            raise TypeError("join keys must be same length")
        cond = None
        for lk, rk in zip(left_on, right_on):
            c = ldf[lk] == rdf[rk]
            cond = c if cond is None else (cond & c)
        joined0 = ldf.join(rdf, on=cond, how=how)

        lcols = list(ldf0.columns)
        rcols = list(rdf0.columns)
        lset = set(lcols)
        select_cols = [ldf[c].alias(c) for c in lcols]
        for c in rcols:
            out_name = c if c not in lset else f"{c}{suffix}"
            select_cols.append(rdf[c].alias(out_name))
        joined = joined0.select(*select_cols)

        return SparkRoot(joined), spark_schema_to_descriptors(joined.schema)

    def execute_groupby_agg(
        self,
        plan: Any,
        root_data: Any,
        by: Sequence[str],
        aggregations: Any,
        *,
        maintain_order: bool = False,
        drop_nulls: bool = True,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        from pyspark.sql import functions as F

        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        g = df.groupBy(*list(by))
        agg_cols = []
        for out_name, spec in dict(aggregations).items():
            op, col = spec
            op_l = str(op).lower()
            if op_l in ("sum",):
                agg_cols.append(F.sum(col).alias(out_name))
            elif op_l in ("mean", "avg"):
                agg_cols.append(F.avg(col).alias(out_name))
            elif op_l in ("min",):
                agg_cols.append(F.min(col).alias(out_name))
            elif op_l in ("max",):
                agg_cols.append(F.max(col).alias(out_name))
            elif op_l in ("count",):
                agg_cols.append(F.count(col).alias(out_name))
            else:
                raise UnsupportedEngineOperationError(f"Unsupported groupby agg op: {op!r}")
        out = g.agg(*agg_cols)
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_concat(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        how: str,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        ldf = self._apply_plan(_ensure_spark_df(left_root_data), left_plan)
        rdf = self._apply_plan(_ensure_spark_df(right_root_data), right_plan)
        if how not in ("vertical", "diagonal"):
            raise UnsupportedEngineOperationError(f"Unsupported concat how={how!r}")
        out = ldf.unionByName(rdf, allowMissingColumns=(how == "diagonal"))
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_except_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        ldf = self._apply_plan(_ensure_spark_df(left_root_data), left_plan)
        rdf = self._apply_plan(_ensure_spark_df(right_root_data), right_plan)
        out = ldf.exceptAll(rdf)
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_intersect_all(
        self,
        left_plan: Any,
        left_root_data: Any,
        right_plan: Any,
        right_root_data: Any,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        ldf = self._apply_plan(_ensure_spark_df(left_root_data), left_plan)
        rdf = self._apply_plan(_ensure_spark_df(right_root_data), right_plan)
        out = ldf.intersectAll(rdf)
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_melt(self, *a: Any, **k: Any) -> tuple[Any, Any]:
        raise UnsupportedEngineOperationError("melt is not implemented for Spark yet.")

    def execute_pivot(self, *a: Any, **k: Any) -> tuple[Any, Any]:
        raise UnsupportedEngineOperationError("pivot is not implemented for Spark yet.")

    def execute_explode(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]:
        from pyspark.sql import functions as F

        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        out = df
        for c in columns:
            out = out.withColumn(c, (F.explode_outer(out[c]) if outer else F.explode(out[c])))
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_posexplode(
        self,
        plan: Any,
        root_data: Any,
        list_column: str,
        pos_name: str,
        value_name: str,
        *,
        streaming: bool = False,
        outer: bool = False,
    ) -> tuple[Any, Any]:
        from pyspark.sql import functions as F

        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        fn = F.posexplode_outer if outer else F.posexplode
        out = df.select("*", fn(df[list_column]).alias(pos_name, value_name))
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_unnest(
        self,
        plan: Any,
        root_data: Any,
        columns: Sequence[str],
        *,
        streaming: bool = False,
    ) -> tuple[Any, Any]:
        df = self._apply_plan(_ensure_spark_df(root_data), plan)
        out = df
        for c in columns:
            # Expand a struct column into top-level columns with prefix.
            struct_fields = out.schema[c].dataType.fields  # type: ignore[attr-defined]
            for f in struct_fields:
                out = out.withColumn(f"{c}_{f.name}", out[c][f.name])
            out = out.drop(c)
        return SparkRoot(out), spark_schema_to_descriptors(out.schema)

    def execute_rolling_agg(self, *a: Any, **k: Any) -> tuple[Any, Any]:
        raise UnsupportedEngineOperationError("rolling_agg is not implemented for Spark yet.")

    def execute_groupby_dynamic_agg(self, *a: Any, **k: Any) -> tuple[Any, Any]:
        raise UnsupportedEngineOperationError(
            "groupby_dynamic_agg is not implemented for Spark yet."
        )

    # ---- internal plan application ----
    def _apply_plan(self, df: Any, plan: SparkPlan) -> Any:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        cur = df
        last_sort_cols: list[Any] | None = None
        for op in plan.ops:
            if isinstance(op, OpWithColumns):
                for name, ex in op.columns.items():
                    cur = cur.withColumn(name, sx.to_column(ex, cur))
            elif isinstance(op, OpSelect):
                cur = cur.select(*list(op.projects))
            elif isinstance(op, OpFilter):
                cur = cur.filter(sx.to_column(op.condition, cur))
            elif isinstance(op, OpSort):
                sort_cols = []
                for k, d, nl in zip(op.keys, op.desc, op.nulls_last):
                    c = cur[k]
                    if d:
                        c = c.desc_nulls_last() if nl else c.desc_nulls_first()
                    else:
                        c = c.asc_nulls_last() if nl else c.asc_nulls_first()
                    sort_cols.append(c)
                cur = cur.orderBy(*sort_cols)
                last_sort_cols = sort_cols
            elif isinstance(op, OpUnique):
                if op.keep not in ("first", "any"):
                    raise UnsupportedEngineOperationError(
                        f"unique(keep={op.keep!r}) is not implemented for Spark yet."
                    )
                subset = list(op.subset) if op.subset is not None else None
                if subset is None:
                    cur = cur.dropDuplicates()
                else:
                    # Preserve "first" row by current sort order when possible.
                    order_by = (
                        last_sort_cols if last_sort_cols else [F.monotonically_increasing_id()]
                    )
                    w = Window.partitionBy(*subset).orderBy(*order_by)
                    cur = cur.withColumn("__raikou_rn__", F.row_number().over(w))
                    cur = cur.filter(cur["__raikou_rn__"] == 1).drop("__raikou_rn__")
            elif isinstance(op, OpDrop):
                cur = cur.drop(*list(op.columns))
            elif isinstance(op, OpRename):
                for old, new in op.rename_map.items():
                    cur = cur.withColumnRenamed(old, new)
            elif isinstance(op, OpSlice):
                # Spark has no offset-based slice without ordering; apply limit after skipping using zipWithIndex-like window.
                w = Window.orderBy(F.monotonically_increasing_id())
                idx = (F.row_number().over(w) - 1).alias("__row_idx__")
                cur = cur.select("*", idx)
                cur = cur.filter(
                    (cur["__row_idx__"] >= op.offset) & (cur["__row_idx__"] < op.offset + op.length)
                )
                cur = cur.drop("__row_idx__")
            elif isinstance(op, OpWithRowCount):
                w = Window.orderBy(F.monotonically_increasing_id())
                cur = cur.withColumn(op.name, (F.row_number().over(w) - 1 + int(op.offset)))
            elif isinstance(op, OpFillNull):
                if op.strategy is not None:
                    raise UnsupportedEngineOperationError(
                        "fill_null(strategy=...) not implemented for Spark yet."
                    )
                subset = list(op.subset) if op.subset is not None else cur.columns
                cur = cur.fillna(op.value, subset=subset)
            elif isinstance(op, OpDropNulls):
                if op.threshold is not None:
                    # Spark uses 'thresh' count of non-nulls; pydantable uses threshold maybe similar.
                    cur = cur.dropna(
                        thresh=int(op.threshold), subset=list(op.subset) if op.subset else None
                    )
                else:
                    cur = cur.dropna(how=op.how, subset=list(op.subset) if op.subset else None)
            elif isinstance(op, tuple) and op and op[0] == "_explode":
                _tag, cols, outer = op
                for c in cols:
                    cur = cur.withColumn(
                        c, (F.explode_outer(cur[c]) if outer else F.explode(cur[c]))
                    )
            elif isinstance(op, tuple) and op and op[0] == "_global_select":
                _tag, items = op
                sel = []
                for name, e in items:
                    if self.expr_is_global_agg(e):
                        e = e[1]
                    sel.append(sx.to_column(e, cur).alias(name))
                cur = cur.select(*sel)
            elif isinstance(op, tuple) and op and op[0] == "_posexplode":
                _tag, list_col, pos_name, value_name, outer = op
                fn = F.posexplode_outer if outer else F.posexplode
                cur = cur.select("*", fn(cur[list_col]).alias(pos_name, value_name))
            elif isinstance(op, tuple) and op and op[0] == "_unnest":
                _tag, cols = op
                for c in cols:
                    struct_fields = cur.schema[c].dataType.fields  # type: ignore[attr-defined]
                    for f in struct_fields:
                        cur = cur.withColumn(f"{c}_{f.name}", cur[c][f.name])
                    cur = cur.drop(c)
            else:
                raise TypeError(f"Unknown plan op: {op!r}")
        return cur
