from __future__ import annotations

from typing import Any, Generic, TypeVar, cast

from raikou_core.engine import SparkExecutionEngine
from raikou_core.roots import SparkRoot
from raikou_core.schema import spark_schema_to_descriptors

from .schema import Schema

SchemaT = TypeVar("SchemaT", bound=Schema)


class RaikouDataFrame(Generic[SchemaT]):
    """Tiny Spark DataFrame-style wrapper backed by raikou-core plans."""

    _schema_type: type[Schema] | None = None

    def __class_getitem__(cls, schema_type: Any) -> type["RaikouDataFrame[Any]"]:
        if not isinstance(schema_type, type) or not issubclass(schema_type, Schema):
            raise TypeError("RaikouDataFrame[Schema] expects a pydantic BaseModel type.")
        name = f"{cls.__name__}[{schema_type.__name__}]"
        return type(name, (cls,), {"_schema_type": schema_type})

    def __init__(self, *, root: SparkRoot, plan: Any, engine: Any) -> None:
        self._root = root
        self._plan = plan
        self._engine = engine

    @classmethod
    def from_spark_dataframe(cls, df: Any, *, engine: Any | None = None) -> "RaikouDataFrame[Any]":
        if cls._schema_type is None:
            raise TypeError("Use RaikouDataFrame[Schema].from_spark_dataframe(df).")
        eng = engine if engine is not None else SparkExecutionEngine()
        # Best-effort initial descriptors from the Spark df schema.
        plan = eng.make_plan({})
        try:
            plan = plan.with_schema_descriptors(spark_schema_to_descriptors(df.schema))
        except Exception:
            pass
        return cls(root=SparkRoot(df), plan=plan, engine=eng)

    def select(self, *cols: str) -> "RaikouDataFrame[Any]":
        plan = self._engine.plan_select(self._plan, list(cols))
        return type(self)(root=self._root, plan=plan, engine=self._engine)

    def filter(self, condition: Any) -> "RaikouDataFrame[Any]":
        plan = self._engine.plan_filter(self._plan, condition)
        return type(self)(root=self._root, plan=plan, engine=self._engine)

    def with_columns(self, **columns: Any) -> "RaikouDataFrame[Any]":
        plan = self._engine.plan_with_columns(self._plan, dict(columns))
        return type(self)(root=self._root, plan=plan, engine=self._engine)

    def to_dict(self) -> dict[str, list[Any]]:
        return cast("dict[str, list[Any]]", self._engine.execute_plan(self._plan, self._root))
