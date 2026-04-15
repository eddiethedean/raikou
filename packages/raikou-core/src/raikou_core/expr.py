from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class SparkExpr:
    """Base type for engine-owned expressions."""

    def to_column(self, df: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Col(SparkExpr):
    name: str

    def to_column(self, df: Any) -> Any:
        return df[self.name]


@dataclass(frozen=True, slots=True)
class Lit(SparkExpr):
    value: Any

    def to_column(self, df: Any) -> Any:
        from pyspark.sql import functions as F

        return F.lit(self.value)


@dataclass(frozen=True, slots=True)
class BinaryOp(SparkExpr):
    op: str
    left: Any
    right: Any

    def to_column(self, df: Any) -> Any:
        left_col = to_column(self.left, df)
        right_col = to_column(self.right, df)
        if self.op == "==":
            return left_col == right_col
        if self.op == "!=":
            return left_col != right_col
        if self.op == "<":
            return left_col < right_col
        if self.op == "<=":
            return left_col <= right_col
        if self.op == ">":
            return left_col > right_col
        if self.op == ">=":
            return left_col >= right_col
        if self.op == "+":
            return left_col + right_col
        if self.op == "-":
            return left_col - right_col
        if self.op == "*":
            return left_col * right_col
        if self.op == "/":
            return left_col / right_col
        if self.op.lower() in ("and", "&&"):
            return left_col & right_col
        if self.op.lower() in ("or", "||"):
            return left_col | right_col
        raise TypeError(f"Unsupported binary op: {self.op!r}")


@dataclass(frozen=True, slots=True)
class Not(SparkExpr):
    inner: Any

    def to_column(self, df: Any) -> Any:
        c = to_column(self.inner, df)
        return ~c


@dataclass(frozen=True, slots=True)
class IsNull(SparkExpr):
    inner: Any

    def to_column(self, df: Any) -> Any:
        return to_column(self.inner, df).isNull()


@dataclass(frozen=True, slots=True)
class IsNotNull(SparkExpr):
    inner: Any

    def to_column(self, df: Any) -> Any:
        return to_column(self.inner, df).isNotNull()


@dataclass(frozen=True, slots=True)
class Cast(SparkExpr):
    inner: Any
    spark_type: str

    def to_column(self, df: Any) -> Any:
        return to_column(self.inner, df).cast(self.spark_type)


@dataclass(frozen=True, slots=True)
class Coalesce(SparkExpr):
    items: tuple[Any, ...]

    def to_column(self, df: Any) -> Any:
        from pyspark.sql import functions as F

        cols = [to_column(i, df) for i in self.items]
        return F.coalesce(*cols)


@dataclass(frozen=True, slots=True)
class Between(SparkExpr):
    inner: Any
    low: Any
    high: Any

    def to_column(self, df: Any) -> Any:
        return to_column(self.inner, df).between(to_column(self.low, df), to_column(self.high, df))


@dataclass(frozen=True, slots=True)
class InList(SparkExpr):
    inner: Any
    values: tuple[Any, ...]

    def to_column(self, df: Any) -> Any:
        c = to_column(self.inner, df)
        raw = [v.value if isinstance(v, Lit) else v for v in self.values]
        return c.isin(raw)


@dataclass(frozen=True, slots=True)
class When(SparkExpr):
    condition: Any
    then_value: Any
    else_value: Any | None = None

    def to_column(self, df: Any) -> Any:
        from pyspark.sql import functions as F

        w = F.when(to_column(self.condition, df), to_column(self.then_value, df))
        if self.else_value is not None:
            w = w.otherwise(to_column(self.else_value, df))
        return w


def to_column(expr: Any, df: Any) -> Any:
    """Normalize engine exprs (or raw Spark Columns) to a Spark Column."""
    if isinstance(expr, SparkExpr):
        return expr.to_column(df)
    # Accept raw pyspark.sql.Column for interop.
    return expr


def parse_op_symbol(op_symbol: str) -> str:
    # Keep this minimal; callers can pass SparkExpr directly if needed.
    return str(op_symbol).strip()


def make_binary(op_symbol: str, left: Any, right: Any) -> SparkExpr:
    return BinaryOp(parse_op_symbol(op_symbol), left, right)


def make_not(inner: Any) -> SparkExpr:
    return Not(inner)


def make_is_null(inner: Any) -> SparkExpr:
    return IsNull(inner)


def make_is_not_null(inner: Any) -> SparkExpr:
    return IsNotNull(inner)


def make_in_list(inner: Any, values: Iterable[Any]) -> SparkExpr:
    return InList(inner=inner, values=tuple(values))


def make_between(inner: Any, low: Any, high: Any) -> SparkExpr:
    return Between(inner=inner, low=low, high=high)


def make_coalesce(*items: Any) -> SparkExpr:
    return Coalesce(tuple(items))


def make_when(condition: Any, then_value: Any, else_value: Any | None = None) -> SparkExpr:
    return When(condition=condition, then_value=then_value, else_value=else_value)
