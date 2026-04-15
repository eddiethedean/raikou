from __future__ import annotations

import pytest

from raikou_core.expr import BinaryOp


class FakeCol:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:  # pragma: no cover
        return f"FakeCol({self.name})"

    def __eq__(self, other):
        return ("==", self.name, other)

    def __ne__(self, other):
        return ("!=", self.name, other)

    def __lt__(self, other):
        return ("<", self.name, other)

    def __le__(self, other):
        return ("<=", self.name, other)

    def __gt__(self, other):
        return (">", self.name, other)

    def __ge__(self, other):
        return (">=", self.name, other)

    def __add__(self, other):
        return ("+", self.name, other)

    def __sub__(self, other):
        return ("-", self.name, other)

    def __mul__(self, other):
        return ("*", self.name, other)

    def __truediv__(self, other):
        return ("/", self.name, other)

    def __and__(self, other):
        return ("and", self.name, other)

    def __or__(self, other):
        return ("or", self.name, other)


def test_binaryop_covers_supported_ops() -> None:
    df = {"x": FakeCol("x")}
    for op in ("==", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "and", "or", "&&", "||"):
        out = BinaryOp(op, df["x"], 2).to_column(df)
        assert out[0] in ("==", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "and", "or")


def test_binaryop_unsupported_raises() -> None:
    df = {"x": FakeCol("x")}
    with pytest.raises(TypeError):
        BinaryOp("??", df["x"], 1).to_column(df)
