from __future__ import annotations

import pytest

from raikou import RaikouDataFrame, Schema


class Row(Schema):
    x: int


def test_schema_forbids_extra_fields() -> None:
    with pytest.raises(Exception):
        Row(x=1, extra=2)  # type: ignore[call-arg]


def test_dataframe_generic_requires_schema_subclass() -> None:
    with pytest.raises(TypeError, match="expects a pydantic BaseModel type"):
        RaikouDataFrame[int]  # type: ignore[type-arg]


def test_from_spark_dataframe_requires_parameterized_class() -> None:
    with pytest.raises(TypeError, match="Use RaikouDataFrame\\[Schema\\]"):
        RaikouDataFrame.from_spark_dataframe(df=None)  # type: ignore[arg-type]


def test_class_getitem_sets_schema_type() -> None:
    T = RaikouDataFrame[Row]
    assert getattr(T, "_schema_type") is Row


def test_from_spark_dataframe_ignores_descriptor_errors(monkeypatch) -> None:
    # Ensure descriptor extraction failure doesn't fail construction.
    from raikou import dataframe as df_mod

    class DummyDF:
        schema = object()

    def boom(_schema):
        raise RuntimeError("nope")

    monkeypatch.setattr(df_mod, "spark_schema_to_descriptors", boom)
    out = RaikouDataFrame[Row].from_spark_dataframe(DummyDF())
    assert out is not None

