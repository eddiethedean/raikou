from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from raikou_core.plan import SparkPlan, field_types_to_descriptors


def test_plan_is_immutable_on_add_and_schema_override() -> None:
    p0 = SparkPlan(field_types={}, _schema_descriptors={})
    p1 = p0.add(("op", 1))
    assert p0 is not p1
    assert p0.ops == ()
    assert p1.ops == (("op", 1),)

    p2 = p1.with_schema_descriptors({"x": {"base": "int", "nullable": False}})
    assert p2 is not p1
    assert p1.schema_descriptors() == {}
    assert p2.schema_descriptors()["x"]["base"] == "int"


def test_field_types_to_descriptors_scalars_and_nullable_union() -> None:
    desc = field_types_to_descriptors({"x": int, "y": str | None, "z": bytes})
    assert desc["x"] == {"base": "int", "nullable": False}
    assert desc["y"] == {"base": "str", "nullable": True}
    assert desc["z"] == {"base": "binary", "nullable": False}


def test_field_types_to_descriptors_list_and_map() -> None:
    desc = field_types_to_descriptors({"xs": list[int], "m": dict[str, float | None]})
    assert desc["xs"]["kind"] == "list"
    assert desc["xs"]["nullable"] is False
    assert desc["xs"]["inner"] == {"base": "int", "nullable": False}

    assert desc["m"]["kind"] == "map"
    assert desc["m"]["value"]["base"] == "float"
    assert desc["m"]["value"]["nullable"] is True


def test_field_types_to_descriptors_nested_pydantic_model_struct() -> None:
    class Inner(BaseModel):
        a: int
        b: str | None

    out = field_types_to_descriptors({"inner": Inner})
    inner = out["inner"]
    assert inner["kind"] == "struct"
    fields = {f["name"]: f["dtype"] for f in inner["fields"]}
    assert fields["a"] == {"base": "int", "nullable": False}
    assert fields["b"] == {"base": "str", "nullable": True}


def test_field_types_to_descriptors_unknown_fallback() -> None:
    class Weird:
        pass

    out = field_types_to_descriptors({"w": Weird})
    assert out["w"] == {"base": "unknown", "nullable": False}

