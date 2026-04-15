from __future__ import annotations

from dataclasses import dataclass
import types
from typing import Any, Mapping, Sequence, get_args, get_origin


@dataclass(frozen=True, slots=True)
class SparkPlan:
    field_types: Any
    _schema_descriptors: Mapping[str, Mapping[str, Any]]
    ops: tuple[Any, ...] = ()

    def add(self, op: Any) -> SparkPlan:
        return SparkPlan(
            field_types=self.field_types,
            _schema_descriptors=self._schema_descriptors,
            ops=(*self.ops, op),
        )

    def with_schema_descriptors(self, descriptors: Mapping[str, Mapping[str, Any]]) -> SparkPlan:
        return SparkPlan(
            field_types=self.field_types, _schema_descriptors=descriptors, ops=self.ops
        )

    def schema_descriptors(self) -> Mapping[str, Mapping[str, Any]]:
        return self._schema_descriptors


@dataclass(frozen=True, slots=True)
class OpWithColumns:
    columns: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class OpSelect:
    projects: Sequence[str]


@dataclass(frozen=True, slots=True)
class OpFilter:
    condition: Any


@dataclass(frozen=True, slots=True)
class OpSort:
    keys: Sequence[str]
    desc: Sequence[bool]
    nulls_last: Sequence[bool]
    maintain_order: bool


@dataclass(frozen=True, slots=True)
class OpUnique:
    subset: Sequence[str] | None
    keep: str
    maintain_order: bool


@dataclass(frozen=True, slots=True)
class OpDrop:
    columns: Sequence[str]


@dataclass(frozen=True, slots=True)
class OpRename:
    rename_map: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class OpSlice:
    offset: int
    length: int


@dataclass(frozen=True, slots=True)
class OpWithRowCount:
    name: str
    offset: int


@dataclass(frozen=True, slots=True)
class OpFillNull:
    subset: Sequence[str] | None
    value: Any
    strategy: str | None


@dataclass(frozen=True, slots=True)
class OpDropNulls:
    subset: Sequence[str] | None
    how: str
    threshold: int | None


def field_types_to_descriptors(field_types: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {name: _annotation_to_descriptor(tp) for name, tp in field_types.items()}


def _annotation_to_descriptor(annotation: Any) -> Mapping[str, Any]:
    ann = annotation
    nullable = False
    origin = get_origin(ann)
    if origin is types.UnionType or origin is getattr(__import__("typing"), "Union", object()):
        args = get_args(ann)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(non_none) != len(args):
            ann = non_none[0]
            nullable = True
            origin = get_origin(ann)

    # list[T]
    if origin is list:
        args = get_args(ann)
        inner = args[0] if args else Any
        return {"kind": "list", "nullable": nullable, "inner": _annotation_to_descriptor(inner)}

    # dict[str, V] (best-effort; only value matters for pydantable descriptors)
    if origin is dict:
        args = get_args(ann)
        val = args[1] if len(args) == 2 else Any
        return {"kind": "map", "nullable": nullable, "value": _annotation_to_descriptor(val)}

    # Nested Pydantic models (pydantable Schema types) - detect by `model_fields`.
    if isinstance(ann, type) and hasattr(ann, "model_fields"):
        fields_raw = getattr(ann, "model_fields", {})
        fields = []
        try:
            items = fields_raw.items()
        except Exception:
            items = []
        for name, finfo in items:
            sub_ann = getattr(finfo, "annotation", Any)
            fields.append({"name": str(name), "dtype": _annotation_to_descriptor(sub_ann)})
        return {"kind": "struct", "nullable": nullable, "fields": fields}

    base_map: dict[Any, str] = {
        int: "int",
        float: "float",
        bool: "bool",
        str: "str",
        bytes: "binary",
    }
    base = base_map.get(ann, "unknown")
    return {"base": base, "nullable": nullable}
