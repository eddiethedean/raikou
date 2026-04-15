from __future__ import annotations

from typing import Any, Mapping


def spark_schema_to_descriptors(schema: Any) -> dict[str, Mapping[str, Any]]:
    """
    Convert a Spark StructType into pydantable-style dtype descriptors.

    The descriptor format is defined by `pydantable.schema.dtype_descriptor_to_annotation`.
    """

    return {
        f.name: _spark_type_to_descriptor(f.dataType, nullable=f.nullable) for f in schema.fields
    }


def _spark_type_to_descriptor(dt: Any, *, nullable: bool) -> Mapping[str, Any]:
    from pyspark.sql import types as T

    # Struct
    if isinstance(dt, T.StructType):
        return {
            "kind": "struct",
            "nullable": bool(nullable),
            "fields": [
                {
                    "name": f.name,
                    "dtype": _spark_type_to_descriptor(f.dataType, nullable=f.nullable),
                }
                for f in dt.fields
            ],
        }

    # List
    if isinstance(dt, T.ArrayType):
        return {
            "kind": "list",
            "nullable": bool(nullable),
            "inner": _spark_type_to_descriptor(dt.elementType, nullable=bool(dt.containsNull)),
        }

    # Map (pydantable only models dict[str, V]; Spark keys may not be str, but we keep value info)
    if isinstance(dt, T.MapType):
        return {
            "kind": "map",
            "nullable": bool(nullable),
            "value": _spark_type_to_descriptor(dt.valueType, nullable=bool(dt.valueContainsNull)),
        }

    # Scalars
    base: str
    if isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        base = "int"
    elif isinstance(dt, (T.FloatType, T.DoubleType, T.DecimalType)):
        base = "float" if not isinstance(dt, T.DecimalType) else "decimal"
    elif isinstance(dt, T.BooleanType):
        base = "bool"
    elif isinstance(dt, T.StringType):
        base = "str"
    elif isinstance(dt, T.BinaryType):
        base = "binary"
    elif isinstance(dt, T.DateType):
        base = "date"
    elif isinstance(dt, T.TimestampType):
        base = "datetime"
    else:
        base = "unknown"

    return {"base": base, "nullable": bool(nullable)}
