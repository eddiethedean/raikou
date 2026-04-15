# Getting started

## Install

```bash
pip install raikou
```

## Quick start

```python
from raikou import RaikouDataFrame, Schema, col, connect


class Row(Schema):
    x: int
    y: str


spark = connect(master="local[2]")
sdf = spark.createDataFrame([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])

df = RaikouDataFrame[Row].from_spark_dataframe(sdf)
out = df.filter(col("x") > 1).select("y").to_dict()
print(out)
```

