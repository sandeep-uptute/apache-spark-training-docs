# PySpark quick reference

[← Course syllabus](../README.md) · [Environment setup](README.md) · [Section 01](../01-spark-foundations/README.md)

Print in landscape or keep open beside the notebooks.

## Session and inspection

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").appName("lab").getOrCreate()

spark.version
spark.sparkContext.master
df.printSchema()
df.show(20, truncate=False)
df.explain("formatted")
df.rdd.getNumPartitions()
```

## Read

```python
spark.read.schema(schema).option("header", True).csv(path)
spark.read.schema(schema).option("header", True).option("sep", "\t").csv(path)
spark.read.schema(schema).json(path)
spark.read.parquet(path)
```

## Transform

```python
df.select("a", "b")
df.filter(F.col("status") == "COMPLETED")
df.withColumn("qty", F.expr("try_cast(quantity as int)"))
df.withColumn("line_total", F.col("qty") * F.col("price"))
df.dropDuplicates(["transaction_id"])
df.orderBy(F.desc("line_total"))
df.isEmpty()  # action
```

## Nulls and strings

```python
F.col("x").isNull()
F.col("x").isNotNull()
F.trim("x")
F.upper("status")
F.coalesce("preferred", "fallback")
F.when(condition, value).otherwise(other)
```

## Aggregate

```python
(df.groupBy("category")
 .agg(F.count("*").alias("rows"),
      F.countDistinct("transaction_id").alias("orders"),
      F.sum("line_total").alias("revenue")))
```

## SQL

```python
df.createOrReplaceTempView("sales")
spark.sql("SELECT category, SUM(line_total) revenue FROM sales GROUP BY category")
```

## Join

```python
left.join(right, "key", "inner")
left.join(right, "key", "left")
left.join(right, "key", "left_anti")
left.join(F.broadcast(small_dimension), "key", "left")
```

## Partition and cache

```python
df.repartition(8, "category")
df.coalesce(2)

reused = df.cache()
reused.count()       # materialize
reused.unpersist()
```

## Write and read back

```python
(df.write.mode("overwrite")
 .partitionBy("order_month")
 .parquet(output_path))

check = spark.read.parquet(output_path)
```

## Debug sequence

```text
path → schema → preview → stage counts → reject reasons
→ join key uniqueness → physical plan → destination read-back
```

## Avoid by default

- `collect()` on unknown or large results
- Python UDFs when a built-in expression exists
- caching every DataFrame
- repeated actions only for logging
- `coalesce(1)` as a universal output fix
- silently dropping malformed or unmatched records
- assuming local mode represents cluster performance

## Project commands

```python
import sys
sys.path.insert(0, str(ROOT / "07-guided-project"))
from project.pipeline import run_pipeline, write_combined_report

PROJECT = ROOT / "07-guided-project" / "project"
metrics, report = run_pipeline(spark, "run-001", PROJECT)
all_report = write_combined_report(spark, PROJECT)
```

Expected combined project output: **21 accepted sales · 103,813.00 revenue units**.

