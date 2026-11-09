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

Temporary views exist only in the current Spark session unless a global temporary view or catalog table is created explicitly.

## Join

```python
left.join(right, "key", "inner")
left.join(right, "key", "left")
left.join(right, "key", "right")
left.join(right, "key", "full")
left.join(right, "key", "left_semi")
left.join(right, "key", "left_anti")
left.join(F.broadcast(small_dimension), "key", "left")
```

## Set, pivot and window operations

```python
first.unionByName(second, allowMissingColumns=True)
first.intersect(second)
first.exceptAll(second)

df.groupBy("city").pivot("category").sum("revenue")

from pyspark.sql.window import Window
rank_window = Window.partitionBy("department").orderBy(F.desc("salary"))
df.withColumn("row_number", F.row_number().over(rank_window))
df.withColumn("rank", F.rank().over(rank_window))
df.withColumn("dense_rank", F.dense_rank().over(rank_window))
```

## Partition, persistence and AQE

```python
df.rdd.getNumPartitions()
df.repartition(8, "category")
df.coalesce(2)

reused = df.cache()
reused.count()       # materialize
reused.unpersist()

from pyspark import StorageLevel
reused = df.persist(StorageLevel.MEMORY_AND_DISK)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

df.explain("formatted")
```

Use `repartition` to redistribute; use `coalesce` mainly to reduce partitions. Cache only an expensive result that will be reused, materialize it with an action, then unpersist it.

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

## Project outputs

```text
08-guided-project/project/destination/curated_sales/
08-guided-project/project/destination/rejected_records/
08-guided-project/project/destination/audit/
08-guided-project/project/reports/
```

Expected combined project output: **21 accepted sales · 103,813.00 revenue units**.

