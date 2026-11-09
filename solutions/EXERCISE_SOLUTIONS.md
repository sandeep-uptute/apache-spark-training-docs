# Topic exercise solutions

These are reference approaches, not the only valid implementations. The snippets assume `ROOT`, `spark`, `F` and the workshop helper functions are available as shown in the numbered notebooks.

## Solution 1 — Execution model

```python
from pyspark.sql import functions as F

numbers = spark.range(0, 20, 1, 4)
answer = numbers.withColumn("square", F.col("id") * F.col("id")).filter(F.col("id") % 2 == 0)
print(answer.rdd.getNumPartitions())  # 4 before later plan adjustments
answer.show(5)
assert answer.count() == 10
```

`withColumn` and `filter` are transformations. `show` and `count` are actions and can trigger separate jobs.

## Solution 2 — File formats

```python
transactions = spark.read.option("header", True).csv(str(ROOT / "data" / "transactions.csv"))
products_json = spark.read.json(str(ROOT / "data" / "products.json"))
products_tsv = (spark.read.option("header", True).option("sep", "\t")
                .csv(str(ROOT / "data" / "products.tsv")))
customers = spark.read.option("header", True).csv(str(ROOT / "data" / "customers.csv"))

assert products_json.count() == products_tsv.count() == 12
customers.select("customer_id", "city").show()

path = ROOT / "output" / "ex02_products_parquet"
products_json.write.mode("overwrite").parquet(str(path))
assert spark.read.parquet(str(path)).count() == 12
```

Spark writes a dataset composed of partition files so tasks can write in parallel.

## Solution 3 — Cleaning

```python
raw, _, _ = read_raw(spark, ROOT)
typed = typed_transactions(raw)

reason = F.concat_ws(
    "; ",
    F.when(F.col("order_date").isNull(), "invalid date"),
    F.when(F.col("qty").isNull() | (F.col("qty") <= 0), "invalid quantity"),
    F.when(F.col("price").isNull() | (F.col("price") < 0), "invalid price"),
    F.when(F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0), "missing customer"),
    F.when(F.col("product_id").isNull() | (F.length(F.trim("product_id")) == 0), "missing product"),
)
classified = typed.withColumn("reject_reason", reason)
valid = (classified.filter("reject_reason = ''")
         .withColumn("line_total", F.col("qty") * F.col("price")))
invalid = classified.filter("reject_reason <> ''")

assert raw.count() == 58
assert typed.count() == 57
assert valid.count() == 50
assert invalid.count() == 7
invalid.select("transaction_id", "reject_reason").show(truncate=False)
```

## Solution 4 — Aggregation and SQL

```python
_, _, products = read_raw(spark, ROOT)
sales = (valid.filter(F.col("status") == "COMPLETED")
         .join(products, "product_id", "inner"))

df_result = (sales.groupBy("category")
             .agg(F.countDistinct("transaction_id").alias("orders"),
                  F.sum("qty").alias("units"),
                  F.sum("line_total").alias("revenue"))
             .orderBy("category"))

sales.createOrReplaceTempView("sales")
sql_result = spark.sql("""
SELECT category, COUNT(DISTINCT transaction_id) orders,
       SUM(qty) units, SUM(line_total) revenue
FROM sales GROUP BY category ORDER BY category
""")

assert df_result.collect() == sql_result.collect()
assert str(sales.agg(F.sum("line_total")).first()[0]) == "342228.00"
```

## Solution 5 — Joins and output

```python
raw, customers, products = read_raw(spark, ROOT)
clean = clean_transactions(raw)
completed = clean.filter(F.col("status") == "COMPLETED")

unknown_products = completed.join(products, "product_id", "left_anti")
unknown_customers = completed.join(customers, "customer_id", "left_anti")
unknown_products.select("product_id").distinct().show()   # P999
unknown_customers.select("customer_id").distinct().show() # C999

assert products.groupBy("product_id").count().filter("count > 1").count() == 0
assert customers.groupBy("customer_id").count().filter("count > 1").count() == 0

enriched = completed.join(customers, "customer_id").join(products, "product_id")
assert enriched.count() == 36
path = ROOT / "output" / "ex05_enriched"
enriched.write.mode("overwrite").parquet(str(path))
assert spark.read.parquet(str(path)).count() == 36
```

## Solution 6 — Plans and optimization

```python
summary = enriched.groupBy("category").agg(F.sum("line_total").alias("revenue"))
summary.explain("formatted")  # aggregation usually includes Exchange

joined = clean.join(F.broadcast(products), "product_id", "left")
joined.explain("formatted")   # look for BroadcastHashJoin

reused = clean.cache()
reused.count()                # materialize
reused.groupBy("status").count().show()
reused.groupBy("customer_id").count().show()
reused.unpersist()
```

The tiny dataset cannot demonstrate realistic disk, network, shuffle, JVM warm-up or cluster scheduling behavior. `coalesce(1)` is useful only for exceptional small-output requirements; it is not a general optimization.

## Solution 7 — Project observations

1. Strings preserve malformed landing values so the pipeline can classify them.
2. Deduplication removes the extra copy; the audit records the count without calling a valid copy a rejected business record.
3. They are valid business events but outside the completed-sales metric.
4. Two rows fail quality rules, two completed rows fail reference matching, two valid rows are non-completed, and one row is an identical duplicate.
5. `QUALITY` and `REFERENCE`.
6. Only run-002 paths under curated sales, rejected records and audit are replaced; its report can be regenerated.
7. It verifies the published destination rather than an in-memory intermediate.
8. Atomic multi-file commit, concurrent writer isolation, general updates/deletes, durable transaction history and other ACID guarantees.

