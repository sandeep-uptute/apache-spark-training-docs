# Section 04 — Aggregations and Spark SQL

[← Previous section](../03-dataframe-transformations/README.md) · [Course syllabus](../README.md) · [Open lab](LAB.ipynb) · [Section quiz](QUIZ.md) · [Next section →](../05-joins-and-parquet/README.md)

## Outcomes

After this section, you can group rows, calculate several measures, group by more than one column, sort summaries and express the same aggregation with Spark SQL.

## DataFrame API and literal SQL

Each aggregation is implemented twice: once with `groupBy().agg()` and once with a literal `GROUP BY` query over a temporary view. Compare the result schema, values and execution plan rather than treating the two routes as separate exercises.

## 1. Count rows in each group

```python
status_counts = transactions.groupBy("status").count()
```

The input contains transaction rows. The output contains one row for each status.

## 2. Calculate several measures

```python
summary = sales.groupBy("category").agg(
    F.count("*").alias("lines"),
    F.sum("quantity").alias("units"),
    F.avg("unit_price").alias("average_price"),
    F.min("unit_price").alias("minimum_price"),
    F.max("unit_price").alias("maximum_price")
)
```

Each expression describes one output measure at the category level.

## 3. Group by more than one column

```python
daily_category = sales.groupBy("order_date", "category").agg(
    F.sum("revenue").alias("revenue")
)
```

The result now contains one row for each date-and-category combination.

## 4. Sort a summary

```python
ranked = summary.orderBy(F.desc("revenue"), F.asc("category"))
```

`orderBy` is already familiar from Section 03. Here it is applied to aggregated rows.

## 5. Express the same logic with SQL

```python
sales.createOrReplaceTempView("sales")

result = spark.sql("""
SELECT category,
       COUNT(*) AS lines,
       SUM(quantity) AS units,
       SUM(revenue) AS revenue
FROM sales
GROUP BY category
ORDER BY revenue DESC
""")
```

The DataFrame API and Spark SQL both create logical plans for Spark to optimize.

Complete [LAB.ipynb](LAB.ipynb), then answer [QUIZ.md](QUIZ.md).

