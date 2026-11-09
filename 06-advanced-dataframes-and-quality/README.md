# Section 06 — Set operations, pivot and data quality

[← Previous section](../05-joins-and-parquet/README.md) · [Course syllabus](../README.md) · [Open lab](LAB.ipynb) · [Section quiz](QUIZ.md) · [Project →](../07-guided-project/README.md)

## Outcomes

After this section, you can combine compatible DataFrames, compare row sets, pivot an aggregation, classify invalid records, inspect execution plans and calculate ranking and analytical window measures.

## DataFrame API and literal SQL

Every activity includes a paired SQL route. The queries cover set operators, `PIVOT`, quality-rule expressions, plan inspection and window functions such as `ROW_NUMBER`, `RANK`, `DENSE_RANK`, running `SUM` and `LAG`.

## 1. Append rows with union

```python
combined = january.union(february)
```

`union` matches columns by position. `unionByName` matches them by name and is safer when column order differs.

```python
combined = january.unionByName(february)
```

Neither operation removes duplicates automatically. Add `distinct()` only when duplicate removal matches the business rule.

## 2. Compare row sets

```python
common = first.intersect(second)
only_first = first.exceptAll(second)
```

- `intersect` returns rows present in both DataFrames.
- `exceptAll` subtracts matching rows while preserving duplicate counts.

## 3. Pivot grouped values into columns

```python
matrix = (
    sales.groupBy("city")
    .pivot("category", ["Accessories", "Audio", "Electronics"])
    .agg(F.sum("revenue"))
)
```

Pivot is an aggregation. Grouping was introduced in Section 04, so the only new idea is converting selected group values into columns.

## 4. Apply data-quality rules

```python
typed = raw.withColumn("qty", F.expr("try_cast(quantity as int)"))

classified = typed.withColumn(
    "reject_reason",
    F.when(F.col("qty").isNull(), "invalid quantity")
     .when(F.col("qty") <= 0, "non-positive quantity")
     .otherwise("")
)
```

Valid and rejected rows remain separate and reconcilable. A rejected row keeps its reason.

## 5. Inspect the plan before optimizing

```python
result.explain("formatted")
```

Look for scans, filters, exchanges, aggregates and join strategies. Prefer built-in expressions, broadcast genuinely small references, cache only reused work and avoid forcing every output through one partition.

## 6. Rank rows with a window

A window calculation keeps the original rows while adding a value calculated from related rows.

```python
from pyspark.sql.window import Window

ranking_window = (
    Window.partitionBy("department")
    .orderBy(F.desc("salary"))
)

ranked = (
    employees
    .withColumn("row_number", F.row_number().over(ranking_window))
    .withColumn("rank", F.rank().over(ranking_window))
    .withColumn("dense_rank", F.dense_rank().over(ranking_window))
)
```

- `row_number` assigns a unique sequence.
- `rank` gives tied rows the same position and leaves a gap afterward.
- `dense_rank` gives tied rows the same position without leaving a gap.

## 7. Add analytical window measures

```python
department_window = Window.partitionBy("department")

running_window = (
    Window.partitionBy("department")
    .orderBy("sale_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

analysed = (
    sales
    .withColumn("department_total", F.sum("revenue").over(department_window))
    .withColumn("department_average", F.avg("revenue").over(department_window))
    .withColumn("running_revenue", F.sum("revenue").over(running_window))
)
```

Unlike `groupBy`, window aggregates do not collapse the detail rows. `lag` and `lead` can access earlier and later rows in an ordered window.

Complete [LAB.ipynb](LAB.ipynb), then answer [QUIZ.md](QUIZ.md).

