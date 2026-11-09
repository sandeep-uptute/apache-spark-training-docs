# Section 03 — DataFrame transformations

[← Previous section](../02-reading-data/README.md) · [Course syllabus](../README.md) · [Open lab](LAB.ipynb) · [Section quiz](QUIZ.md) · [Next section →](../04-aggregations-and-sql/README.md)

## Outcomes

After this section, you can choose columns with `select`, keep rows with `filter`, create columns with `withColumn`, rename expressions with `alias`, and sort results with `orderBy`.

## 1. Select the columns you need

```python
compact = transactions.select("transaction_id", "customer_id", "status")
```

`select` changes columns, not rows. Expressions can be renamed with `alias`:

```python
compact = transactions.select(
    "transaction_id",
    F.upper("status").alias("status_upper")
)
```

## 2. Filter rows

```python
completed = transactions.filter(F.col("status") == "COMPLETED")
```

Combine conditions with `&` for AND, `|` for OR and `~` for NOT. Place each condition inside parentheses.

```python
selected = transactions.filter(
    (F.col("status") == "COMPLETED") &
    (F.col("customer_id").isNotNull())
)
```

## 3. Create or replace columns

```python
typed = transactions.withColumn(
    "qty",
    F.expr("try_cast(quantity as int)")
)
```

`withColumn` returns another DataFrame. It does not modify the earlier DataFrame.

```python
calculated = typed.withColumn("double_qty", F.col("qty") * 2)
```

## 4. Sort the result

```python
ordered = calculated.orderBy(F.desc("double_qty"), F.asc("transaction_id"))
```

Sorting is applied after the required columns and filters have been defined.

## 5. Build a readable transformation chain

```mermaid
flowchart LR
  A[Source columns] --> B[select]
  B --> C[filter]
  C --> D[withColumn]
  D --> E[orderBy]
```

```python
result = (
    transactions
    .select("transaction_id", "quantity", "status")
    .filter(F.col("status") == "COMPLETED")
    .withColumn("qty", F.expr("try_cast(quantity as int)"))
    .orderBy(F.desc("qty"))
)
```

Complete [LAB.ipynb](LAB.ipynb), then answer [QUIZ.md](QUIZ.md).

