# 04. Aggregations and Spark SQL

**Duration:** 60 minutes  
**Notebook:** `04_aggregations_and_sql.ipynb`  
**Outcome:** Group transactions, compute summaries, register temporary views, and query with Spark SQL.

---

## Business question

The retailer wants category-level sales and daily completed-sales revenue. Only transactions that satisfy all of these rules count toward reported revenue:

1. The transaction ID is not a duplicate.
2. Quantity/date/price/customer/product key passes base quality checks.
3. `status = COMPLETED`.
4. Both customer and product IDs match the reference tables.

We remove bad transactions *before* aggregate reporting. The transformation module produced 50 base-quality records. Two of those are completed but have unmatched reference IDs; after completed-status filtering and matching references, **36** records qualify for the business report.

## Group by status

```python
clean.groupBy('status').count().orderBy('status').show()
```

This counts records in each status category. It does **not** tell us revenue until we apply the metric definition.

## Join to enrich the fact table

```python
from training_lib import completed_enriched
enriched = completed_enriched(clean, customers, products)
```

The join details are explained in the next module. Here we use the result to focus on aggregates.

## Group by and summary metrics

```python
from pyspark.sql import functions as F
by_category = (enriched.groupBy('category')
    .agg(
        F.countDistinct('transaction_id').alias('orders'),
        F.sum('line_total').alias('revenue')
    )
    .orderBy('category'))
by_category.show(truncate=False)
```

| Column | Meaning | Why it matters |
|---|---|---|
| `category` | Product grouping | Business drilldown |
| `orders` | Distinct transaction IDs | Prevent duplicate order inflation |
| `revenue` | Sum of quantity × unit price | Completed-sales value |

**Important:** Our synthetic data has one row per transaction ID. A real order header can contain many order lines, so in that case `COUNT(DISTINCT order_id)` is different from `COUNT(*)`.

## Expected result

| Category | Orders | Revenue |
|---|---:|---:|
| Accessories | 16 | 52574.00 |
| Audio | 4 | 20992.00 |
| Electronics | 12 | 265968.00 |
| Stationery | 4 | 2694.00 |
| **Total** | **36** | **342228.00** |

These are expected figures for the supplied dataset. Currency is illustrative; the goal is consistent transformation, not financial accounting or tax treatment.

## The same logic using Spark SQL

```python
enriched.createOrReplaceTempView('sales_valid')
result = spark.sql('''
    SELECT category,
           COUNT(DISTINCT transaction_id) AS orders,
           SUM(line_total) AS revenue
    FROM sales_valid
    GROUP BY category
    ORDER BY category
''')
result.show()
```

A temporary view registers a DataFrame query in the current SparkSession. It is not automatically saved as a durable table or file. A view's name can be reused with `createOrReplaceTempView`.

## SQL vs DataFrame API

| Topic | DataFrame API | Spark SQL |
|---|---|---|
| Select | `.select('category')` | `SELECT category` |
| Filter | `.filter(F.col('status') == 'COMPLETED')` | `WHERE status = 'COMPLETED'` |
| Aggregate | `.groupBy('category').agg(F.sum('line_total'))` | `GROUP BY category` / `SUM(line_total)` |
| Join | `.join(other, 'product_id')` | `JOIN ... ON ...` |
| Execution | Spark optimizer plans query | Same Spark optimizer plans query |

Avoid claiming SQL is intrinsically faster than equivalent DataFrame operations. Their logical plans often optimize similarly.

## Date-level analytics

```python
(enriched.groupBy('order_date')
 .agg(F.count('*').alias('sales_rows'), F.sum('line_total').alias('daily_revenue'))
 .orderBy('order_date')
 .show())
```

This provides a basic daily revenue trend. The actual status and valid-reference rules continue to apply; do not replace `enriched` with raw CSV by accident.

## Practice lab (20 minutes)

1. Create completed-sales revenue by city and sort city names.
2. Write a Spark SQL query for category revenue.
3. Compute the daily number of qualifying sales.
4. Compare `COUNT(*)` with `COUNT(DISTINCT transaction_id)` on the original raw dataset.
5. Verify total qualifying completed revenue is 342228.00.

## Questions to check understanding

**Is `groupBy` a narrow transformation?** Normally not: grouping by a key commonly redistributes records by that key and causes a shuffle.  
**Does `show()` return the full DataFrame to Python?** No; it displays a small sample.  
**Why choose DECIMAL for price?** Binary floating point can represent many decimal amounts approximately, which can distort money calculations.  
**How long does a temporary view live?** It is scoped to the active SparkSession, not permanent storage.

**Checkpoint:** Both DataFrame and SQL produce the same four category totals.
