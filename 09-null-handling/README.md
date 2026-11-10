# Section 09 — Handling null and missing values

[← Previous section](../08-guided-project/README.md) · [Course syllabus](../README.md) · [Open lab](LAB.ipynb) · [Knowledge check](QUIZ.md) · [Next section →](../10-intermediate-pyspark-exercises/README.md)

## Outcomes

After this section, you can identify missing values, remove incomplete rows deliberately, fill values by column, choose fallback values and carry known values through an ordered window.

## Null is not the same as zero or an empty string

`NULL` means that a value is unknown or absent. It does not automatically mean zero, false or blank text.

```python
df.filter(F.col("email").isNull())
df.filter(F.col("email").isNotNull())
```

An empty string can be converted to null when the business rule treats it as missing:

```python
cleaned = df.withColumn(
    "email",
    F.when(F.trim("email") == "", None).otherwise(F.trim("email"))
)
```

## Count missing values before changing them

```python
df.select([
    F.sum(F.col(name).isNull().cast("int")).alias(name)
    for name in df.columns
]).show()
```

This creates one null count for every column. Measuring first makes the cleaning decision visible.

## Remove rows with `dropna`

```python
df.dropna()                              # remove rows containing any null
df.dropna(how="all")                    # remove rows where every column is null
df.dropna(subset=["customer_id"])       # require a particular column
df.dropna(thresh=3)                      # require at least three non-null values
```

Dropping a row is a business decision. Use a subset when only specific fields are mandatory.

## Fill values with `fillna`

```python
filled = df.fillna({
    "city": "UNKNOWN",
    "discount": 0.0,
    "quantity": 0,
})
```

The replacement type must match the column type. A dictionary makes the rule for each column explicit.

## Choose the first available value

`coalesce` returns the first non-null expression:

```python
contact = df.withColumn(
    "contact_number",
    F.coalesce("mobile_number", "home_number", F.lit("NOT PROVIDED"))
)
```

This expression is different from `DataFrame.coalesce(n)`, which changes the number of partitions.

## Null-aware aggregations

Most aggregates such as `sum`, `avg`, `min` and `max` ignore null input values. `count("column")` counts only non-null values, while `count("*")` counts rows.

```python
df.groupBy("category").agg(
    F.count("rating").alias("known_ratings"),
    F.count("*").alias("all_rows"),
    F.avg("rating").alias("average_known_rating"),
)
```

## Carry a known value forward

Ordered event data sometimes omits a value when it has not changed. A window can carry the most recent known value forward within each entity.

```python
from pyspark.sql.window import Window

history = (Window.partitionBy("device_id")
           .orderBy("event_time")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))

filled = df.withColumn(
    "temperature_filled",
    F.last("temperature", ignorenulls=True).over(history)
)
```

Always partition by the entity whose history is independent. A window without a suitable partition can move unrelated data together and produce the wrong result.

## Carry the next known value backward

```python
future = (Window.partitionBy("device_id")
          .orderBy("event_time")
          .rowsBetween(Window.currentRow, Window.unboundedFollowing))

filled = df.withColumn(
    "next_known_temperature",
    F.first("temperature", ignorenulls=True).over(future)
)
```

Forward or backward filling is appropriate only when the business meaning supports it. It should not be used merely to make null counts disappear.

## DataFrame API and literal SQL

Every exercise first uses the DataFrame API and then repeats the same rule through a temporary view and `spark.sql`. Compare both outputs before moving forward.

## Official references

- [PySpark missing-data functions](https://spark.apache.org/docs/4.1.4/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html)
- [Spark SQL built-in functions](https://spark.apache.org/docs/4.1.0/api/sql/index.html)

Continue to [Section 10 — Intermediate to difficult exercises](../10-intermediate-pyspark-exercises/README.md).
