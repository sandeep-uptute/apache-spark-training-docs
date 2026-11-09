# Section 04 quiz

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

What does `groupBy("category")` establish?

- A. The grouping key for later aggregate calculations
- B. A permanent SQL table
- C. A file partition on disk
- D. A row-level window

### MCQ 2

Which expression counts distinct orders?

- A. `F.count("order_id")`
- B. `F.countDistinct("order_id")`
- C. `F.sum("order_id")`
- D. `F.first("order_id")`

### MCQ 3

What does grouping by both `order_date` and `category` produce?

- A. One row per source record
- B. One group per date-category combination
- C. One group per file
- D. One group for the whole dataset

### MCQ 4

What must be done before querying a DataFrame by name in `spark.sql`?

- A. Convert it to Pandas
- B. Register a temporary view
- C. Cache every partition
- D. Write it as CSV

### MCQ 5

Why can equivalent SQL and DataFrame expressions produce similar physical plans?

- A. Both are analysed and optimised by Spark SQL
- B. Both always run as Python loops
- C. SQL bypasses Spark's optimiser
- D. DataFrames never use SQL execution

## Review prompt

Explain how grouping changes the level of detail in a result.
