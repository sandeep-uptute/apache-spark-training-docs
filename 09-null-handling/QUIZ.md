# Section 09 knowledge check

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Which expression selects rows where `email` is missing?

- A. `F.col("email") == "NULL"`
- B. `F.col("email").isNull()`
- C. `F.col("email").isEmpty()`
- D. `F.null("email")`

### MCQ 2

What does `df.dropna(subset=["customer_id"])` do?

- A. Removes the `customer_id` column
- B. Removes rows whose `customer_id` is null
- C. Fills missing customer IDs with zero
- D. Removes rows containing any zero

### MCQ 3

Which method fills different columns with different replacement values?

- A. `df.fillna({"city": "UNKNOWN", "discount": 0.0})`
- B. `df.dropna("city", "discount")`
- C. `df.replaceNulls()`
- D. `df.coalesce(2)`

### MCQ 4

What does `F.coalesce("mobile", "home")` return?

- A. The first non-null value from the listed columns
- B. A DataFrame with fewer partitions
- C. The sum of the columns
- D. Only rows where both columns are null

### MCQ 5

Which window expression carries the most recent known reading forward?

- A. `F.last("reading", ignorenulls=True)` over preceding rows
- B. `F.count("reading")` over following rows
- C. `F.row_number()` without ordering
- D. `F.lag("reading", -1)` without partitioning

## Review prompts

1. Why should nulls be counted before rows are dropped or filled?
2. Give one example where forward filling would create an incorrect business meaning.
