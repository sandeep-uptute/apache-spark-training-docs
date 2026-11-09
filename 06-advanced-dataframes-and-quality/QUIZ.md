# Section 06 quiz

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

What is the main advantage of `unionByName` over positional `union`?

- A. It aligns columns by name
- B. It removes duplicates automatically
- C. It joins rows by key
- D. It always sorts the result

### MCQ 2

What does `exceptAll` return?

- A. Rows common to both DataFrames
- B. Left-side rows remaining after right-side occurrences are removed
- C. All rows from both DataFrames
- D. Only duplicate column names

### MCQ 3

Why is pivot considered an aggregation?

- A. Values must be combined for each row-and-pivot-column group
- B. It only renames columns
- C. It reads a different file format
- D. It creates a SparkSession

### MCQ 4

How does `dense_rank` handle a tie followed by the next distinct value?

- A. It leaves a gap after the tie
- B. It gives every row a unique number
- C. It assigns the next consecutive rank without a gap
- D. It returns null

### MCQ 5

Which window frame creates a running total from the first row through the current row?

- A. `rowsBetween(Window.unboundedPreceding, Window.currentRow)`
- B. `rowsBetween(Window.currentRow, Window.unboundedFollowing)`
- C. `partitionBy()` without ordering
- D. `groupBy()` without an aggregate

## Review prompts

1. Why should a rejected row retain a rejection reason?
2. Compare `row_number`, `rank` and `dense_rank` when values are tied.
