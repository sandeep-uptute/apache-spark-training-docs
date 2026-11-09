# Section 02 quiz

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Which option makes Spark parse a tab-separated file?

- A. `.option("delimiter", "\\t")`
- B. `.option("header", "tab")`
- C. `.option("format", "tabs")`
- D. `.option("inferSchema", "tab")`

### MCQ 2

Why should a stable pipeline prefer an explicit schema?

- A. It guarantees one output file
- B. It makes expected types and nullability intentional
- C. It removes every invalid row
- D. It sorts the input automatically

### MCQ 3

What does `inferSchema=True` ask Spark to do for CSV data?

- A. Guess column data types from the data
- B. Rename all columns
- C. Create a database table
- D. Convert the file to Parquet

### MCQ 4

What does `printSchema()` display?

- A. Only the row count
- B. Column names, types and nullability
- C. Only the first twenty rows
- D. File permissions

### MCQ 5

Which format is columnar?

- A. CSV
- B. TSV
- C. Plain text
- D. Parquet

## Review prompt

Compare schema inference with an explicit `StructType` schema. When would you use each one?
