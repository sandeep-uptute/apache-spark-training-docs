# Section 07 project review

[← Project notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Why are landing fields initially read as strings?

- A. Validation can control when and how values are converted
- B. Strings are always smaller than numeric values
- C. Parquet requires every field to be a string
- D. Spark cannot read dates

### MCQ 2

Why is a duplicate counted separately from a rejected row?

- A. Deduplication and data-quality validation describe different outcomes
- B. Duplicates cannot be detected in Spark
- C. Every rejected row is a duplicate
- D. Duplicates must always be accepted

### MCQ 3

Why are cancelled and returned transactions not classified as malformed quality rejects?

- A. Their records may be valid while their business status excludes them from sales
- B. They contain no transaction identifier
- C. They can never contain a price
- D. Spark drops them automatically

### MCQ 4

Why are customer and product reference tables suitable broadcast candidates in this project?

- A. They are small compared with the transaction data
- B. They contain only strings
- C. They are CSV files
- D. Broadcast joins require more shuffling

### MCQ 5

Why are reports generated after reading the destination Parquet data back?

- A. The report then validates the persisted result rather than only an in-memory DataFrame
- B. CSV files cannot be aggregated
- C. Spark SQL works only on Parquet
- D. Reading back guarantees a single output file

## Review prompts

1. Reconcile run-002: `raw=10`, `duplicates=1`, `quality rejects=2`, `non-completed=2`, `reference rejects=2`, `accepted=3`.
2. Which additional transactional or operational guarantees would a production platform require?
