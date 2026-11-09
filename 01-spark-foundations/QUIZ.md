# Section 01 quiz

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

What is the Spark driver's main responsibility?

- A. Store every input file permanently
- B. Coordinate the application and schedule work
- C. Execute every task on one executor
- D. Convert Parquet files to CSV

### MCQ 2

What does one Spark task normally process?

- A. One partition
- B. One application
- C. One cluster
- D. One notebook

### MCQ 3

Which operation is an action?

- A. `select`
- B. `filter`
- C. `count`
- D. `withColumn`

### MCQ 4

Why can lazy evaluation improve execution?

- A. Spark can optimise the complete transformation plan before running it
- B. Spark skips all actions
- C. Spark stores every intermediate result automatically
- D. Spark uses only one partition

### MCQ 5

What does `local[2]` mean?

- A. Two executors on two remote machines
- B. Local execution with two worker threads
- C. Two Spark applications
- D. Two output files are guaranteed

## Review prompt

Explain the sequence **action → job → stage → task** in your own words.
