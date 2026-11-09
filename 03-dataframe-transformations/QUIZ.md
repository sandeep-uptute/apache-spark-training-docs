# Section 03 quiz

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Which transformation chooses a subset of columns?

- A. `select`
- B. `filter`
- C. `orderBy`
- D. `count`

### MCQ 2

How are two PySpark column conditions combined with logical AND?

- A. `and`
- B. `&&`
- C. `&`
- D. `AND()`

### MCQ 3

What does `withColumn` return?

- A. A new DataFrame
- B. A changed source file
- C. A Python list
- D. A SparkSession

### MCQ 4

What is the purpose of `alias` inside `select`?

- A. Cache a DataFrame
- B. Give an expression a result-column name
- C. Remove nulls
- D. Change the partition count

### MCQ 5

Which expression sorts revenue from highest to lowest?

- A. `.orderBy("revenue")`
- B. `.orderBy(F.asc("revenue"))`
- C. `.orderBy(F.desc("revenue"))`
- D. `.filter(F.desc("revenue"))`

## Review prompt

Describe a readable transformation order that selects columns, derives a value, filters rows and sorts the result.
