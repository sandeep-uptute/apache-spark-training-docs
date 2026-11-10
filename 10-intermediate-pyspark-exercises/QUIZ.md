# Section 10 knowledge check

[← Section notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Which operation turns each element of an array into a separate row?

- A. `explode`
- B. `collect_list`
- C. `pivot`
- D. `coalesce`

### MCQ 2

Which window pattern is commonly used to keep the latest record for each business key?

- A. `row_number` partitioned by key and ordered by timestamp descending
- B. `count` without a partition
- C. `rank` ordered by key ascending only
- D. `lead` without ordering

### MCQ 3

Which join returns left-side rows that do not have a matching right-side key?

- A. Inner
- B. Left semi
- C. Left anti
- D. Cross

### MCQ 4

Why is `dense_rank` useful for a top-products report?

- A. Products with equal revenue receive the same rank without leaving a gap
- B. It removes duplicate rows automatically
- C. It always returns exactly one row per category
- D. It replaces null revenue with zero

### MCQ 5

What is normally required to detect a new user session?

- A. A time-ordered previous-event value for each user
- B. A full outer join with the product table
- C. A global sort without a user partition
- D. A pivot before reading timestamps

## Review prompts

1. Why should change detection use a null-safe comparison?
2. What checks would you perform before and after joining a fact dataset to a reference dataset?
