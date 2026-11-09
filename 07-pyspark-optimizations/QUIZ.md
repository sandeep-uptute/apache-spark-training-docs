# Section 07 knowledge check

[← Section notes](README.md) · [Course syllabus](../README.md) · [Project →](../08-guided-project/README.md)

Choose one answer for each question.

### MCQ 1

What does one Spark task normally process during a stage?

- A. One partition
- B. One complete cluster
- C. One notebook
- D. One SparkSession

### MCQ 2

When is `coalesce` usually the better first choice?

- A. Increasing from 4 partitions to 40 with even redistribution
- B. Reducing partitions after a large filter before writing
- C. Balancing a heavily skewed join key
- D. Sorting every row globally

### MCQ 3

When is caching most likely to help?

- A. A cheap DataFrame used by one action
- B. Every DataFrame created by the application
- C. An expensive intermediate DataFrame reused by several actions
- D. A DataFrame immediately written and never used again

### MCQ 4

What information does Adaptive Query Execution use to revise a plan?

- A. Runtime statistics collected while the query executes
- B. The notebook filename
- C. Python variable names
- D. The number of Markdown cells

### MCQ 5

Why can broadcasting a small dimension improve a join?

- A. It guarantees that no executor uses memory
- B. It can avoid shuffling the large side of the join
- C. It converts every join into a cross join
- D. It automatically removes duplicate keys

## Review prompts

1. Explain why `repartition(1)` is not a universal solution for controlling output files.
2. Name two items you would inspect before changing an optimization setting.
