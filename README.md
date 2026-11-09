# Apache Spark fundamentals with PySpark

This course follows one continuous path from Spark fundamentals to a complete multi-run e-commerce pipeline.

## Course syllabus

| Section | Study materials | Main outcome |
|---|---|---|
| 00 | [Environment setup](00-setup/README.md) · [Quick reference](00-setup/QUICK_REFERENCE.md) | Verify Spark, Java, Python and Jupyter |
| 01 | [Notes](01-spark-foundations/README.md) · [Lab](01-spark-foundations/LAB.ipynb) · [Quiz](01-spark-foundations/QUIZ.md) | Explain Spark architecture and execution terms |
| 02 | [Notes](02-reading-data/README.md) · [Lab](02-reading-data/LAB.ipynb) · [Quiz](02-reading-data/QUIZ.md) | Read CSV, JSON, TSV and Parquet |
| 03 | [Notes](03-dataframes-and-quality/README.md) · [Lab](03-dataframes-and-quality/LAB.ipynb) · [Quiz](03-dataframes-and-quality/QUIZ.md) | Transform, type and validate DataFrames |
| 04 | [Notes](04-aggregations-and-sql/README.md) · [Lab](04-aggregations-and-sql/LAB.ipynb) · [Quiz](04-aggregations-and-sql/QUIZ.md) | Build summaries with DataFrames and SQL |
| 05 | [Notes](05-joins-and-parquet/README.md) · [Lab](05-joins-and-parquet/LAB.ipynb) · [Quiz](05-joins-and-parquet/QUIZ.md) | Join references and publish Parquet |
| 06 | [Notes](06-execution-and-optimization/README.md) · [Lab](06-execution-and-optimization/LAB.ipynb) · [Quiz](06-execution-and-optimization/QUIZ.md) | Read plans and apply practical optimizations |
| 07 | [Project notes](07-guided-project/README.md) · [Project lab](07-guided-project/LAB.ipynb) · [Project review](07-guided-project/QUIZ.md) | Run and verify the complete pipeline |

## Schedule

| Time | Section |
|---|---|
| 6:30–6:45 | Environment check |
| 6:45–7:15 | Section 01 — Spark foundations |
| 7:15–7:50 | Section 02 — Reading data |
| 7:50–8:50 | Section 03 — DataFrames and quality |
| 8:50–9:05 | Break |
| 9:05–9:50 | Section 04 — Aggregations and SQL |
| 9:50–10:35 | Section 05 — Joins and Parquet |
| 10:35–11:05 | Section 06 — Execution and optimization |
| 11:05–11:50 | Meal break |
| 11:50–2:20 | Section 07 — Guided project |
| 2:20–2:30 | Final review |

## How to work through each section

1. Read the section notes.
2. Complete the section lab from the first notebook cell.
3. Answer the section quiz without running additional code.
4. Continue to the next numbered section.

Start Jupyter from this folder so that all relative paths resolve correctly:

```bash
bash 00-setup/scripts/launch_jupyter.sh
```

The final project processes CSV, JSON and TSV batches, writes partitioned Parquet, records rejected rows and audit metrics, produces visual reports, and verifies that a repeated run does not duplicate accepted output.

