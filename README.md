# Apache Spark Fundamentals — 8-Hour Industry Workshop

> **Final delivery entry point:** use [`START_HERE.md`](START_HERE.md). It indexes the expanded learner handbook, section-based trainer flow, three-run guided project, exercises, solutions, quiz, presentation, setup guide and quick reference. The notes below describe the original foundation package retained for compatibility.

**Environment:** Ubuntu 22.04 LTS (confirm on the host), Apache Spark/PySpark **4.1.x**, OpenJDK **17**, Python **3.12.3**, JupyterLab (local Spark).  
**Format:** Markdown presentations and notes + independent Jupyter notebooks + reusable datasets.  
**Audience:** Data analysts and software developers who know basic Python/SQL.  
**Result:** A validated notebook and a Parquet sales summary for an e-commerce pipeline.

## Read this first

1. Follow [`notes/00_ubuntu_preflight.md`](notes/00_ubuntu_preflight.md) **before the session**, ideally on the actual training machine. No separate Hadoop server or Windows `winutils` is needed on Ubuntu for these local file exercises.
2. Use [`notes/00_session_guide.md`](notes/00_session_guide.md) for the complete 9:30–5:30 plan. The 7 topic notes are readable as presentations in any Markdown previewer; Mermaid diagrams render in compatible viewers (for example, VS Code Markdown Preview with Mermaid support).
3. Open the corresponding numbered notebook and run each cell. Every notebook boots its own Spark session or reuses the active one. Do not create another kernel for every attendee on a single machine without considering RAM.
4. Use [`notebooks/07_guided_project_starter.ipynb`](notebooks/07_guided_project_starter.ipynb) for the 60-minute final challenge. Keep [`notebooks/08_project_reference_solution.ipynb`](notebooks/08_project_reference_solution.ipynb) aside until the debrief.
5. For questions or troubleshooting, consult [`notes/09_trainer_runbook.md`](notes/09_trainer_runbook.md).

## Time allocation

| Time | Module | Duration |
|---|---|---:|
| 9:30–9:45 | Welcome, objectives, preflight | 15 min |
| 9:45–10:15 | 01 Spark Introduction | 30 min |
| 10:15–11:00 | 02 Getting Started | 45 min |
| 11:00–11:15 | Break | 15 min |
| 11:15–12:30 | 03 DataFrame Fundamentals | 75 min |
| 12:30–1:30 | 04 Aggregations and Spark SQL | 60 min |
| 1:30–2:15 | Lunch | 45 min |
| 2:15–3:15 | 05 Joining and Saving | 60 min |
| 3:15–3:45 | 06 Execution and Performance | 30 min |
| 3:45–4:00 | Break | 15 min |
| 4:00–5:00 | 07 Guided Mini-Project | 60 min |
| 5:00–5:15 | Validation and knowledge check | 15 min |
| 5:15–5:30 | Questions and closing | 15 min |

**Total:** 480 minutes = 405 teaching / knowledge check + 75 breaks. Original seven module durations total 360 minutes; extra instructional minutes are welcome (15), validation (15), closing (15).

## Folder map

```text
README.md
training_lib.py                    # shared read/clean/report helpers
notes/
  00_ubuntu_preflight.md
  00_session_guide.md
  01_spark_introduction.md
  02_getting_started.md
  03_dataframe_fundamentals.md
  04_aggregations_and_spark_sql.md
  05_joining_and_saving.md
  06_execution_and_performance.md
  07_guided_mini_project.md
  09_trainer_runbook.md
notebooks/
  01_introduction.ipynb
  02_getting_started.ipynb
  03_dataframe_fundamentals.ipynb
  04_aggregations_and_sql.ipynb
  05_joins_and_saving.ipynb
  06_execution_and_performance.ipynb
  07_guided_project_starter.ipynb
  08_project_reference_solution.ipynb
data/
  transactions.csv
  customers.csv
  products.tsv
  products.json
solutions/
  expected_metrics.json
scripts/
  generate_data.py
  smoke_test.py
  launch_jupyter.sh
  validate_package.py
output/                          # notebooks generate Parquet and CSV here
```

## Reproducibility notes

- Run `python scripts/generate_data.py` if you intentionally want to regenerate the provided dataset. It is deterministic and does not require PySpark.
- `python scripts/validate_package.py` checks notebook Python syntax and the supplied file layout. It does **not** execute Spark.
- `python scripts/smoke_test.py` is the important **actual PySpark integration check**. It starts Spark, reads the sources, runs joins and aggregations, saves and reads Parquet, and validates expected totals.
- Some notebooks write to `output/` with `mode('overwrite')`, so re-running is intentional and safe **provided you do not point those cells at a different directory**.
- For live training, aim for `local[2]` and `spark.sql.shuffle.partitions=4` so demonstrations work comfortably on modest hardware. These settings are illustrative and not production recommendations.

## Core expected result

**58** raw records → **57** unique transaction IDs → **50** base-quality records → **36** completed, reference-valid sales.  
Final completed revenue: **342,228.00** (illustrative currency units). Four categories.

## Official background references

- [Spark 4.1.4 documentation](https://spark.apache.org/docs/4.1.4/)
- [PySpark 4.1.4 getting started](https://spark.apache.org/docs/4.1.4/api/python/getting_started/index.html)
- [PySpark SparkSession API](https://spark.apache.org/docs/4.1.4/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
