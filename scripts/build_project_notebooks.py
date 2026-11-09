#!/usr/bin/env python3
"""Build the two project notebooks without requiring Jupyter or nbformat."""

from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = ROOT / "project-notebooks"


def markdown(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}


def code(text):
    return {
        "cell_type": "code", "execution_count": None, "metadata": {},
        "outputs": [], "source": text.splitlines(keepends=True),
    }


def write(name, cells):
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "PySpark 4.1.4 (Python 3.12)", "language": "python", "name": "pyspark-4.1.4"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        "nbformat": 4, "nbformat_minor": 5,
    }
    (NOTEBOOKS / name).write_text(json.dumps(notebook, indent=1) + "\n", encoding="utf-8")


guided = [
    markdown("""# Guided project — three-run e-commerce batch pipeline

**Time box:** 150 minutes  
**Goal:** Execute three file batches (CSV, JSON, TSV), quarantine bad records, enrich completed sales, write replay-safe Parquet partitions, and create a visual report for every run.

```mermaid
flowchart LR
  A[CSV / JSON / TSV batches] --> B[Explicit landing schema]
  B --> C[Deduplicate and safe-cast]
  C --> D{Quality valid?}
  D -- No --> Q[Rejected Parquet]
  D -- Yes --> E[Completed sales]
  E --> F[Broadcast customer + product joins]
  F --> G{Keys matched?}
  G -- No --> Q
  G -- Yes --> H[Curated Parquet by run and month]
  H --> I[Read destination]
  I --> J[HTML report + audit]
```

Run each cell in order. Every code cell is complete; the trainer pauses at the checkpoints for discussion."""),
    markdown("""## 1. Start Spark and locate the project (10 minutes)

The settings are intentionally small for two cloned local machines. Four shuffle partitions make plans easy to inspect; AQE remains enabled."""),
    code("""from pathlib import Path
import json
import sys

for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / "training_lib.py").is_file():
        ROOT = candidate
        break
else:
    raise FileNotFoundError("Launch Jupyter in the workshop folder")

sys.path.insert(0, str(ROOT))
from training_lib import create_spark
from project.pipeline import (
    load_run_config, read_source_batch, read_references, parse_and_classify,
    enrich_completed, run_pipeline, write_combined_report,
)

spark = create_spark("guided-ecommerce-pipeline")
spark.version, spark.sparkContext.master, spark.conf.get("spark.sql.adaptive.enabled")"""),
    code("""configs = load_run_config(ROOT / "project")
configs"""),
    markdown("""## 2. Walk through run-001 (35 minutes)

Run 1 is a clean CSV batch. We first inspect each stage manually, then call the reusable orchestration function."""),
    code("""raw_1 = read_source_batch(spark, "run-001", ROOT / "project")
raw_1.printSchema()
raw_1.show(20, truncate=False)
assert raw_1.count() == 12"""),
    code("""unique_1, valid_1, quality_rejected_1 = parse_and_classify(raw_1)
print("raw:", raw_1.count())
print("unique:", unique_1.count())
print("valid:", valid_1.count())
print("quality rejected:", quality_rejected_1.count())
valid_1.select("transaction_id", "order_date", "qty", "price", "line_total", "status").show()"""),
    code("""customers, products = read_references(spark, ROOT / "project")
completed_1, accepted_1, reference_rejected_1 = enrich_completed(valid_1, customers, products)
accepted_1.select("transaction_id", "customer_name", "product_name", "category", "line_total").show(truncate=False)
assert completed_1.count() == 10
assert reference_rejected_1.count() == 0"""),
    markdown("""**Checkpoint:** Why broadcast the two reference tables? Why is `COMPLETED` a business rule rather than a data-quality rule?"""),
    code("""metrics_1, report_1 = run_pipeline(spark, "run-001", ROOT / "project")
metrics_1, report_1"""),
    code("""from IPython.display import HTML, display
display(HTML(report_1.read_text(encoding="utf-8")))"""),
    markdown("""## 3. Run-002: observe failures instead of hiding them (35 minutes)

Run 2 is newline-delimited JSON. It contains an identical duplicate, malformed and missing fields, and unknown customer/product identifiers."""),
    code("""raw_2 = read_source_batch(spark, "run-002", ROOT / "project")
raw_2.show(20, truncate=False)
unique_2, valid_2, quality_rejected_2 = parse_and_classify(raw_2)
quality_rejected_2.select("transaction_id", "reject_reason").show(truncate=False)"""),
    code("""completed_2, accepted_2, reference_rejected_2 = enrich_completed(valid_2, customers, products)
reference_rejected_2.select("transaction_id", "customer_id", "product_id", "reject_reason").show(truncate=False)"""),
    code("""metrics_2, report_2 = run_pipeline(spark, "run-002", ROOT / "project")
metrics_2"""),
    code("""rejected_2_path = ROOT / "project" / "destination" / "rejected_records" / "run_id=run-002"
spark.read.parquet(str(rejected_2_path)).select(
    "transaction_id", "reject_stage", "reject_reason"
).orderBy("transaction_id").show(truncate=False)
display(HTML(report_2.read_text(encoding="utf-8")))"""),
    markdown("""## 4. Run-003: corrections and new sales (25 minutes)

Run 3 is TSV. Four identifiers rejected in run 2 return as corrected records, followed by new business transactions."""),
    code("""raw_3 = read_source_batch(spark, "run-003", ROOT / "project")
raw_3.show(20, truncate=False)
metrics_3, report_3 = run_pipeline(spark, "run-003", ROOT / "project")
metrics_3"""),
    code("""display(HTML(report_3.read_text(encoding="utf-8")))"""),
    markdown("""## 5. Replay run-002 safely (15 minutes)

The pipeline overwrites only `run_id=run-002`. Replaying that batch must not append another copy."""),
    code("""curated_base = ROOT / "project" / "destination" / "curated_sales"
before = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
replay_metrics, _ = run_pipeline(spark, "run-002", ROOT / "project", make_report=False)
after = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
print({"before": before, "after": after, "stable": before == after})
assert before == after == 21"""),
    markdown("""## 6. Build the combined destination report (15 minutes)"""),
    code("""all_report = write_combined_report(spark, ROOT / "project")
display(HTML(all_report.read_text(encoding="utf-8")))"""),
    code("""destination = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base))
destination.groupBy("run_id", "order_month").count().orderBy("run_id", "order_month").show()
destination.select("run_id", "transaction_id", "category", "city", "line_total").show(30, truncate=False)"""),
    markdown("""## 7. Validate and inspect the plan (15 minutes)"""),
    code("""expected = json.loads((ROOT / "solutions" / "project_expected_metrics.json").read_text())
for run_id, actual in [("run-001", metrics_1), ("run-002", metrics_2), ("run-003", metrics_3)]:
    for key, expected_value in expected[run_id].items():
        assert actual[key] == expected_value, (run_id, key, actual[key], expected_value)
print("PASS: all three run audits match the expected metrics")"""),
    code("""from pyspark.sql import functions as F
destination.groupBy("category").agg(F.sum("line_total").alias("revenue")).explain("formatted")"""),
    markdown("""## Debrief

- Source format changed; the normalized DataFrame contract did not.
- Invalid values were preserved in a rejection destination with reasons.
- Small dimensions used broadcast joins; the final aggregation still has a shuffle.
- Each `run_id` partition is replaceable, so same-batch replay is idempotent.
- Reports are generated only after reading Parquet back from the destination.
- In production, add orchestration, durable metadata, access controls, alerts, and a transactional table format when requirements justify it."""),
]

solution = [
    markdown("""# Project reference solution — compact execution

This is the trainer's concise end-to-end solution. Use the guided notebook during delivery and reveal this notebook during the final review."""),
    code("""from pathlib import Path
import json, sys
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / "training_lib.py").is_file():
        ROOT = candidate; break
else: raise FileNotFoundError("Workshop root not found")
sys.path.insert(0, str(ROOT))
from training_lib import create_spark
from project.pipeline import run_pipeline, write_combined_report
spark = create_spark("project-reference-solution")"""),
    code("""results = {}
for run_id in ("run-001", "run-002", "run-003"):
    metrics, report = run_pipeline(spark, run_id, ROOT / "project")
    results[run_id] = metrics
    print(run_id, metrics, report)
combined_report = write_combined_report(spark, ROOT / "project")
combined_report"""),
    code("""expected = json.loads((ROOT / "solutions" / "project_expected_metrics.json").read_text())
for run_id, metrics in results.items():
    assert all(metrics[key] == value for key, value in expected[run_id].items())
base = ROOT / "project" / "destination" / "curated_sales"
destination = spark.read.option("basePath", str(base)).parquet(str(base))
assert destination.count() == expected["all-runs"]["accepted_sales_rows"]
print("PASS — three runs, partitioned destination, audits and reports are complete")"""),
]

write("07_guided_project_starter.ipynb", guided)
write("08_project_reference_solution.ipynb", solution)
print("Built guided and reference project notebooks")

