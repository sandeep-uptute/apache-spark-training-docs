// Authoring helper for Windows hosts without Python. The delivered notebooks
// themselves run Python/PySpark on the cloned Ubuntu training environment.
const fs = require('fs');
const path = require('path');
const root = path.resolve(__dirname, '..');

const md = (source) => ({cell_type: 'markdown', metadata: {}, source: source.split(/(?<=\n)/)});
const py = (source) => ({cell_type: 'code', execution_count: null, metadata: {}, outputs: [], source: source.split(/(?<=\n)/)});
const write = (name, cells) => {
  const notebook = {
    cells,
    metadata: {
      kernelspec: {display_name: 'PySpark 4.1.4 (Python 3.12)', language: 'python', name: 'pyspark-4.1.4'},
      language_info: {name: 'python', version: '3.12'}
    },
    nbformat: 4,
    nbformat_minor: 5
  };
  fs.writeFileSync(path.join(root, 'project-notebooks', name), JSON.stringify(notebook, null, 1) + '\n');
};

const guided = [
  md(`# Guided project — three-run e-commerce batch pipeline

**Time box:** 150 minutes  
**Goal:** Execute three file batches (CSV, JSON, TSV), quarantine bad records, enrich completed sales, write replay-safe Parquet partitions, and create a visual report for every run.

\`\`\`mermaid
flowchart LR
  A[CSV / JSON / TSV] --> B[Explicit schema]
  B --> C[Deduplicate and safe-cast]
  C --> D{Quality valid?}
  D -- No --> Q[Rejected Parquet]
  D -- Yes --> E[Completed sales]
  E --> F[Broadcast reference joins]
  F --> G{Keys matched?}
  G -- No --> Q
  G -- Yes --> H[Curated Parquet by run and month]
  H --> I[Read destination]
  I --> J[Audit and HTML report]
\`\`\`

Run every cell in order. All code is supplied; checkpoints are discussion pauses.`),
  md(`## 1. Start Spark and locate the project — 10 minutes

These settings suit two cloned local machines. Four shuffle partitions make plans readable; adaptive execution remains enabled.`),
  py(`from pathlib import Path
import json, sys

for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / "training_lib.py").is_file():
        ROOT = candidate
        break
else:
    raise FileNotFoundError("Launch Jupyter in the workshop folder")

sys.path.insert(0, str(ROOT))
from training_lib import create_spark
from project.pipeline import (load_run_config, read_source_batch, read_references,
    parse_and_classify, enrich_completed, run_pipeline, write_combined_report)

spark = create_spark("guided-ecommerce-pipeline")
spark.version, spark.sparkContext.master, spark.conf.get("spark.sql.adaptive.enabled")`),
  py(`configs = load_run_config(ROOT / "project")
configs`),
  md(`## 2. Walk through run-001 — 35 minutes

Run 1 is a clean CSV batch. Inspect every stage, then call the reusable orchestration function.`),
  py(`raw_1 = read_source_batch(spark, "run-001", ROOT / "project")
raw_1.printSchema()
raw_1.show(20, truncate=False)
assert raw_1.count() == 12`),
  py(`unique_1, valid_1, quality_rejected_1 = parse_and_classify(raw_1)
print("raw:", raw_1.count(), "unique:", unique_1.count(), "valid:", valid_1.count())
print("quality rejected:", quality_rejected_1.count())
valid_1.select("transaction_id", "order_date", "qty", "price", "line_total", "status").show()`),
  py(`customers, products = read_references(spark, ROOT / "project")
completed_1, accepted_1, reference_rejected_1 = enrich_completed(valid_1, customers, products)
accepted_1.select("transaction_id", "customer_name", "product_name", "category", "line_total").show(truncate=False)
assert completed_1.count() == 10
assert reference_rejected_1.count() == 0`),
  md(`**Checkpoint:** Why broadcast the references? Why is \`COMPLETED\` a business rule rather than a data-quality rule?`),
  py(`metrics_1, report_1 = run_pipeline(spark, "run-001", ROOT / "project")
metrics_1, report_1`),
  py(`from IPython.display import HTML, display
display(HTML(report_1.read_text(encoding="utf-8")))`),
  md(`## 3. Run-002: expose failures — 35 minutes

The newline-delimited JSON batch contains an identical duplicate, malformed and missing values, and unknown identifiers.`),
  py(`raw_2 = read_source_batch(spark, "run-002", ROOT / "project")
unique_2, valid_2, quality_rejected_2 = parse_and_classify(raw_2)
quality_rejected_2.select("transaction_id", "reject_reason").show(truncate=False)`),
  py(`completed_2, accepted_2, reference_rejected_2 = enrich_completed(valid_2, customers, products)
reference_rejected_2.select("transaction_id", "customer_id", "product_id", "reject_reason").show(truncate=False)`),
  py(`metrics_2, report_2 = run_pipeline(spark, "run-002", ROOT / "project")
metrics_2`),
  py(`rejected_2_path = ROOT / "project" / "destination" / "rejected_records" / "run_id=run-002"
spark.read.parquet(str(rejected_2_path)).select(
    "transaction_id", "reject_stage", "reject_reason"
).orderBy("transaction_id").show(truncate=False)
display(HTML(report_2.read_text(encoding="utf-8")))`),
  md(`## 4. Run-003: corrections and new sales — 25 minutes

The TSV batch supplies corrected versions of four rejected identifiers plus new transactions.`),
  py(`raw_3 = read_source_batch(spark, "run-003", ROOT / "project")
raw_3.show(20, truncate=False)
metrics_3, report_3 = run_pipeline(spark, "run-003", ROOT / "project")
metrics_3`),
  py(`display(HTML(report_3.read_text(encoding="utf-8")))`),
  md(`## 5. Replay run-002 safely — 15 minutes

Only the \`run_id=run-002\` partition is replaced. The destination count must remain stable.`),
  py(`curated_base = ROOT / "project" / "destination" / "curated_sales"
before = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
replay_metrics, _ = run_pipeline(spark, "run-002", ROOT / "project", make_report=False)
after = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
print({"before": before, "after": after, "stable": before == after})
assert before == after == 21`),
  md(`## 6. Combined destination report — 15 minutes`),
  py(`all_report = write_combined_report(spark, ROOT / "project")
display(HTML(all_report.read_text(encoding="utf-8")))`),
  py(`destination = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base))
destination.groupBy("run_id", "order_month").count().orderBy("run_id", "order_month").show()
destination.select("run_id", "transaction_id", "category", "city", "line_total").show(30, truncate=False)`),
  md(`## 7. Validate and inspect the plan — 15 minutes`),
  py(`expected = json.loads((ROOT / "solutions" / "project_expected_metrics.json").read_text())
for run_id, actual in [("run-001", metrics_1), ("run-002", metrics_2), ("run-003", metrics_3)]:
    for key, expected_value in expected[run_id].items():
        assert actual[key] == expected_value, (run_id, key, actual[key], expected_value)
print("PASS: all three run audits match expected metrics")`),
  py(`from pyspark.sql import functions as F
destination.groupBy("category").agg(F.sum("line_total").alias("revenue")).explain("formatted")`),
  md(`## Debrief

- Source format changed; the normalized DataFrame contract did not.
- Invalid records were preserved with rejection reasons.
- Small dimensions used broadcast joins; the aggregation still shuffles.
- Each run partition is replaceable, making same-batch replay idempotent.
- Reports were built only after reading the destination Parquet.
- Production systems also need orchestration, durable metadata, access control, monitoring, and recovery procedures.`)
];

const solution = [
  md(`# Project reference solution — compact execution

Use the guided notebook during delivery. This compact solution is for the trainer review.`),
  py(`from pathlib import Path
import json, sys
for candidate in (Path.cwd(), *Path.cwd().parents):
    if (candidate / "training_lib.py").is_file():
        ROOT = candidate; break
else: raise FileNotFoundError("Workshop root not found")
sys.path.insert(0, str(ROOT))
from training_lib import create_spark
from project.pipeline import run_pipeline, write_combined_report
spark = create_spark("project-reference-solution")`),
  py(`results = {}
for run_id in ("run-001", "run-002", "run-003"):
    metrics, report = run_pipeline(spark, run_id, ROOT / "project")
    results[run_id] = metrics
    print(run_id, metrics, report)
combined_report = write_combined_report(spark, ROOT / "project")
combined_report`),
  py(`expected = json.loads((ROOT / "solutions" / "project_expected_metrics.json").read_text())
for run_id, metrics in results.items():
    assert all(metrics[key] == value for key, value in expected[run_id].items())
base = ROOT / "project" / "destination" / "curated_sales"
destination = spark.read.option("basePath", str(base)).parquet(str(base))
assert destination.count() == expected["all-runs"]["accepted_sales_rows"]
print("PASS — three runs, Parquet partitions, audits, and reports complete")`)
];

write('07_guided_project_starter.ipynb', guided);
write('08_project_reference_solution.ipynb', solution);
console.log('Built guided and reference project notebooks');

