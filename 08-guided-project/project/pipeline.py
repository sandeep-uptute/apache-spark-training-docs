"""Reusable PySpark pipeline for the guided three-run course project.

The implementation deliberately stays within core PySpark and the Python
standard library. It writes replay-safe, run-partitioned Parquet data and
builds standalone HTML reports from data read back from the destination.
"""

from __future__ import annotations

from decimal import Decimal
from html import escape
import json
from pathlib import Path
import re
from typing import Any


TRANSACTION_COLUMNS = [
    "transaction_id", "transaction_date", "customer_id", "product_id",
    "quantity", "unit_price", "status",
]


def _schemas():
    from pyspark.sql.types import StringType, StructField, StructType

    transaction = StructType([StructField(name, StringType(), True) for name in TRANSACTION_COLUMNS])
    customer = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
    ])
    product = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("list_price", StringType(), True),
    ])
    return transaction, customer, product


def project_root(start: str | Path | None = None) -> Path:
    """Find the folder containing the project run configuration."""
    start_path = Path(start).resolve() if start else Path.cwd().resolve()
    for candidate in (start_path, *start_path.parents):
        if (candidate / "project" / "run_config.json").is_file():
            return candidate / "project"
        if candidate.name == "project" and (candidate / "run_config.json").is_file():
            return candidate
    raise FileNotFoundError("Could not find project/run_config.json. Start Jupyter in the course folder.")


def load_run_config(root: str | Path | None = None) -> dict[str, dict[str, str]]:
    root_path = project_root(root)
    configs = json.loads((root_path / "run_config.json").read_text(encoding="utf-8"))
    return {item["run_id"]: item for item in configs["runs"]}


def _safe_run_id(run_id: str) -> str:
    if not re.fullmatch(r"run-[0-9]{3}", run_id):
        raise ValueError(f"Unexpected run_id {run_id!r}; expected run-NNN")
    return run_id


def read_source_batch(spark, run_id: str, root: str | Path | None = None):
    """Read one configured batch using an explicit all-string landing schema."""
    from pyspark.sql import functions as F

    root_path = project_root(root)
    run_id = _safe_run_id(run_id)
    config = load_run_config(root_path)[run_id]
    transaction_schema, _, _ = _schemas()
    source_path = root_path / config["path"]
    fmt = config["format"].lower()
    reader = spark.read.schema(transaction_schema)
    if fmt == "csv":
        frame = reader.option("header", True).csv(str(source_path))
    elif fmt == "tsv":
        frame = reader.option("header", True).option("sep", "\t").csv(str(source_path))
    elif fmt == "json":
        frame = reader.json(str(source_path))
    else:
        raise ValueError(f"Unsupported configured source format: {fmt}")
    return (frame.withColumn("source_file", F.input_file_name())
            .withColumn("run_id", F.lit(run_id)))


def read_references(spark, root: str | Path | None = None):
    """Read the small customer and product dimensions with explicit schemas."""
    root_path = project_root(root)
    _, customer_schema, product_schema = _schemas()
    references = root_path / "reference"
    customers = (spark.read.schema(customer_schema).option("header", True)
                 .csv(str(references / "customers.csv")))
    products = spark.read.schema(product_schema).json(str(references / "products.json"))
    return customers, products


def parse_and_classify(raw):
    """Deduplicate, safely type fields, and attach a readable quality reason."""
    from pyspark.sql import functions as F

    unique = raw.dropDuplicates(["transaction_id"])
    parsed = (unique
              .withColumn("transaction_id", F.trim("transaction_id"))
              .withColumn("customer_id", F.trim("customer_id"))
              .withColumn("product_id", F.trim("product_id"))
              .withColumn("status", F.upper(F.trim("status")))
              .withColumn("order_date", F.expr("try_cast(transaction_date as date)"))
              .withColumn("qty", F.expr("try_cast(quantity as int)"))
              .withColumn("price", F.expr("try_cast(unit_price as decimal(12,2))")))
    reason = F.concat_ws(
        "; ",
        F.when(F.col("transaction_id").isNull() | (F.length("transaction_id") == 0), "missing transaction_id"),
        F.when(F.col("order_date").isNull(), "invalid transaction_date"),
        F.when(F.col("customer_id").isNull() | (F.length("customer_id") == 0), "missing customer_id"),
        F.when(F.col("product_id").isNull() | (F.length("product_id") == 0), "missing product_id"),
        F.when(F.col("qty").isNull() | (F.col("qty") <= 0), "quantity must be a positive integer"),
        F.when(F.col("price").isNull() | (F.col("price") < 0), "unit_price must be non-negative"),
        F.when(F.col("status").isNull() | ~F.col("status").isin("COMPLETED", "CANCELLED", "RETURNED"), "unknown status"),
    )
    classified = parsed.withColumn("reject_reason", reason)
    valid = (classified.filter(F.col("reject_reason") == "").drop("reject_reason")
             .withColumn("line_total", F.col("qty") * F.col("price")))
    rejected = (classified.filter(F.col("reject_reason") != "")
                .withColumn("reject_stage", F.lit("QUALITY")))
    return unique, valid, rejected


def enrich_completed(valid, customers, products):
    """Broadcast small dimensions and separate unmatched business keys."""
    from pyspark.sql import functions as F

    completed = valid.filter(F.col("status") == "COMPLETED")
    joined = (completed.join(F.broadcast(customers), "customer_id", "left")
              .join(F.broadcast(products), "product_id", "left"))
    reason = F.concat_ws(
        "; ",
        F.when(F.col("customer_name").isNull(), "unknown customer_id"),
        F.when(F.col("product_name").isNull(), "unknown product_id"),
    )
    unmatched = (joined.withColumn("reject_reason", reason)
                 .filter(F.col("reject_reason") != "")
                 .withColumn("reject_stage", F.lit("REFERENCE")))
    accepted = (joined.filter(F.col("customer_name").isNotNull() & F.col("product_name").isNotNull())
                .withColumn("order_month", F.date_format("order_date", "yyyy-MM")))
    return completed, accepted, unmatched


def _replace_run_outputs(accepted, rejected, project_path: Path, run_id: str) -> None:
    """Overwrite only the selected run partition so a replay is idempotent."""
    curated_path = project_path / "destination" / "curated_sales" / f"run_id={run_id}"
    rejected_path = project_path / "destination" / "rejected_records" / f"run_id={run_id}"
    (accepted.drop("run_id").write.mode("overwrite").partitionBy("order_month")
     .parquet(str(curated_path)))
    (rejected.drop("run_id").write.mode("overwrite").partitionBy("reject_stage")
     .parquet(str(rejected_path)))


def _write_audit(spark, metrics: dict[str, Any], project_path: Path, run_id: str) -> None:
    audit_path = project_path / "destination" / "audit" / f"run_id={run_id}"
    spark.createDataFrame([metrics]).drop("run_id").write.mode("overwrite").parquet(str(audit_path))


def _bar_rows(rows: list[dict[str, Any]], label_key: str, value_key: str) -> str:
    values = [float(row[value_key] or 0) for row in rows]
    maximum = max(values, default=1.0) or 1.0
    parts = []
    for row, value in zip(rows, values):
        width = max(1.0, (value / maximum) * 100.0) if value else 0
        parts.append(
            '<div class="bar-row"><span class="bar-label">{}</span>'
            '<span class="bar-track"><span class="bar" style="width:{:.1f}%"></span></span>'
            '<span class="bar-value">{:,.2f}</span></div>'.format(
                escape(str(row[label_key])), width, value
            )
        )
    return "\n".join(parts) or "<p>No accepted sales in this run.</p>"


def _table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    header = "".join(f"<th>{escape(column.replace('_', ' ').title())}</th>" for column in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(row.get(column, '')))}</td>" for column in columns) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def _report_document(title: str, subtitle: str, metrics: dict[str, Any], category_rows, city_rows) -> str:
    cards = "".join(
        f'<div class="card"><span>{escape(label)}</span><strong>{escape(str(value))}</strong></div>'
        for label, value in [
            ("Input rows", metrics.get("raw_rows", metrics.get("accepted_sales_rows", 0))),
            ("Accepted sales", metrics.get("accepted_sales_rows", 0)),
            ("Rejected rows", metrics.get("rejected_rows", 0)),
            ("Revenue", metrics.get("accepted_revenue", "0.00")),
        ]
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title><style>
body{{font-family:system-ui,sans-serif;margin:0;background:#f4f7fb;color:#172033}}main{{max-width:1050px;margin:auto;padding:32px}}
h1{{margin-bottom:4px}}.subtitle{{color:#59657a;margin-top:0}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:24px 0}}
.card,section{{background:white;border:1px solid #dce3ed;border-radius:12px;padding:18px;box-shadow:0 2px 8px #1b2b4b12}}.card span{{display:block;color:#667085;font-size:.86rem}}
.card strong{{display:block;font-size:1.55rem;margin-top:6px}}section{{margin:18px 0}}.bar-row{{display:grid;grid-template-columns:150px 1fr 100px;gap:12px;align-items:center;margin:10px 0}}
.bar-track{{height:18px;background:#e7ecf4;border-radius:9px;overflow:hidden}}.bar{{display:block;height:100%;background:linear-gradient(90deg,#1f77b4,#35a7a0)}}.bar-value{{text-align:right;font-variant-numeric:tabular-nums}}
table{{width:100%;border-collapse:collapse}}th,td{{padding:9px;border-bottom:1px solid #e5e9f0;text-align:left}}th{{background:#f7f9fc}}footer{{color:#667085;font-size:.85rem;margin-top:24px}}
@media(max-width:750px){{.cards{{grid-template-columns:1fr 1fr}}.bar-row{{grid-template-columns:100px 1fr 80px}}}}
</style></head><body><main><h1>{escape(title)}</h1><p class="subtitle">{escape(subtitle)}</p>
<div class="cards">{cards}</div>
<section><h2>Revenue by category</h2>{_bar_rows(category_rows, 'category', 'revenue')}</section>
<section><h2>Revenue by city</h2>{_bar_rows(city_rows, 'city', 'revenue')}</section>
<section><h2>Category detail</h2>{_table(category_rows, ['category', 'orders', 'units', 'revenue'])}</section>
<footer>Generated only from the Parquet destination after the pipeline write completed.</footer>
</main></body></html>"""


def write_run_report(spark, project_path: Path, run_id: str, metrics: dict[str, Any]) -> Path:
    """Read one destination partition and create a dependency-free visual report."""
    from pyspark.sql import functions as F

    curated_path = project_path / "destination" / "curated_sales" / f"run_id={run_id}"
    destination = spark.read.parquet(str(curated_path))
    category_rows = [row.asDict() for row in (destination.groupBy("category")
                     .agg(F.countDistinct("transaction_id").alias("orders"),
                          F.sum("qty").alias("units"), F.sum("line_total").alias("revenue"))
                     .orderBy("category").collect())]
    city_rows = [row.asDict() for row in (destination.groupBy("city")
                 .agg(F.sum("line_total").alias("revenue"))
                 .orderBy(F.desc("revenue")).collect())]
    document = _report_document(
        f"E-commerce pipeline report — {run_id}",
        "Accepted completed sales, read back from the run-partitioned Parquet destination.",
        metrics, category_rows, city_rows,
    )
    report_path = project_path / "reports" / f"{run_id}.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(document, encoding="utf-8")
    return report_path


def write_combined_report(spark, root: str | Path | None = None) -> Path:
    """Create the all-runs report from the complete curated destination."""
    from pyspark.sql import functions as F

    project_path = project_root(root)
    base = project_path / "destination" / "curated_sales"
    destination = spark.read.option("basePath", str(base)).parquet(str(base))
    revenue = destination.agg(F.sum("line_total").alias("value")).first()["value"] or Decimal("0")
    metrics = {
        "raw_rows": "—", "accepted_sales_rows": destination.count(),
        "rejected_rows": "see run reports", "accepted_revenue": f"{revenue:.2f}",
    }
    category_rows = [row.asDict() for row in (destination.groupBy("category")
                     .agg(F.countDistinct("transaction_id").alias("orders"),
                          F.sum("qty").alias("units"), F.sum("line_total").alias("revenue"))
                     .orderBy("category").collect())]
    city_rows = [row.asDict() for row in (destination.groupBy("city")
                 .agg(F.sum("line_total").alias("revenue"))
                 .orderBy(F.desc("revenue")).collect())]
    document = _report_document(
        "E-commerce pipeline report — all runs",
        "Combined accepted sales read from every run_id partition in the Parquet destination.",
        metrics, category_rows, city_rows,
    )
    report_path = project_path / "reports" / "all-runs.html"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(document, encoding="utf-8")
    return report_path


def run_pipeline(spark, run_id: str, root: str | Path | None = None, make_report: bool = True):
    """Execute and audit one complete, replay-safe batch pipeline run."""
    from pyspark.sql import functions as F

    project_path = project_root(root)
    run_id = _safe_run_id(run_id)
    raw = read_source_batch(spark, run_id, project_path).cache()
    unique, valid, quality_rejected = parse_and_classify(raw)
    customers, products = read_references(spark, project_path)
    completed, accepted, reference_rejected = enrich_completed(valid, customers, products)
    accepted = accepted.cache()
    quality_rejected = quality_rejected.cache()
    reference_rejected = reference_rejected.cache()

    raw_count = raw.count()
    unique_count = unique.count()
    valid_count = valid.count()
    completed_count = completed.count()
    accepted_count = accepted.count()
    quality_rejected_count = quality_rejected.count()
    reference_rejected_count = reference_rejected.count()
    revenue = accepted.agg(F.sum("line_total").alias("value")).first()["value"] or Decimal("0")
    rejected = quality_rejected.unionByName(reference_rejected, allowMissingColumns=True)
    metrics = {
        "run_id": run_id,
        "raw_rows": raw_count,
        "duplicate_rows": raw_count - unique_count,
        "unique_rows": unique_count,
        "quality_rejected_rows": quality_rejected_count,
        "valid_rows": valid_count,
        "non_completed_rows": valid_count - completed_count,
        "reference_rejected_rows": reference_rejected_count,
        "rejected_rows": quality_rejected_count + reference_rejected_count,
        "accepted_sales_rows": accepted_count,
        "accepted_revenue": f"{revenue:.2f}",
    }
    _replace_run_outputs(accepted, rejected, project_path, run_id)
    _write_audit(spark, metrics, project_path, run_id)
    report_path = write_run_report(spark, project_path, run_id, metrics) if make_report else None
    for frame in (raw, accepted, quality_rejected, reference_rejected):
        frame.unpersist()
    return metrics, report_path

