#!/usr/bin/env python3
"""Execute all project runs and validate Parquet/audit/report outcomes on Ubuntu."""

from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from training_lib import create_spark
from project.pipeline import run_pipeline, write_combined_report


def main() -> None:
    spark = create_spark("multi-run-project-smoke-test")
    expected = json.loads((ROOT / "solutions" / "project_expected_metrics.json").read_text(encoding="utf-8"))
    try:
        for run_id in ("run-001", "run-002", "run-003"):
            actual, report = run_pipeline(spark, run_id, ROOT / "project")
            for key, value in expected[run_id].items():
                assert actual[key] == value, (run_id, key, actual[key], value)
            assert report and report.is_file() and report.stat().st_size > 1000

        combined = write_combined_report(spark, ROOT / "project")
        assert combined.is_file() and combined.stat().st_size > 1000

        curated_base = ROOT / "project" / "destination" / "curated_sales"
        before = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
        run_pipeline(spark, "run-002", ROOT / "project", make_report=False)
        after = spark.read.option("basePath", str(curated_base)).parquet(str(curated_base)).count()
        assert before == after == expected["all-runs"]["accepted_sales_rows"]
        print("MULTI-RUN PROJECT PASS", {"accepted_rows": after, "combined_report": str(combined)})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

