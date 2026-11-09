#!/usr/bin/env python3
"""Offline structural checks for the final workshop package (stdlib only)."""

from pathlib import Path
import ast
import csv
import json

ROOT = Path(__file__).resolve().parents[1]

REQUIRED = [
    "START_HERE.md", "DELIVERY_GUIDE.md", "LEARNER_HANDBOOK.md",
    "PRESENTATION.md", "PROJECT_GUIDE.md", "SETUP_AND_TROUBLESHOOTING.md",
    "QUICK_REFERENCE.md", "KNOWLEDGE_CHECK.md", "exercises/TOPIC_EXERCISES.md",
    "solutions/EXERCISE_SOLUTIONS.md", "solutions/KNOWLEDGE_CHECK_ANSWERS.md",
    "solutions/project_expected_metrics.json", "project/pipeline.py",
    "project/run_config.json", "project-notebooks/07_guided_project_starter.ipynb",
    "project-notebooks/08_project_reference_solution.ipynb",
    "project/source/run-001/transactions.csv", "project/source/run-002/transactions.json",
    "project/source/run-003/transactions.tsv", "project/reference/customers.csv",
    "project/reference/products.json",
]


def row_count(path: Path, delimiter: str = ",") -> int:
    with path.open(newline="", encoding="utf-8") as stream:
        return sum(1 for _ in csv.DictReader(stream, delimiter=delimiter))


def main() -> None:
    missing = [item for item in REQUIRED if not (ROOT / item).is_file()]
    assert not missing, f"Missing files: {missing}"

    for relative in ("project/pipeline.py", "scripts/smoke_test_multirun_project.py"):
        ast.parse((ROOT / relative).read_text(encoding="utf-8"), filename=relative)

    for relative in (
        "project/run_config.json", "solutions/project_expected_metrics.json",
        "project-notebooks/07_guided_project_starter.ipynb",
        "project-notebooks/08_project_reference_solution.ipynb",
    ):
        parsed = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        if relative.endswith("ipynb"):
            assert parsed["nbformat"] == 4 and parsed["cells"]

    assert row_count(ROOT / "project/source/run-001/transactions.csv") == 12
    assert sum(1 for line in (ROOT / "project/source/run-002/transactions.json").read_text().splitlines() if line) == 10
    assert row_count(ROOT / "project/source/run-003/transactions.tsv", "\t") == 9
    print("FINAL PACKAGE STRUCTURE PASS")


if __name__ == "__main__":
    main()

