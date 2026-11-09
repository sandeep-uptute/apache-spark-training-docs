#!/usr/bin/env python3
"""Offline structure, notebook and wording checks for the course package."""

from pathlib import Path
import ast
import csv
import json
import re

ROOT = Path(__file__).resolve().parents[2]
SECTIONS = [
    "01-spark-foundations",
    "02-reading-data",
    "03-dataframes-and-quality",
    "04-aggregations-and-sql",
    "05-joins-and-parquet",
    "06-execution-and-optimization",
    "07-guided-project",
]


def row_count(path: Path, delimiter: str = ",") -> int:
    with path.open(newline="", encoding="utf-8") as stream:
        return sum(1 for _ in csv.DictReader(stream, delimiter=delimiter))


def validate_notebook(path: Path) -> None:
    book = json.loads(path.read_text(encoding="utf-8"))
    assert book.get("nbformat") == 4 and book.get("cells"), path
    code_sources = []
    for index, cell in enumerate(book["cells"], start=1):
        if cell.get("cell_type") == "code":
            source = "".join(cell.get("source", []))
            code_sources.append(source)
            ast.parse(source, filename=f"{path.name}:cell{index}")
            forbidden = ("training_lib", "project.pipeline", "sys.path", "Path.cwd")
            assert not any(term in source for term in forbidden), f"Hidden setup/helper found in {path}:cell{index}"
    assert len(code_sources) == 6, f"Expected one setup plus five activities: {path}"
    assert sum("SparkSession.builder" in source for source in code_sources) == 1, f"Expected one Spark session: {path}"
    assert "SparkSession.builder" in code_sources[0], f"First code cell must create Spark: {path}"
    text = path.read_text(encoding="utf-8")
    if path.parent.name == "07-guided-project":
        assert text.count("## Stage ") >= 5, f"Expected five project stages: {path}"
    else:
        assert text.count("## Exercise ") >= 5, f"Expected five exercises: {path}"


def main() -> None:
    required = [ROOT / "README.md", ROOT / "00-setup" / "README.md"]
    for section in SECTIONS:
        required.extend([
            ROOT / section / "README.md",
            ROOT / section / "LAB.ipynb",
            ROOT / section / "QUIZ.md",
        ])
    required.extend([
        ROOT / "data" / "transactions.csv",
        ROOT / "data" / "customers.csv",
        ROOT / "data" / "products.json",
        ROOT / "07-guided-project" / "project" / "pipeline.py",
        ROOT / "07-guided-project" / "project" / "run_config.json",
        ROOT / "07-guided-project" / "project" / "validation" / "expected_metrics.json",
    ])
    missing = [str(path.relative_to(ROOT)) for path in required if not path.is_file()]
    assert not missing, f"Missing files: {missing}"

    for section in SECTIONS:
        validate_notebook(ROOT / section / "LAB.ipynb")

    schema_lab = (ROOT / "02-reading-data" / "LAB.ipynb").read_text(encoding="utf-8")
    assert "inferSchema" in schema_lab and "StructType" in schema_lab

    ast.parse((ROOT / "07-guided-project" / "project" / "pipeline.py").read_text(encoding="utf-8"), filename="pipeline.py")
    json.loads((ROOT / "07-guided-project" / "project" / "run_config.json").read_text(encoding="utf-8"))
    json.loads((ROOT / "07-guided-project" / "project" / "validation" / "expected_metrics.json").read_text(encoding="utf-8"))

    assert row_count(ROOT / "data" / "transactions.csv") == 58
    source = ROOT / "07-guided-project" / "project" / "source"
    assert row_count(source / "run-001" / "transactions.csv") == 12
    assert sum(1 for line in (source / "run-002" / "transactions.json").read_text().splitlines() if line) == 10
    assert row_count(source / "run-003" / "transactions.tsv", "\t") == 9

    banned = re.compile(
        r"participant exercise|topic-wise learner|trainer copy|trainer-only|"
        r"useful for teaching|small teaching sample|ask learners",
        re.IGNORECASE,
    )
    checked = list(ROOT.rglob("*.md")) + list(ROOT.glob("0*-*/LAB.ipynb"))
    violations = [str(path.relative_to(ROOT)) for path in checked if banned.search(path.read_text(encoding="utf-8"))]
    assert not violations, f"Non-student-facing wording found in: {violations}"

    print("COURSE PACKAGE PASS: section notes, labs, quizzes, data and project files are valid")


if __name__ == "__main__":
    main()

