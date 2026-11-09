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
    "03-dataframe-transformations",
    "04-aggregations-and-sql",
    "05-joins-and-parquet",
    "06-advanced-dataframes-and-quality",
    "07-pyspark-optimizations",
    "08-guided-project",
]
LAB_SECTIONS = [section for section in SECTIONS if section != "07-pyspark-optimizations"]


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
    assert sum("SparkSession.builder" in source for source in code_sources) == 1, f"Expected one Spark session: {path}"
    assert "SparkSession.builder" in code_sources[0], f"First code cell must create Spark: {path}"
    text = path.read_text(encoding="utf-8")
    if path.parent.name == "08-guided-project":
        activity_count = text.count("## Stage ")
        assert activity_count >= 5, f"Expected five project stages: {path}"
    else:
        activity_count = text.count("## Exercise ")
        assert activity_count >= 5, f"Expected five exercises: {path}"
    assert text.count("### Step ") >= 15, f"Expected step-by-step activities: {path}"
    paired_count = text.count("### Paired literal SQL solution") + text.count("### DataFrame API comparison")
    assert paired_count == activity_count, f"Every activity needs a paired DataFrame/SQL route: {path}"
    assert text.count("createOrReplaceTempView") >= activity_count, f"Temporary views missing from SQL routes: {path}"
    assert text.count("spark.sql") >= activity_count, f"Literal SQL missing from activity routes: {path}"


def validate_demo_notebook(path: Path) -> None:
    book = json.loads(path.read_text(encoding="utf-8"))
    assert book.get("nbformat") == 4 and book.get("cells"), path
    code_sources = []
    for index, cell in enumerate(book["cells"], start=1):
        if cell.get("cell_type") == "code":
            source = "".join(cell.get("source", []))
            code_sources.append(source)
            ast.parse(source, filename=f"{path.name}:cell{index}")
    assert sum("SparkSession.builder" in source for source in code_sources) == 1, path
    assert "SparkSession.builder" in code_sources[0], path
    text = path.read_text(encoding="utf-8")
    for required_term in ("repartition", "coalesce", "persist", "unpersist", "adaptive", "broadcast", "skew", "explain"):
        assert required_term in text, f"Missing optimization demonstration: {required_term}"
    assert "## Exercise " not in text and "### Step " not in text, f"Optimization section must not be a learner lab: {path}"


def main() -> None:
    required = [ROOT / "README.md", ROOT / "00-setup" / "README.md"]
    for section in SECTIONS:
        required.extend([
            ROOT / section / "README.md",
            ROOT / section / "QUIZ.md",
        ])
    for section in LAB_SECTIONS:
        required.append(ROOT / section / "LAB.ipynb")
    required.extend([
        ROOT / "07-pyspark-optimizations" / "GUIDED_DEMOS.ipynb",
        ROOT / "data" / "transactions.csv",
        ROOT / "data" / "customers.csv",
        ROOT / "data" / "products.json",
        ROOT / "08-guided-project" / "project" / "pipeline.py",
        ROOT / "08-guided-project" / "project" / "run_config.json",
        ROOT / "08-guided-project" / "project" / "validation" / "expected_metrics.json",
    ])
    missing = [str(path.relative_to(ROOT)) for path in required if not path.is_file()]
    assert not missing, f"Missing files: {missing}"

    for section in LAB_SECTIONS:
        validate_notebook(ROOT / section / "LAB.ipynb")
    validate_demo_notebook(ROOT / "07-pyspark-optimizations" / "GUIDED_DEMOS.ipynb")
    for section in SECTIONS:
        quiz = (ROOT / section / "QUIZ.md").read_text(encoding="utf-8")
        assert quiz.count("### MCQ ") >= 5, f"Expected at least five MCQs: {section}"

    optimization_notes = (ROOT / "07-pyspark-optimizations" / "README.md").read_text(encoding="utf-8").lower()
    for required_term in ("adaptive query execution", "cache", "persist", "coalesce", "repartition", "partition", "broadcast", "data skew"):
        assert required_term in optimization_notes, f"Missing optimization topic: {required_term}"

    schema_lab = (ROOT / "02-reading-data" / "LAB.ipynb").read_text(encoding="utf-8")
    assert "inferSchema" in schema_lab and "StructType" in schema_lab
    for section in SECTIONS[:3]:
        text = (ROOT / section / "LAB.ipynb").read_text(encoding="utf-8")
        assert ".groupBy(" not in text, f"Aggregation introduced too early: {section}"
    for section in SECTIONS[:4]:
        text = (ROOT / section / "LAB.ipynb").read_text(encoding="utf-8")
        assert ".join(" not in text, f"Join introduced too early: {section}"
    advanced_lab = (ROOT / "06-advanced-dataframes-and-quality" / "LAB.ipynb").read_text(encoding="utf-8")
    for required_term in ("unionByName", "intersect", "exceptAll", ".pivot(", "row_number", "dense_rank", "rowsBetween"):
        assert required_term in advanced_lab, f"Missing advanced operation: {required_term}"

    ast.parse((ROOT / "08-guided-project" / "project" / "pipeline.py").read_text(encoding="utf-8"), filename="pipeline.py")
    json.loads((ROOT / "08-guided-project" / "project" / "run_config.json").read_text(encoding="utf-8"))
    json.loads((ROOT / "08-guided-project" / "project" / "validation" / "expected_metrics.json").read_text(encoding="utf-8"))

    assert row_count(ROOT / "data" / "transactions.csv") == 58
    source = ROOT / "08-guided-project" / "project" / "source"
    assert row_count(source / "run-001" / "transactions.csv") == 12
    assert sum(1 for line in (source / "run-002" / "transactions.json").read_text().splitlines() if line) == 10
    assert row_count(source / "run-003" / "transactions.tsv", "\t") == 9

    banned = re.compile(
        r"participant exercise|topic-wise learner|trainer copy|trainer-only|"
        r"useful for teaching|small teaching sample|ask learners",
        re.IGNORECASE,
    )
    checked = list(ROOT.rglob("*.md")) + list(ROOT.glob("0*-*/LAB.ipynb")) + [ROOT / "07-pyspark-optimizations" / "GUIDED_DEMOS.ipynb"]
    violations = [str(path.relative_to(ROOT)) for path in checked if banned.search(path.read_text(encoding="utf-8"))]
    assert not violations, f"Non-student-facing wording found in: {violations}"

    print("COURSE PACKAGE PASS: section notes, paired labs, optimization demonstrations, quizzes, data and project files are valid")


if __name__ == "__main__":
    main()

