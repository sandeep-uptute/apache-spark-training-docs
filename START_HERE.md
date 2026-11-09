# PySpark industry workshop — package index

This folder is a complete eight-hour instructor-led workshop for **two participants** using cloned Ubuntu environments. Installation is completed before the session. The active learning time is seven hours; breaks total one hour.

## Delivery order

1. Trainer reads [`DELIVERY_GUIDE.md`](DELIVERY_GUIDE.md) and completes [`SETUP_AND_TROUBLESHOOTING.md`](SETUP_AND_TROUBLESHOOTING.md).
2. Present from [`PRESENTATION.md`](PRESENTATION.md), using the numbered foundation notebooks for demonstrations.
3. Learners use [`LEARNER_HANDBOOK.md`](LEARNER_HANDBOOK.md) and [`exercises/TOPIC_EXERCISES.md`](exercises/TOPIC_EXERCISES.md).
4. Run the 150-minute guided project in [`project-notebooks/07_guided_project_starter.ipynb`](project-notebooks/07_guided_project_starter.ipynb).
5. Use [`project-notebooks/08_project_reference_solution.ipynb`](project-notebooks/08_project_reference_solution.ipynb) only for review.
6. Finish with [`KNOWLEDGE_CHECK.md`](KNOWLEDGE_CHECK.md) and give learners [`QUICK_REFERENCE.md`](QUICK_REFERENCE.md).

## Deliverables map

| Requested deliverable | File or folder |
|---|---|
| Trainer runbook and section flow | `DELIVERY_GUIDE.md` |
| Learner Markdown handbook | `LEARNER_HANDBOOK.md` |
| Topic exercises | `exercises/TOPIC_EXERCISES.md` |
| Foundation notebooks | `notebooks/01_...` through `06_...` |
| Guided project notebook | `project-notebooks/07_guided_project_starter.ipynb` |
| Solutions | `solutions/EXERCISE_SOLUTIONS.md`, project reference notebook |
| Sample datasets | `data/` and `project/source/` + `project/reference/` |
| Knowledge check | `KNOWLEDGE_CHECK.md` and trainer answers in `solutions/` |
| Setup and troubleshooting | `SETUP_AND_TROUBLESHOOTING.md` and `setup-installer/` |
| Printable quick reference | `QUICK_REFERENCE.md` |
| Markdown presentation | `PRESENTATION.md` |
| Project design and run contract | `PROJECT_GUIDE.md`, `project/run_config.json` |

## Target stack

- Ubuntu 22.04 LTS clone
- OpenJDK 17
- Python 3.12
- PySpark 4.1.4
- JupyterLab 4
- Local Spark master `local[2]`

The workshop uses only core PySpark and file-based storage. Delta Lake is intentionally out of scope.

## Completion evidence

After the project, these paths should exist:

```text
project/destination/
├── curated_sales/run_id=run-001/order_month=2026-10/*.parquet
├── curated_sales/run_id=run-002/order_month=2026-10/*.parquet
├── curated_sales/run_id=run-003/order_month=2026-10/*.parquet
├── rejected_records/run_id=run-002/reject_stage=QUALITY/*.parquet
├── rejected_records/run_id=run-002/reject_stage=REFERENCE/*.parquet
└── audit/run_id=run-NNN/*.parquet

project/reports/
├── run-001.html
├── run-002.html
├── run-003.html
└── all-runs.html
```

Expected combined result: **21 accepted completed sales** and **103,813.00 revenue units**.

