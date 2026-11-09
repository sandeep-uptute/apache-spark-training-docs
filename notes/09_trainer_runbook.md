# Trainer Runbook — 8-Hour Apache Spark Fundamentals

> Superseded for final delivery by [`../DELIVERY_GUIDE.md`](../DELIVERY_GUIDE.md). The final guide is section-based and reflects two learners, cloned environments, one hour of breaks, and the expanded project.

**Training stack:** Ubuntu 22.04 LTS (assumed), OpenJDK 17, Python 3.12.3, Spark/PySpark 4.1.x, local Spark + Jupyter.  
**Materials:** Topic-wise Markdown notes (presentation + reference), notebooks, CSV/JSON/TSV, deterministic expected metrics.  
**Session:** 9:30 AM–5:30 PM, 75 minutes breaks, 405 minutes learning/validation/closing.

## Readiness checklist (finish before attendees arrive)

- [ ] Confirm OS and versions using `notes/00_ubuntu_preflight.md`.
- [ ] Activate the same environment that Jupyter's selected kernel uses.
- [ ] Run `python scripts/validate_package.py` and `python scripts/smoke_test.py`; record PASS.
- [ ] Open and execute the first cells of notebooks 01–06 once on the presenter machine.
- [ ] Keep an offline copy of the workshop zip and a backup working laptop/environment.
- [ ] Check the Markdown renderer supports Mermaid or have a fallback image-free explanation ready.
- [ ] Use a legible terminal/notebook font and test screen share/projector output.
- [ ] Ensure attendee machines have enough memory and that local file permissions allow writes to `output/`.

## Module-by-module teaching sequence

### 9:30–9:45 — Welcome + verification (15m)

- **0–5:** Set outcome: ingest → clean → enrich → summarize → export.
- **5–10:** Ask participants to show `spark.version`, `sys.version`, `spark.sparkContext.master`.
- **10–15:** Validate the included data folder and explain the one shared project.
- **Recovery:** If someone's Spark fails to launch, pair them with a working attendee rather than use the instructional slot for installations.

### 9:45–10:15 — Module 01: Introduction (30m)

- **0–8:** Why Spark is useful and when not to use it.
- **8–15:** Driver, executors, tasks and partitions; local mode nuance.
- **15–25:** Notebook 01: range/partitions/derived column/actions.
- **25–30:** Ask learners to identify two transformations and two actions.
- **Likely question:** Does Spark automatically run on several computers? **Answer:** No. `local[2]` executes on a single computer; a cluster manager is needed for multiple machines.

### 10:15–11:00 — Module 02: Getting Started (45m)

- **0–8:** SparkSession and kernels, `local[2]`.
- **8–18:** Read transactions CSV; inspect schema and preview.
- **18–28:** Read products JSON and TSV; discuss source formats.
- **28–38:** Parquet write/read round-trip using `output/`.
- **38–45:** Learners read customers CSV and validate counts.
- **Likely question:** Why is a Parquet path a folder? **Answer:** Spark writes datasets composed of partition files and metadata.

### 11:00–11:15 — Tea break

### 11:15–12:30 — Module 03: DataFrame Fundamentals (75m)

- **0–10:** Select/filter and immutable DataFrames.
- **10–25:** `withColumn`, safe casts, DECIMAL and intentionally malformed strings.
- **25–40:** Duplicates, null checking and clean record rules.
- **40–65:** Participant exercise: filter, status grouping, reject report and count assertions.
- **65–75:** Explain why valid-but-unknown reference IDs are handled later.
- **Must hit:** `58 → 57 → 50`.
- **Likely question:** Why not fill all nulls with zero? **Answer:** Zero may invent valid business activity and corrupt revenue.

### 12:30–1:30 — Module 04: Aggregations and Spark SQL (60m)

- **0–12:** Clarify the revenue business definition and eligible records.
- **12–25:** DataFrame `groupBy`, `sum`, `countDistinct`.
- **25–40:** `createOrReplaceTempView` and equivalent SQL.
- **40–52:** Participant challenge: daily revenue or city-level grouping.
- **52–60:** Reconcile total 342228.00 and four categories.
- **Likely question:** Is Spark SQL slower than the DataFrame API? **Answer:** Equivalent queries can share the same optimizer and plan.

### 1:30–2:15 — Lunch break

### 2:15–3:15 — Module 05: Joining and Saving (60m)

- **0–12:** Explain transaction fact, product dimension and customer dimension.
- **12–28:** Inner vs left vs anti joins, inspect `P999`, `C999`.
- **28–45:** Perform both dimension joins and save Parquet/CSV outputs.
- **45–55:** Participant exercise: read back output; show unmatched IDs.
- **55–60:** Explain join multiplication risks from duplicate dimension keys.
- **Must hit:** 36 reference-valid completed records.

### 3:15–3:45 — Module 06: Execution and Performance (30m)

- **0–8:** Lazy evaluation; transformation vs action.
- **8–18:** `explain('formatted')`, exchanges, wide vs narrow.
- **18–26:** Partition count, cache and `unpersist` demonstration.
- **26–30:** Explain why performance benchmarking needs realistic data size.
- **Likely question:** Should we cache every DataFrame? **Answer:** No. Cache only when reuse outweighs memory and materialization cost.

### 3:45–4:00 — Tea break

### 4:00–5:00 — Module 07: Guided project (60m)

- **0–10:** Read sources and inspect data.
- **10–25:** Deduplicate, parse and reject invalid rows.
- **25–40:** Filter completed and match both dimensions.
- **40–50:** Produce category-level report and export to Parquet.
- **50–60:** Run assertions and self-check.
- **Facilitation tip:** Start with `07_guided_project_starter.ipynb`; reveal the reference solution only after effort.

### 5:00–5:15 — Project validation (15m)

- Open Notebook 08 to compare key steps and expected outcomes.
- Verify `58 raw / 57 unique / 50 clean / 36 completed / 4 categories / 342228.00 revenue`.
- Ask what data quality checks they would monitor in production.

### 5:15–5:30 — Knowledge check and Q&A (15m)

Ask five short questions: (1) what triggers a job? (2) why does `try_cast` help with messy input? (3) how do we find orphan keys? (4) why does a `groupBy` commonly shuffle? (5) why should we not force a single output file? Close by pointing to notes and complete reference notebook.

## Failure modes and contingency plan

| Issue | Trainer response |
|---|---|
| Kernel uses different Python than shell | Show `sys.executable`; switch kernel or restart Jupyter from activated venv |
| Spark fails to start | Validate Java 17, `JAVA_HOME`, hostname/IP, then use a known-good presenter machine |
| Unsupported cast causes exception | Use the supplied `try_cast` helper; avoid plain `cast()` for malformed values |
| One notebook has stale variables | Restart kernel and run from first cell; each notebook has its own setup |
| Report total differs | Check status filter, duplicate removal, missing dimension references, decimal casts and join keys |
| Several Spark sessions cause confusion | Close idle kernels; `.getOrCreate()` reuses the current session in the same kernel |
| Time slips by 15 minutes | Shorten the module-03 extra exercises and module-04 city challenge; **retain** the guided project and break allocation |
| Mermaid diagrams do not render | Use text below each diagram and the notebook demonstration; don't troubleshoot Markdown plugins mid-session |

## Business and technical blind spots

1. **Definition of revenue:** We only report completed, dimension-matched records. The exercise is not a general ledger or refund reconciliation system.
2. **Duplicate IDs:** Dataset contains an identical duplicate. Conflicting duplicates in production require deterministic sequencing.
3. **DataFrame joins:** If reference keys are duplicated, revenue can inflate. Validate dimension uniqueness.
4. **Local execution:** `local[2]` is not a multi-node cluster; explain the distributed model without misleading attendees.
5. **Typed data:** Spark 4.1 ANSI behavior makes safe casting important for dirty CSV fields.
6. **Spark output semantics:** CSV and Parquet are dataset directories; writes are actions and overwrite replaces the destination.
7. **Caching:** The examples illustrate concepts, not measurable speed benefits with 58 rows.
8. **Notebook isolation:** Each notebook can start by itself; if students run many kernels at once, local resources become a constraint.
9. **Data privacy:** All workshop data is synthetic; avoid loading participant/customer production data into shared training environments.
10. **Version mixups:** The Spark tarball version and pip-package version can diverge. Inspect the real notebook kernel rather than assuming installed versions.

## Post-session outcomes

Participants should retain their completed notebook and Parquet summary, be able to explain the transformation flow, and have a reproducible base for further SQL/PySpark practice.
