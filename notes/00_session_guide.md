# Session Guide — Apache Spark Fundamentals

> Superseded for final delivery by [`../DELIVERY_GUIDE.md`](../DELIVERY_GUIDE.md), which uses the confirmed 6:30 AM–2:30 PM schedule and a 150-minute three-run project. This file remains as the original draft.

## Outcomes

At the end of the workshop, participants will be able to create a local SparkSession, load CSV/JSON/TSV and Parquet data, use DataFrame transformations, handle common quality issues, group and query data with Spark SQL, join references, save a Parquet report, and explain why shuffles and caching matter.

## Proposed learning journey

```mermaid
flowchart LR
 A[Why Spark?] --> B[SparkSession and files]
 B --> C[Transform and validate]
 C --> D[Aggregate with SQL]
 D --> E[Join reference data]
 E --> F[Explain execution]
 F --> G[Deliver sales pipeline]
```

The modules share an e-commerce dataset. Each numbered notebook can be opened on its own; later notebooks repeat a small amount of setup so no prior notebook state is necessary. The project starter intentionally contains TODO cells and is designed to be attempted by participants.

## Timetable: 9:30 AM – 5:30 PM

| Slot | Work | Duration | Main artifact |
|---|---|---:|---|
| 9:30–9:45 | Orientation and Jupyter smoke check | 15m | 00 preflight |
| 9:45–10:15 | Spark architecture / partitions / actions | 30m | Note 01 + Notebook 01 |
| 10:15–11:00 | SparkSession / CSV / TSV / JSON / Parquet | 45m | Note 02 + Notebook 02 |
| 11:00–11:15 | Tea break | 15m | — |
| 11:15–12:30 | DataFrames / casting / nulls / deduplication | 75m | Note 03 + Notebook 03 |
| 12:30–1:30 | Aggregations / temporary views / SQL | 60m | Note 04 + Notebook 04 |
| 1:30–2:15 | Lunch | 45m | — |
| 2:15–3:15 | Joins / unmatched reference data / Parquet writes | 60m | Note 05 + Notebook 05 |
| 3:15–3:45 | Lazy execution / shuffles / cache / plans | 30m | Note 06 + Notebook 06 |
| 3:45–4:00 | Tea break | 15m | — |
| 4:00–5:00 | E-commerce guided mini-project | 60m | Note 07 + Notebook 07 |
| 5:00–5:15 | Review exact output and answers | 15m | Notebook 08 |
| 5:15–5:30 | Knowledge check / questions / closing | 15m | Trainer runbook |

## Simple teaching rhythm

**Concept → show an example → run a cell → ask attendees to modify it → verify an observable result.** Prefer compact notebook cells. In modules 3–5, spend more time performing exercises than explaining APIs.

## Practical checkpoints

| Checkpoint | Observable pass condition |
|---|---|
| Environment | Spark 4.1.x starts with `local[2]`; Parquet round-trip succeeds |
| After module 2 | 58 transaction rows, 12 product rows, Parquet round-trip 12 |
| After module 3 | 57 unique IDs, 7 malformed records after dedup, 50 clean |
| After module 4 | 36 completed reference-valid rows, revenue 342228.00 |
| After module 5 | One unmatched product and one unmatched customer; report saved |
| After module 6 | Explain plan displayed; students identify groupBy/shuffle |
| After module 7 | Four-category revenue summary written and validated |

## Scope boundaries

This is a **fundamentals** workshop. Discuss advanced topics only briefly: cluster managers, Spark Connect, streaming, UDFs, bucketing, adaptive query execution, and enterprise resource sizing. Avoid turning the workshop into a Hadoop installation or a distributed-cluster administration lab.
