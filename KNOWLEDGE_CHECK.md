# Knowledge check

Complete without opening the solution. Suggested time: 10 minutes. Discuss answers as a pair.

## Questions

1. What is the difference between a Spark transformation and an action? Give one example of each.
2. In this workshop, what does `local[2]` mean?
3. Why can an explicit landing schema be safer than inference for a repeatable pipeline?
4. Why does the project initially read quantity and price as strings?
5. What is the difference between a quality-invalid transaction and a valid cancelled transaction?
6. Why is `try_cast` used instead of an ordinary cast for dirty fields?
7. Which join returns only left-side rows that have no match on the right?
8. How can duplicate product keys inflate reported revenue?
9. Why does `groupBy` commonly introduce an `Exchange` in the physical plan?
10. When is a broadcast join appropriate?
11. What must happen before a newly cached DataFrame actually occupies cache storage?
12. Why is `coalesce(1)` not a general output optimization?
13. What makes a run replay-safe in the guided project?
14. Why are the HTML reports built after reading the destination Parquet?
15. Name two guarantees the file-based design does not provide that a transactional table system could provide.

## Practical interpretation

Given run metrics:

```text
raw=10, duplicates=1, unique=9, quality_rejected=2,
valid=7, non_completed=2, reference_rejected=2, accepted=3
```

16. Reconcile the metrics in three equations.
17. How many records are written to the rejected destination?
18. Why is the duplicate not included in that rejected count?

## Self-rating

| Rating | Meaning |
|---|---|
| 3 | I can explain and implement it without notes |
| 2 | I can implement it with the quick reference |
| 1 | I recognize it but need a worked example |
| 0 | I need the concept explained again |

Rate yourself for: DataFrames, quality rules, aggregations, joins, Parquet, plans/optimization, and batch replay.

