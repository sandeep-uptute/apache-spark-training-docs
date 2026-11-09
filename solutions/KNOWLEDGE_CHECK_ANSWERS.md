# Knowledge check — trainer answers

1. A transformation describes a new DataFrame, such as `filter`; an action requires a result or side effect, such as `count` or a write.
2. Spark runs locally on one computer with two worker threads. It is not a two-node cluster.
3. It stabilizes expected types, avoids repeated inference work and prevents source samples from changing the inferred contract.
4. Dirty text remains observable and can be safely cast and classified rather than failing during ingestion.
5. A quality-invalid row violates the data contract. A cancelled row can satisfy the contract but is excluded by the completed-sales business rule.
6. `try_cast` returns null on malformed input, allowing the pipeline to attach a reject reason; ordinary ANSI cast can raise an exception.
7. `left_anti`.
8. A transaction matches each duplicate dimension row, multiplying joined rows and any subsequent sum.
9. Records with the same grouping key must be brought to the same partition, requiring redistribution.
10. When one side is reliably small enough to send to executors and doing so avoids a larger shuffle join.
11. An action must materialize the cached plan.
12. It funnels output through one partition, reduces parallelism and can create a bottleneck or memory pressure.
13. The pipeline overwrites only the deterministic `run_id=...` destination partition, so replay replaces rather than appends that batch.
14. To verify and report the artifact actually published, not merely an in-memory intermediate.
15. Any two: atomic multi-file transactions, concurrent writer isolation, general updates/deletes, version history/time travel, transactional schema enforcement.
16. `10 - 1 = 9`; `9 - 2 = 7`; `7 - 2 non-completed - 2 reference-rejected = 3 accepted`.
17. Four: two quality rejects plus two reference rejects.
18. It is an extra copy removed during deduplication; the surviving identical row is valid. Duplicate count is an audit metric, not a rejection stage.

Suggested interpretation: 14–18 correct indicates the learner can proceed independently with the supplied quick reference; 10–13 calls for targeted review; below 10 calls for rerunning the relevant notebook sections.

