# Section 07 project review

[← Project notes](README.md) · [Course syllabus](../README.md)

1. Why are landing fields initially read as strings?
2. Why is a duplicate counted separately from rejected rows?
3. Why are cancelled and returned transactions not quality rejects?
4. What is the difference between `QUALITY` and `REFERENCE` rejection stages?
5. Why are customer and product tables suitable broadcast candidates?
6. What paths are replaced when run-002 is repeated?
7. Why are reports generated after reading Parquet back from the destination?
8. Reconcile run-002: `raw=10`, `duplicates=1`, `quality rejects=2`, `non-completed=2`, `reference rejects=2`, `accepted=3`.
9. What does this file-based replay design guarantee?
10. Which transactional or operational guarantees would still be required in a production platform?

