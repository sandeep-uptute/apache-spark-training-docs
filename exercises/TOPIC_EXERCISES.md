# Topic-wise learner exercises

Use the supplied `data/` files for exercises 1–6. Work in the corresponding numbered notebook or create `notebooks/my_exercises.ipynb`. Do not open the solutions until both learners have compared approaches.

## Exercise 1 — Spark execution model

**Goal:** Identify lazy transformations, actions and partitions.

1. Create `spark.range(0, 20, 1, 4)`.
2. Add `square = id * id` and filter to even `id` values.
3. Before executing, predict the row count and first five results.
4. Print the partition count.
5. Trigger one action with `show()` and another with `count()`.
6. Explain why these are two actions even though they use the same logical DataFrame.

**Pass evidence:** 10 rows, four starting partitions, and a correct transformation/action classification.

## Exercise 2 — Read several formats

**Goal:** Load CSV, JSON and TSV while understanding schemas.

1. Read `transactions.csv` with a header.
2. Read `products.json` and `products.tsv`.
3. Compare the two product schemas and counts.
4. Read `customers.csv` and display only `customer_id` and `city`.
5. Write products to `output/ex02_products_parquet`, then read them back.
6. Explain why the Parquet path contains part files rather than one file.

**Pass evidence:** Both product inputs have 12 rows; Parquet round-trip also has 12.

## Exercise 3 — Clean and classify transactions

**Goal:** Use safe types and make rejected data observable.

Starting from `transactions.csv`:

1. Remove duplicate `transaction_id` values.
2. Create `order_date`, `qty` and `price` with `try_cast`.
3. Define invalid records as bad date, null/non-positive quantity, null/negative price, missing customer ID or missing product ID.
4. Split valid and invalid rows.
5. Add `line_total` to valid rows.
6. Show invalid transaction IDs and a reason.

**Pass evidence:** 58 raw → 57 unique → 50 valid and 7 quality-invalid rows.

## Exercise 4 — Aggregation and Spark SQL

**Goal:** Reconcile a business metric using two APIs.

Using clean records and the product reference:

1. Keep `COMPLETED` rows.
2. Inner join products so only known product IDs remain.
3. Produce category-level orders, units and revenue with DataFrame operations.
4. Register the joined DataFrame as a temporary view.
5. Produce the same result in Spark SQL.
6. Compare both result sets and the total revenue.

**Pass evidence:** Four categories and total completed reference-valid revenue of `342228.00` for the foundation dataset.

## Exercise 5 — Diagnose joins and publish output

**Goal:** Keep missing reference keys visible before publishing.

1. Find transaction rows with unknown product IDs using `left_anti`.
2. Find transaction rows with unknown customer IDs using `left_anti`.
3. Confirm product and customer keys are unique in their reference tables.
4. Join completed clean records to both dimensions.
5. Save the enriched result to Parquet.
6. Read it back and confirm the accepted row count.

**Pass evidence:** Unknown product `P999`, unknown customer `C999`, and 36 completed reference-valid rows.

## Exercise 6 — Plans and practical optimization

**Goal:** Recognize common plan features and avoid inappropriate optimization.

1. Build a category revenue aggregation and run `explain("formatted")`.
2. Locate an `Exchange` and explain why it exists.
3. Broadcast the product reference and inspect the join plan.
4. Cache the cleaned transaction DataFrame, materialize it, use it twice, then unpersist it.
5. Compare `repartition(4, "category")` with `coalesce(1)` conceptually.
6. List two reasons the 58-row dataset cannot prove a speed improvement.

**Pass evidence:** Learners correctly identify shuffle, broadcast and cache lifecycle.

## Exercise 7 — Project observation questions

Complete these while running the guided project:

1. Why is the landing schema all strings?
2. Why is an identical duplicate counted but not written to rejected data?
3. Why are cancelled and returned rows not quality rejects?
4. Why is run-002's accepted count three?
5. Which two reject stages exist?
6. What destination paths are overwritten during a run-002 replay?
7. Why is the visual report generated after reading Parquet?
8. Which production guarantees are still missing without Delta Lake or another transactional store?

