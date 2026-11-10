# Section 11 project review

[← Project notes](README.md) · [Course syllabus](../README.md)

Choose one answer for each question.

### MCQ 1

Why are shipment source fields initially read as strings?

- A. The pipeline can apply controlled conversions and retain malformed values for rejection
- B. Matplotlib requires every Spark field to be a string
- C. Parquet cannot store dates
- D. String fields remove duplicates automatically

### MCQ 2

Which shipments contribute to on-time delivery metrics?

- A. Every valid status
- B. Only delivered shipments with matched references
- C. Only cancelled shipments
- D. Every row with a promised date

### MCQ 3

Why does Spark aggregate before data is collected for Matplotlib?

- A. Matplotlib requires Parquet input
- B. Only compact summary data should be moved to the driver
- C. Spark cannot calculate averages
- D. Collection automatically fixes null values

### MCQ 4

What makes a shipment on time in this project?

- A. `delivered_date` is before or equal to `promised_date`
- B. `order_date` equals `delivered_date`
- C. Shipping cost is zero
- D. Package weight is below one kilogram

### MCQ 5

Why does each run overwrite only its own `run_id` destination path?

- A. It makes a repeated run idempotent without removing other runs
- B. It guarantees one Parquet file
- C. It changes JSON into CSV
- D. It prevents reference joins

## Review prompts

1. Explain why reports are generated from data read back from the destination.
2. Describe the difference between a quality rejection and a reference rejection.
