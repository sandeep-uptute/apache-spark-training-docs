#!/usr/bin/env python3
"""Direct PySpark verification for the course environment and foundation data."""

from pathlib import Path
import sys
import tempfile

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    import pyspark

    assert pyspark.__version__.startswith("4.1."), f"Expected Spark 4.1.x; got {pyspark.__version__}"
    spark = (SparkSession.builder.master("local[2]")
             .appName("PySpark-Course-Smoke-Test")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    transaction_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("transaction_date", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", StringType()),
        StructField("unit_price", StringType()),
        StructField("status", StringType()),
    ])
    customer_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("city", StringType()),
    ])
    product_schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("list_price", DoubleType()),
    ])
    try:
        raw = (spark.read.schema(transaction_schema).option("header", True)
               .csv(str(ROOT / "data" / "transactions.csv")))
        customers = (spark.read.schema(customer_schema).option("header", True)
                     .csv(str(ROOT / "data" / "customers.csv")))
        products = spark.read.schema(product_schema).json(str(ROOT / "data" / "products.json"))
        assert raw.count() == 58

        typed = (raw.dropDuplicates(["transaction_id"])
                 .withColumn("order_date", F.expr("try_cast(transaction_date as date)"))
                 .withColumn("qty", F.expr("try_cast(quantity as int)"))
                 .withColumn("price", F.expr("try_cast(unit_price as decimal(12,2))")))
        clean = (typed.filter(
            F.col("order_date").isNotNull()
            & F.col("qty").isNotNull() & (F.col("qty") > 0)
            & F.col("price").isNotNull() & (F.col("price") >= 0)
            & (F.length(F.trim("customer_id")) > 0)
            & (F.length(F.trim("product_id")) > 0))
            .withColumn("line_total", F.col("qty") * F.col("price")))
        assert clean.count() == 50

        enriched = (clean.filter(F.col("status") == "COMPLETED")
                    .join(customers, "customer_id", "inner")
                    .join(products, "product_id", "inner"))
        assert enriched.count() == 36
        report = enriched.groupBy("category").agg(F.sum("line_total").alias("revenue"))
        assert str(report.agg(F.sum("revenue")).first()[0]) == "342228.00"
        with tempfile.TemporaryDirectory(prefix="pyspark_course_") as temp:
            destination = str(Path(temp) / "report_parquet")
            report.write.mode("overwrite").parquet(destination)
            assert spark.read.parquet(destination).count() == 4
        print("ENVIRONMENT PASS", {"python": sys.version.split()[0], "spark": spark.version})
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

