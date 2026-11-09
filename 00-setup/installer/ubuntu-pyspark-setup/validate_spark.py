"""Local integration test for PySpark 4.1.4, including filesystem I/O."""
import json
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("salary", IntegerType(), False),
])


def main():
    spark = (
        SparkSession.builder
        .appName("Ubuntu-PySpark-ReadWrite-Test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        assert spark.version == "4.1.4", spark.version
        with tempfile.TemporaryDirectory(prefix="pyspark-io-") as temp:
            root = Path(temp)
            inputs = root / "inputs"
            outputs = root / "outputs"
            inputs.mkdir()
            outputs.mkdir()

            # Native Python values -> DataFrame
            records = [(101, "Alice", 50000), (102, "Bob", 65000), (103, "Charlie", 80000)]
            df = spark.createDataFrame(records, schema=schema)
            assert df.count() == 3
            assert df.filter(col("salary") > 55000).count() == 2
            print("[PASS] Python variables -> DataFrame and transformations")

            (inputs / "employees.csv").write_text(
                "emp_id,name,salary\n101,Alice,50000\n102,Bob,65000\n", encoding="utf-8"
            )
            (inputs / "employees.tsv").write_text(
                "emp_id\tname\tsalary\n101\tAlice\t50000\n102\tBob\t65000\n", encoding="utf-8"
            )
            (inputs / "employees.json").write_text(
                "\n".join(json.dumps({"emp_id": x, "name": y, "salary": z}) for x, y, z in records[:2]) + "\n",
                encoding="utf-8",
            )
            csv_df = spark.read.option("header", True).schema(schema).csv(str(inputs / "employees.csv"))
            tsv_df = spark.read.option("header", True).option("sep", "\t").schema(schema).csv(str(inputs / "employees.tsv"))
            json_df = spark.read.schema(schema).json(str(inputs / "employees.json"))
            assert [x.count() for x in (csv_df, tsv_df, json_df)] == [2, 2, 2]
            print("[PASS] Read CSV, TSV and newline-delimited JSON")

            # Spark creates directories with part-files, rather than one file per output.
            destinations = {
                "csv": outputs / "csv",
                "tsv": outputs / "tsv",
                "json": outputs / "json",
                "parquet": outputs / "parquet",
            }
            df.write.mode("overwrite").option("header", True).csv(str(destinations["csv"]))
            df.write.mode("overwrite").option("header", True).option("sep", "\t").csv(str(destinations["tsv"]))
            df.write.mode("overwrite").json(str(destinations["json"]))
            df.write.mode("overwrite").parquet(str(destinations["parquet"]))
            assert spark.read.option("header", True).csv(str(destinations["csv"])).count() == 3
            assert spark.read.option("header", True).option("sep", "\t").csv(str(destinations["tsv"])).count() == 3
            assert spark.read.json(str(destinations["json"])).count() == 3
            assert spark.read.parquet(str(destinations["parquet"])).count() == 3
            for name, folder in destinations.items():
                assert any(p.name.startswith("part-") for p in folder.iterdir()), name
            print("[PASS] Write and read back CSV, TSV, JSON and Parquet")

            # Multiline JSON documents require a separate reader option.
            (inputs / "multiline.json").write_text(
                json.dumps([{"id": 1}, {"id": 2}], indent=2), encoding="utf-8"
            )
            assert spark.read.option("multiLine", True).json(str(inputs / "multiline.json")).count() == 2
            print("[PASS] Multiline JSON")
    finally:
        spark.stop()
    print("SUCCESS: ALL PYSPARK TESTS PASSED")


if __name__ == "__main__":
    main()
