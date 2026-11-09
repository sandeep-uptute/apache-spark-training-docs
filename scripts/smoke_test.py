#!/usr/bin/env python3
"""Run from the project root with the same interpreter as Jupyter's kernel."""
import os, sys, tempfile
from pathlib import Path

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT))
from training_lib import create_spark, read_raw, clean_transactions, completed_enriched, revenue_by_category


def main():
    import pyspark
    assert pyspark.__version__.startswith('4.1.'), f'Expected Spark 4.1.x; got {pyspark.__version__}'
    print('Python:', sys.version.split()[0], 'PySpark:', pyspark.__version__)
    spark = create_spark('TG117-Workshop-Smoke-Test')
    try:
        print('Spark:',spark.version, 'master:',spark.sparkContext.master)
        tx,cust,prod=read_raw(spark,PROJECT)
        assert tx.count()==58
        clean=clean_transactions(tx)
        assert clean.count()==50
        enriched=completed_enriched(clean,cust,prod)
        assert enriched.count()==36
        report=revenue_by_category(enriched)
        assert str(report.agg({'revenue':'sum'}).first()[0])=='342228.00'
        with tempfile.TemporaryDirectory(prefix='tg117_spark_test_') as tmp:
            dst=str(Path(tmp)/'report_parquet')
            report.write.mode('overwrite').parquet(dst)
            assert spark.read.parquet(dst).count()==4
        print('PASS: CSV, JSON, transformations, joins, aggregation, Parquet read/write.')
    finally:
        spark.stop()


if __name__=='__main__':main()
