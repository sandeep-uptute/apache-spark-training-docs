"""Shared PySpark helpers for the Spark 4.1 Ubuntu course.

The module intentionally uses the DataFrame API instead of pandas or Python UDFs.
"""
from pathlib import Path
import os
import sys


def project_root():
    """Support notebooks opened from the course root or a numbered section folder."""
    for base in (Path.cwd(), *Path.cwd().parents):
        if (base / 'data' / 'transactions.csv').is_file():
            return base
    raise FileNotFoundError('Could not find data/transactions.csv. Launch Jupyter in the course folder.')


def create_spark(name='PySpark-Course'):
    from pyspark.sql import SparkSession
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    return (SparkSession.builder
            .master('local[2]')
            .appName(name)
            .config('spark.sql.shuffle.partitions', '4')
            .config('spark.sql.adaptive.enabled', 'true')
            .config('spark.sql.session.timeZone', 'UTC')
            .getOrCreate())


def read_raw(spark, root=None):
    root = Path(root) if root else project_root()
    transactions = spark.read.option('header', True).csv(str(root / 'data' / 'transactions.csv'))
    customers = spark.read.option('header', True).csv(str(root / 'data' / 'customers.csv'))
    products = spark.read.json(str(root / 'data' / 'products.json'))
    return transactions, customers, products


def typed_transactions(raw):
    """Parse intentionally dirty fields safely, including bad numeric strings."""
    from pyspark.sql import functions as F
    return (raw.dropDuplicates(['transaction_id'])
            .withColumn('order_date', F.expr('try_cast(transaction_date as date)'))
            .withColumn('qty', F.expr('try_cast(quantity as int)'))
            .withColumn('price', F.expr('try_cast(unit_price as decimal(12,2))')))


def quality_condition():
    from pyspark.sql import functions as F
    return (F.col('order_date').isNotNull()
            & F.col('qty').isNotNull() & (F.col('qty') > 0)
            & F.col('price').isNotNull() & (F.col('price') >= 0)
            & F.col('customer_id').isNotNull() & (F.length(F.trim(F.col('customer_id'))) > 0)
            & F.col('product_id').isNotNull() & (F.length(F.trim(F.col('product_id'))) > 0))


def clean_transactions(raw):
    from pyspark.sql import functions as F
    return (typed_transactions(raw)
            .filter(quality_condition())
            .withColumn('line_total', F.col('qty') * F.col('price')))


def completed_enriched(clean, customers, products):
    """Only completed sales with matching customer and product reference records."""
    from pyspark.sql import functions as F
    return (clean.filter(F.col('status') == 'COMPLETED')
            .join(customers, 'customer_id', 'inner')
            .join(products, 'product_id', 'inner'))


def revenue_by_category(enriched):
    from pyspark.sql import functions as F
    return (enriched.groupBy('category')
            .agg(F.countDistinct('transaction_id').alias('orders'),
                 F.sum('line_total').alias('revenue'))
            .orderBy('category'))
