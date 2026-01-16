# Batch job: Parquet -> PostgreSQL
# Inserts only NEW records (idempotent load)
# Comments in English as requested

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ParquetToPostgresBatch")
    .getOrCreate()
)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
PARQUET_PATH = "/opt/spark/work-dir/data/parquet/binance/symbol=BTCUSDT"

PG_URL = "jdbc:postgresql://postgres:5432/crypto"
PG_TABLE = "crypto_prices"
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DRIVER = "org.postgresql.Driver"

# --------------------------------------------------
# READ PARQUET (HISTORICAL DATA)
# --------------------------------------------------
parquet_df = spark.read.parquet(PARQUET_PATH)

# --------------------------------------------------
# READ EXISTING DATA FROM POSTGRES (ONLY KEYS)
# --------------------------------------------------
existing_df = (
    spark.read
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", PG_DRIVER)
    .load()
    .select("symbol", "open_time")
)

# --------------------------------------------------
# REMOVE DUPLICATES (ANTI JOIN)
# --------------------------------------------------
new_rows_df = (
    parquet_df.alias("p")
    .join(
        existing_df.alias("e"),
        on=[
            col("p.symbol") == col("e.symbol"),
            col("p.open_time") == col("e.open_time")
        ],
        how="left_anti"
    )
)

# --------------------------------------------------
# WRITE ONLY NEW RECORDS
# --------------------------------------------------
(
    new_rows_df
    .write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASSWORD)
    .option("driver", PG_DRIVER)
    .mode("append")
    .save()
)

spark.stop()
