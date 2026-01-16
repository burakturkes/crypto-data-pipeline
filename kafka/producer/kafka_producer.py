from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("ParquetToKafkaProducer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "crypto_prices"

PARQUET_PATH = "/opt/spark/work-dir/data/parquet/binance/symbol=BTCUSDT"

# -------------------------------------------------
# READ PARQUET (BATCH)
# -------------------------------------------------
df = spark.read.parquet(PARQUET_PATH)

print(f"ROW COUNT: {df.count()}")

# -------------------------------------------------
# Kafka expects: key (optional), value (binary)
# -------------------------------------------------
kafka_df = (
    df
    .select(
        to_json(
            struct(
                col("symbol"),
                col("open_time"),
                col("open_price"),
                col("high_price"),
                col("low_price"),
                col("close_price"),
                col("volume"),
                col("close_time")
            )
        ).alias("value")
    )
)

# -------------------------------------------------
# WRITE TO KAFKA (BATCH)
# -------------------------------------------------
(
    kafka_df
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", KAFKA_TOPIC)
    .save()
)

print("? Data successfully sent to Kafka")

spark.stop()
