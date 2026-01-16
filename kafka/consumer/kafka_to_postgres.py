# Bu streaming job, Kafka’ya gelen canlı kripto para verilerini okuyarak
# gerçek zamanlı şekilde Postgres veritabanına yazmak için kullanılır.

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *
import os

# Streaming job’unu çalıştırmak için Spark session oluşturulur.
spark = (
    SparkSession.builder
    .appName("KafkaToPostgresStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Kafka’dan gelen JSON mesajlarının beklenen yapısı şema olarak tanımlanır.
schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("open_time", StringType(), False),
    StructField("open_price", DoubleType()),
    StructField("high_price", DoubleType()),
    StructField("low_price", DoubleType()),
    StructField("close_price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("close_time", StringType(), False)
])

# Kafka topic’inden yalnızca yeni gelen mesajları okuyacak şekilde streaming kaynak oluşturulur.
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "crypto_prices")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 1000)
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/crypto_stream")
    .load()
)

# Kafka’dan gelen JSON payload, tanımlı şemaya göre parse edilerek kolonlara ayrılır.
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select(
          col("data.symbol"),
          to_timestamp(col("data.open_time")).alias("open_time"),
          col("data.open_price"),
          col("data.high_price"),
          col("data.low_price"),
          col("data.close_price"),
          col("data.volume"),
          to_timestamp(col("data.close_time")).alias("close_time")
      )
)

# Her micro-batch’te duplicate kayıtlar filtrelenerek veri Postgres’e yazılır.
def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .dropDuplicates(["symbol", "open_time"])
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/crypto")
        .option("dbtable", "crypto_prices")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

# Streaming job başlatılır ve Kafka’dan gelen canlı veriler sürekli olarak işlenir.
query = (
    parsed.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()
