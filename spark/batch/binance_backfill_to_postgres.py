# Bu batch job, Binance API’den belirli bir tarih aralığı için 1 dakikalık mum (kline) verilerini çekerek doğrudan Postgres veritabanına yüklemek amacıyla kullanılır.

import argparse
import requests
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# Binance kline endpoint’i ve API’nin izin verdiği maksimum veri çekme limiti tanımlanır.
BINANCE_URL = "https://api.binance.com/api/v3/klines"
LIMIT = 1000

# Belirtilen sembol ve zaman aralığı için Binance API’den kline verilerini çeken yardımcı fonksiyon.
def fetch_klines(symbol, interval, start_ms, end_ms):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT
    }
    r = requests.get(BINANCE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# Python datetime objesini Binance API’nin beklediği milisaniye formatına dönüştürür.
def to_ms(dt):
    return int(dt.timestamp() * 1000)

# Script, komut satırından sembol ve tarih aralığı alacak şekilde başlatılır.
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--start-date", required=True)  # YYYY-MM-DD
    parser.add_argument("--end-date", required=True)    # YYYY-MM-DD
    args = parser.parse_args()

    # Batch backfill işlemini çalıştırmak için Spark session oluşturulur.
    spark = (
        SparkSession.builder
        .appName("Binance2025Backfill")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Postgres’e yazılacak kline verisinin kolon tipleri şema olarak tanımlanır.
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("open_time", TimestampType(), False),
        StructField("open_price", DoubleType()),
        StructField("high_price", DoubleType()),
        StructField("low_price", DoubleType()),
        StructField("close_price", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("close_time", TimestampType(), False),
    ])

    # Kullanıcıdan alınan başlangıç ve bitiş tarihleri datetime formatına çevrilir.
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Binance API limitlerine takılmamak için zaman aralığı parça parça ilerletilir.
    current = start_dt

    while current < end_dt:
        next_dt = current + timedelta(minutes=LIMIT)

        # İlgili zaman dilimi için Binance’tan kline verileri çekilir.
        klines = fetch_klines(
            args.symbol,
            args.interval,
            to_ms(current),
            to_ms(min(next_dt, end_dt))
        )

        if klines:
            rows = []
            # API’den gelen ham veriler Spark DataFrame’e yazılacak satırlara dönüştürülür.
            for k in klines:
                rows.append((
                    args.symbol,
                    datetime.fromtimestamp(k[0] / 1000),
                    float(k[1]),
                    float(k[2]),
                    float(k[3]),
                    float(k[4]),
                    float(k[5]),
                    datetime.fromtimestamp(k[6] / 1000),
                ))

            # Toplanan satırlar Spark DataFrame’e dönüştürülür.
            df = spark.createDataFrame(rows, schema)

            # DataFrame, Postgres’teki hedef tabloya batch olarak eklenir.
            (
                df.write
                .format("jdbc")
                .option("url", "jdbc:postgresql://postgres:5432/crypto")
                .option("dbtable", "crypto_prices")
                .option("user", os.getenv("POSTGRES_USER"))
                .option("password", os.getenv("POSTGRES_PASSWORD"))
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

        # Bir sonraki zaman dilimine geçilir.
        current = next_dt

    # Backfill işlemi tamamlandıktan sonra Spark session kapatılır.
    spark.stop()
