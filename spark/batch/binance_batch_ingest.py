# Bu batch script, Binance API’den geçmiş mum (kline) verilerini çekerek  analiz ve ileri batch işlemler için Parquet formatında saklamak amacıyla kullanılır.

import argparse
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

# Binance kline endpoint’i ve API’nin izin verdiği maksimum kayıt limiti tanımlanır.
BINANCE_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000

# Belirli bir zaman aralığı için Binance API’den kline verisini çekmek için yardımcı fonksiyon.
def fetch_klines(symbol, interval, start_ms, end_ms):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": MAX_LIMIT
    }
    r = requests.get(BINANCE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# Python datetime objesini Binance API’nin beklediği milisaniye formatına çevirir.
def to_ms(dt):
    return int(dt.timestamp() * 1000)

# Script, parametreler üzerinden sembol ve tarih aralığı alacak şekilde çalıştırılır.
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--start-date", required=True)  # YYYY-MM-DD
    parser.add_argument("--end-date", required=True)    # YYYY-MM-DD
    parser.add_argument("--output", default="data/parquet/binance")
    args = parser.parse_args()

    # Batch ingest işlemi için Spark session oluşturulur.
    spark = SparkSession.builder \
        .appName("BinanceBatchIngest") \
        .getOrCreate()

    # Kullanıcıdan alınan tarih parametreleri datetime formatına çevrilir.
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Binance API limitlerine takılmamak için veri parça parça çekilir.
    rows = []
    current = start_dt

    while current < end_dt:
        next_dt = current + timedelta(minutes=MAX_LIMIT)
        klines = fetch_klines(
            args.symbol,
            args.interval,
            to_ms(current),
            to_ms(min(next_dt, end_dt))
        )

        # API’den gelen her mum verisi Spark DataFrame’e yazılacak satırlara dönüştürülür.
        for k in klines:
            rows.append((
                args.symbol,
                datetime.fromtimestamp(k[0] / 1000),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                datetime.fromtimestamp(k[6] / 1000)
            ))

        current = next_dt

    # Parquet’e yazılacak verinin kolon tipleri açık şekilde tanımlanır.
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("open_time", TimestampType(), False),
        StructField("open_price", DoubleType(), False),
        StructField("high_price", DoubleType(), False),
        StructField("low_price", DoubleType(), False),
        StructField("close_price", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("close_time", TimestampType(), False),
    ])

    # API’den toplanan satırlar Spark DataFrame’e dönüştürülür.
    df = spark.createDataFrame(rows, schema)

    # Veri, sembol bazlı partition edilerek Parquet formatında saklanır.
    (
        df
        .repartition("symbol")
        .write
        .mode("overwrite")
        .parquet(f"{args.output}/symbol={args.symbol}")
    )

    # Batch ingest işlemi tamamlandıktan sonra Spark session kapatılır.
    spark.stop()
