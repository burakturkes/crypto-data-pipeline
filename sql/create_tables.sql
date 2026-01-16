-- Bu tablo, Binance’tan alınan kripto para mum (kline) verilerini zaman bazlı ve sembol bazlı olarak saklamak için oluşturuldu.
-- (symbol, open_time) birlikte primary key tanımlanarak duplicate veri girişini engelledim.

CREATE TABLE IF NOT EXISTS crypto_prices (
    symbol TEXT NOT NULL,
    open_time TIMESTAMP NOT NULL,
    close_time TIMESTAMP NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (symbol, open_time)
);