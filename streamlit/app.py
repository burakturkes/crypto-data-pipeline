# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import psycopg2
import altair as alt
import time
import re
import requests
import os

# ==================================================
# PAGE CONFIG
# ==================================================
st.set_page_config(
    page_title="BTCUSDT Dashboard",
    layout="wide"
)

st.title("📈 BTCUSDT Price Dashboard")

# ==================================================
# CHATBOT CONFIG
# ==================================================
LLM_ENABLED = False  # Demo icin LLM KAPALI

# ==================================================
# SIDEBAR
# ==================================================
st.sidebar.header("⏱ Zaman Filtresi")

time_options = {
    "Son 1 Gün": "1 day",
    "Son 7 Gün": "7 days",
    "Son 1 Ay": "1 month",
    "Son 3 Ay": "3 months",
    "Son 6 Ay": "6 months",
    "Son 1 Yıl": "1 year",
}

selected_label = st.sidebar.selectbox(
    "Gösterilecek zaman aralığı",
    list(time_options.keys())
)

interval_sql = time_options[selected_label]

auto_refresh = st.sidebar.selectbox(
    "Otomatik yenileme",
    [
        ("Kapalı", 0),
        ("1 dakika", 1),
        ("5 dakika", 5)
    ],
    format_func=lambda x: x[0]
)

# ==================================================
# DATABASE (Dashboard)
# ==================================================
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="crypto",
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

query = f"""
SELECT
    open_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM crypto_prices
WHERE open_time >= NOW() - INTERVAL '{interval_sql}'
ORDER BY open_time
"""

df = pd.read_sql(query, conn)
conn.close()

st.write(f"📊 Toplam kayıt ({selected_label}): **{len(df)}**")

if df.empty:
    st.warning("Seçilen zaman aralığında veri yok.")
    st.stop()

# ==================================================
# CHART
# ==================================================
y_min = df["close_price"].min() * 0.999
y_max = df["close_price"].max() * 1.001

chart = (
    alt.Chart(df)
    .mark_line(color="#4da3ff")
    .encode(
        x=alt.X("open_time:T", title="Zaman"),
        y=alt.Y(
            "close_price:Q",
            title="Kapanış Fiyatı (USDT)",
            scale=alt.Scale(domain=[y_min, y_max], zero=False)
        )
    )
    .properties(height=450)
)

st.altair_chart(chart, use_container_width=True)

# ==================================================
# TABLE
# ==================================================
st.subheader(f"{selected_label} BTCUSDT Verisi (Son 100 Kayıt)")

st.dataframe(
    df.sort_values("open_time", ascending=False).head(100),
    use_container_width=True
)

st.caption(f"⏱ Son veri zamanı: {df['open_time'].max()}")

# ==================================================
# AUTO REFRESH
# ==================================================
if auto_refresh[1] > 0:
    time.sleep(auto_refresh[1] * 60)
    st.rerun()

# ==================================================
# RULE-BASED SQL GENERATOR
# ==================================================
def _parse_interval(question: str) -> str | None:
    q = question.lower()

    if "son 1 saat" in q or "son bir saat" in q:
        return "1 hour"
    if "son 24 saat" in q or "son 1 gün" in q or "son bir gün" in q:
        return "1 day"
    if "son 7 gün" in q or "son bir hafta" in q:
        return "7 days"
    if "son 1 ay" in q or "son bir ay" in q:
        return "1 month"
    if "son 3 ay" in q:
        return "3 months"
    if "son 6 ay" in q:
        return "6 months"
    if "son 1 yıl" in q or "son bir yıl" in q:
        return "1 year"

    m = re.search(r"son\s+(\d+)\s*saat", q)
    if m:
        return f"{m.group(1)} hours"

    m = re.search(r"son\s+(\d+)\s*gün", q)
    if m:
        return f"{m.group(1)} days"

    return None


def generate_sql_local(question: str) -> str:
    q = question.lower()

    # --- Specific date: YYYY-MM-DD ---
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", q)
    if m:
        date_str = m.group(1)
        return f"""
SELECT open_time, close_price
FROM crypto_prices
WHERE open_time >= '{date_str}'
  AND open_time < '{date_str}'::date + INTERVAL '1 day'
ORDER BY open_time DESC
LIMIT 1;
""".strip()

    interval = _parse_interval(question) or "1 day"
    where_clause = f"WHERE open_time >= NOW() - INTERVAL '{interval}'"

    if "ortalama" in q:
        return f"""
SELECT ROUND(AVG(close_price)::numeric, 4) AS avg_close_price
FROM crypto_prices
{where_clause};
""".strip()

    if "en yüksek" in q or "maksimum" in q:
        return f"""
SELECT MAX(high_price) AS max_price
FROM crypto_prices
{where_clause};
""".strip()

    if "en düşük" in q or "minimum" in q:
        return f"""
SELECT MIN(low_price) AS min_price
FROM crypto_prices
{where_clause};
""".strip()

    if "kaç kayıt" in q or "kayıt sayısı" in q:
        return f"""
SELECT COUNT(*) AS row_count
FROM crypto_prices
{where_clause};
""".strip()

    if "son fiyat" in q or "son kapanış" in q:
        return """
SELECT open_time, close_price
FROM crypto_prices
ORDER BY open_time DESC
LIMIT 1;
""".strip()

    if "yüzde" in q or "değişim" in q:
        return f"""
WITH t AS (
  SELECT open_time, close_price
  FROM crypto_prices
  {where_clause}
  ORDER BY open_time
),
x AS (
  SELECT
    (SELECT close_price FROM t ORDER BY open_time ASC LIMIT 1) AS first_price,
    (SELECT close_price FROM t ORDER BY open_time DESC LIMIT 1) AS last_price
)
SELECT
  first_price,
  last_price,
  ROUND(((last_price - first_price) / first_price) * 100, 4) AS pct_change
FROM x;
""".strip()

    return "-- Soru anlaşılamadı"

# ==================================================
# CHATBOT UI
# ==================================================
st.markdown("---")
st.subheader("💬 Veri Asistanı")

user_question = st.text_input(
    "BTCUSDT verisi hakkında bir soru sor:",
    placeholder="Örn: Son 1 saatte ortalama kapanış fiyatı ne?"
)

if user_question:
    sql_query = generate_sql_local(user_question)

    st.markdown("🧠 **Üretilen SQL sorgusu:**")
    st.code(sql_query, language="sql")

    if not sql_query.startswith("--"):
        try:
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                dbname="crypto",
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
            result_df = pd.read_sql(sql_query, conn)
            conn.close()

            st.markdown("📊 **Sorgu Sonucu:**")
            st.dataframe(result_df, use_container_width=True)

        except Exception as e:
            st.error(f"Sorgu çalıştırılamadı: {e}")
