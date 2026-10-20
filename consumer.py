from kafka import KafkaConsumer
from psycopg2.extras import execute_values
import psycopg2
import json
import os
from datetime import datetime

consumer = KafkaConsumer(
    "crypto_ticks",
    bootstrap_servers="localhost:29092",
    group_id="postgres_latest_price",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    database=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"],
    host=os.environ["PGHOST"],
    port=os.getenv("PGPORT", "5432")
)

cur = conn.cursor()


cur.execute("""
CREATE TABLE IF NOT EXISTS crypto_latest_price (
    symbol VARCHAR(20) PRIMARY KEY,
    latest_price DOUBLE PRECISION,
    latest_volume DOUBLE PRECISION,
    event_time TIMESTAMP
);
""")

conn.commit()

BATCH_SIZE = 10
batch = []

query = """
INSERT INTO crypto_latest_price
(symbol, latest_price, latest_volume, event_time)
VALUES %s
ON CONFLICT (symbol)
DO UPDATE SET
    latest_price = EXCLUDED.latest_price,
    latest_volume = EXCLUDED.latest_volume,
    event_time = EXCLUDED.event_time;
"""

for msg in consumer:
    data = msg.value

    batch.append((
        data["symbol"],
        float(data["price"]),
        float(data["volume"]),
        datetime.fromtimestamp(data["event_time"] / 1000)
    ))

    if len(batch) >= BATCH_SIZE:
        try:
            
            unique_batch = {}

            for row in batch:
                symbol = row[0]
                unique_batch[symbol] = row

            batch = list(unique_batch.values())

            execute_values(cur, query, batch)
            conn.commit()

            print(f"Inserted/updated {len(batch)} unique symbols")

            batch = []

        except Exception as e:
            conn.rollback()
            print("ERROR:", e)
            batch = []

