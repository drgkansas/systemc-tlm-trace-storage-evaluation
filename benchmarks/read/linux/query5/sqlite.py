# read/linux/query5/sqlite_time_hist.py

import sqlite3
import time

DB = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/sqlite/1/sim.1241200.db"

# Adjust this after you see min/max(st) if needed
BUCKET_UNITS = 1_000_000  # example: 1e6 "time units" per bucket (ns, cycles, etc.)

conn = sqlite3.connect(DB)
cur = conn.cursor()

query = f"""
SELECT
    CAST(st / ? AS INTEGER) AS bucket,
    COUNT(*) AS cnt
FROM transactions
GROUP BY bucket
ORDER BY bucket;
"""

start = time.perf_counter()
cur.execute(query, (BUCKET_UNITS,))
rows = cur.fetchall()
elapsed = time.perf_counter() - start

print(f"BUCKET_UNITS={BUCKET_UNITS}")
print(f"NUM_BUCKETS={len(rows)}")
print(f"TIME={elapsed:.6f} sec")

print("bucket_index, count  (first 20)")
for bucket, cnt in rows[:20]:
    print(bucket, cnt)

conn.close()

