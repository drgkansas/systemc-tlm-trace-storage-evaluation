# read/stream/query3/sqlite.stream.q3.py
import sqlite3
import time

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/sqlite/sim.925898.db"  # adjust if needed

conn = sqlite3.connect(db_path)
cur = conn.cursor()

start = time.perf_counter()
cur.execute("SELECT proto, COUNT(*) FROM transactions GROUP BY proto")
rows = cur.fetchall()
elapsed = time.perf_counter() - start

print("Protocol, Count")
for proto, count in rows:
    print(f"{proto}, {count}")
print(f"TIME: {elapsed:.6f} sec")

conn.close()

