# read/stream/query2/sqlite.stream.q2.py
import sqlite3
import time

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/sqlite/sim.925898.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

start = time.perf_counter()
cur.execute("SELECT dir, COUNT(*) FROM transactions GROUP BY dir")
rows = cur.fetchall()
elapsed = time.perf_counter() - start

fw = bw = 0
for d, c in rows:
    # In your schema, dir is 0 = FW, 1 = BW
    if d == 0:
        fw = c
    elif d == 1:
        bw = c

print(f"FW: {fw}, BW: {bw}, TIME: {elapsed:.6f} sec")

conn.close()

