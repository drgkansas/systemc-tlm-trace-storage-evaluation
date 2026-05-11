import sqlite3
import time

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/sqlite/sim.925898.db"

conn = sqlite3.connect(db_path)
cur = conn.cursor()

start = time.perf_counter()
cur.execute("SELECT COUNT(*) FROM transactions")
count = cur.fetchone()[0]
elapsed = time.perf_counter() - start

print(count, elapsed)

conn.close()

