# read/linux/query4/sqlite.linux.q4.py
import sqlite3
import time

DB_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/sqlite/1/sim.1241200.db"  # adjust if needed

PORT_TARGET = 140736999305272

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

start = time.perf_counter()

cur.execute(
    "SELECT COUNT(*) FROM transactions WHERE port = ?",
    (PORT_TARGET,),
)

count = cur.fetchone()[0]
elapsed = time.perf_counter() - start

print(f"PORT={PORT_TARGET}")
print(f"COUNT={count}, TIME={elapsed:.6f} sec")

conn.close()

