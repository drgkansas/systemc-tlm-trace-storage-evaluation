# read/linux/query4/csv.linux.q4.py
import time

parquet_dir = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/parquet/1"

CSV_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/csv/1/transactions.1242703.csv"
PORT_TARGET = 140734622172264

start = time.perf_counter()
count = 0

with open(CSV_PATH, "r", encoding="utf-8") as f:
    # skip header if present
    # next(f, None)

    for line in f:
        if not line.strip():
            continue

        parts = line.split(",", 4)
        if len(parts) < 2:
            continue

        try:
            port_val = int(parts[1])
        except ValueError:
            continue

        if port_val == PORT_TARGET:
            count += 1

elapsed = time.perf_counter() - start

print(f"PORT={PORT_TARGET}")
print(f"COUNT={count}, TIME={elapsed:.6f} sec")

