import time

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/csv/transactions.925994.csv"

start = time.perf_counter()
with open(db_path, "r", encoding="utf-8") as f:
    # If there is a header row, uncomment *one* of these approaches:

    # Option A: skip header explicitly
    # header = next(f, None)

    # Count remaining lines
    row_count = sum(1 for _ in f)

elapsed = time.perf_counter() - start

print("ROWS:", row_count, "TIME:", elapsed)

