# read/stream/query3/csv.stream.q3.py
import time
from collections import Counter

csv_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/csv/transactions.925994.csv"


start = time.perf_counter()

proto_counts = Counter()

with open(csv_path, "r", encoding="utf-8") as f:
    # If you have a header, uncomment:
    # header = next(f, None)

    for line in f:
        parts = line.rstrip("\n").split(",", 4)
        if len(parts) < 4:
            continue
        st_str, port_str, dir_str, proto_str = parts[:4]
        proto_counts[proto_str] += 1

elapsed = time.perf_counter() - start

print("Protocol, Count")
for proto, count in proto_counts.items():
    print(f"{proto}, {count}")
print(f"TIME: {elapsed:.6f} sec")

