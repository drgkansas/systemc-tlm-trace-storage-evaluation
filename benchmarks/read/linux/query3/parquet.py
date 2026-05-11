import pyarrow.dataset as ds
import glob
import time
from collections import Counter

parquet_dir = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/parquet/1"
parquet_files = sorted(glob.glob(f"{parquet_dir}/transactions_*.parquet"))


dataset = ds.dataset(parquet_files, format="parquet")

start = time.perf_counter()

# Read only the protocol column
table = dataset.to_table(columns=["protocol"])
proto_arr = table["protocol"].to_numpy()

elapsed = time.perf_counter() - start

counts = Counter(int(p) for p in proto_arr)

print("Protocol, Count")
for proto, count in counts.items():
    print(f"{proto}, {count}")
print(f"TIME: {elapsed:.6f} sec")

