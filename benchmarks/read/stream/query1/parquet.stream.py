import pyarrow.dataset as ds
import glob
import time

parquet_dir = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/parquet"

# Only select the Parquet files, skip output.txt and anything else
parquet_files = glob.glob(f"{parquet_dir}/transactions_*.parquet")

print("Found parquet files:", len(parquet_files))

dataset = ds.dataset(parquet_files, format="parquet")

start = time.perf_counter()
count = dataset.count_rows()
elapsed = time.perf_counter() - start

print(f"Rows: {count}, Time: {elapsed:.6f} sec")

