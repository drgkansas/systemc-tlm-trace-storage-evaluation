# read/stream/query2/parquet.stream.q2.py
import pyarrow.dataset as ds
import glob
import numpy as np
import time

parquet_dir = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/parquet/1"
parquet_files = sorted(glob.glob(f"{parquet_dir}/transactions_*.parquet"))

dataset = ds.dataset(parquet_files, format="parquet")

start = time.perf_counter()

# Read only the 'direction' column into memory
table = dataset.to_table(columns=["direction"])
dir_arr = table["direction"].to_numpy()  # should be 0/1 ints

fw = int((dir_arr == 0).sum())
bw = int((dir_arr == 1).sum())

elapsed = time.perf_counter() - start

print(f"FW: {fw}, BW: {bw}, TIME: {elapsed:.6f} sec")

