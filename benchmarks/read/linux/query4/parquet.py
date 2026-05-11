# read/linux/query4/parquet.linux.q4.py
import pyarrow.dataset as ds
import glob
import time

PARQUET_DIR = "../goldenoutput/linux/parquet/1"
PORT_TARGET = 140722308007176

parquet_files = sorted(glob.glob(f"{PARQUET_DIR}/transactions_*.parquet"))

dataset = ds.dataset(parquet_files, format="parquet")

start = time.perf_counter()

table = dataset.to_table(
    filter=(ds.field("port_id") == PORT_TARGET),
    columns=["port_id"],
)

count = table.num_rows
elapsed = time.perf_counter() - start

print(f"PORT={PORT_TARGET}")
print(f"COUNT={count}, TIME={elapsed:.6f} sec")

