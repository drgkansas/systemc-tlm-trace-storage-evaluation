import zarr
import time
import numpy as np

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/zarr/1/data.zr"

root = zarr.open(db_path, mode="r")
ids = root["id"]

start = time.perf_counter()

# Read IDs and compute "number of written entries"
max_id = ids[:].max()
row_count = int(max_id + 1)

elapsed = time.perf_counter() - start

print("ROWS:", row_count, "TIME:", elapsed)

