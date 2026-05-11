# read/stream/query3/zarr.stream.q3.py
import zarr
import time
import numpy as np
from collections import Counter

zarr_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/stream/zarr/data.zr"

root = zarr.open(zarr_path, mode="r")
proto_arr = root["proto"]  # int32

start = time.perf_counter()

p = proto_arr[:]  # load into NumPy
counts = Counter(int(x) for x in p)

elapsed = time.perf_counter() - start

print("Protocol, Count")
for proto, count in counts.items():
    print(f"{proto}, {count}")
print(f"TIME: {elapsed:.6f} sec")

