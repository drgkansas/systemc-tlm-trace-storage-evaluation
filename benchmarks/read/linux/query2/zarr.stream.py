import zarr
import time
import numpy as np

db_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/zarr/1/data.zr"

root = zarr.open(db_path, mode="r")
dir_arr = root["dir"]  # zarr array, int32 0/1

start = time.perf_counter()

# Load into memory once; STREAM is small enough to be safe
d = dir_arr[:]  # numpy array

fw = int((d == 0).sum())
bw = int((d == 1).sum())

elapsed = time.perf_counter() - start

print(f"FW: {fw}, BW: {bw}, TIME: {elapsed:.6f} sec")

