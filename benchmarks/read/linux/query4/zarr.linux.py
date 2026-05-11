# read/linux/query4/zarr.linux.q4.py
import zarr
import numpy as np
import time

ZARR_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/zarr/1/data.zr"

PORT_TARGET = 140733363631720


root = zarr.open(ZARR_PATH, mode="r")
port_arr = root["port"]

n = port_arr.shape[0]
chunk = 1_000_000

start = time.perf_counter()

count = 0
for i in range(0, n, chunk):
    block = port_arr[i:i+chunk]
    count += int((block == PORT_TARGET).sum())

elapsed = time.perf_counter() - start

print(f"PORT={PORT_TARGET}")
print(f"COUNT={count}, TIME={elapsed:.6f} sec")

