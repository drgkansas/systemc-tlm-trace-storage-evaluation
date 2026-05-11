
import time
from collections import Counter

import pandas as pd
import numpy as np

CSV_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/csv/1/transactions.1242703.csv"  # adjust
TIME_COL = "st"

# 1 ms in picoseconds (must match SQLite / Parquet / Zarr)
BUCKET_UNITS = 1_000_000_000

# Tune this if needed
CHUNK_ROWS = 1_000_000

def main():
    start = time.perf_counter()

    hist = Counter()
    total_rows = 0

    for chunk in pd.read_csv(
        CSV_PATH,
        usecols=[TIME_COL],
        chunksize=CHUNK_ROWS,
    ):
        data = chunk[TIME_COL].to_numpy(dtype=np.int64, copy=False)
        total_rows += data.size

        buckets = data // BUCKET_UNITS
        vals, cnts = np.unique(buckets, return_counts=True)

        for v, c in zip(vals, cnts):
            hist[int(v)] += int(c)

    elapsed = time.perf_counter() - start

    sorted_buckets = sorted(hist.items())

    print(f"BUCKET_UNITS={BUCKET_UNITS}")
    print(f"NUM_BUCKETS={len(sorted_buckets)}")
    print(f"TIME={elapsed:.6f} sec")

    print("bucket_index, count (first 20)")
    for b, c in sorted_buckets[:20]:
        print(f"{b}, {c}")

    # Sanity check
    print(f"TOTAL_ROWS_COUNTED={total_rows}")

if __name__ == "__main__":
    main()
