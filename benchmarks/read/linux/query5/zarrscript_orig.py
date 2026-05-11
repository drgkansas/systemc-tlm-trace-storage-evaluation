import time
from collections import Counter

import numpy as np
import zarr

ZARR_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/zarr/1/data.zr"  # adjust
TIME_KEY = "st"  # arrays: ['dir', 'id', 'json', 'port', 'proto', 'st']

# 1 ms in picoseconds (ps). Use the SAME value in SQLite and Parquet scripts.
BUCKET_UNITS = 10_000_000

def main():
    start = time.perf_counter()

    root = zarr.open(ZARR_PATH, mode="r")
    if TIME_KEY not in root:
        raise KeyError(f"'{TIME_KEY}' not found. Available arrays: {list(root.array_keys())}")

    st = root[TIME_KEY]  # uint64

    # Expect 1D; if not, flatten logic would need adjustment
    if len(st.shape) != 1:
        raise RuntimeError(f"Expected 1D array for '{TIME_KEY}', got shape={st.shape}")

    n = st.shape[0]
    chunk_len = st.chunks[0] if isinstance(st.chunks, tuple) else st.chunks

    hist = Counter()

    # Manual chunk-by-chunk iteration
    for start_idx in range(0, n, chunk_len):
        end_idx = min(start_idx + chunk_len, n)
        data = np.asarray(st[start_idx:end_idx], dtype=np.int64)

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
    total = sum(hist.values())
    print(f"TOTAL_ROWS_COUNTED={total}")
    print(f"ZARR_ARRAY_LEN={n}")

if __name__ == "__main__":
    main()
