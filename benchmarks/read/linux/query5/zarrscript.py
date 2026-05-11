import time
from collections import Counter
import numpy as np
import zarr

ZARR_PATH = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/zarr/1/data.zr"
TIME_KEY = "st"

# MUST match SQLite & Parquet
BUCKET_UNITS = 10_000_000   # 10 µs in picoseconds

def main():
    start = time.perf_counter()

    root = zarr.open(ZARR_PATH, mode="r")
    st = root[TIME_KEY]

    n = st.shape[0]

    # Correct chunk length handling
    if isinstance(st.chunks, tuple):
        chunk_len = st.chunks[0]
    else:
        chunk_len = st.chunks

    hist = Counter()

    for i in range(0, n, chunk_len):
        data = np.asarray(st[i:i + chunk_len], dtype=np.int64)
        buckets = data // BUCKET_UNITS

        vals, cnts = np.unique(buckets, return_counts=True)
        for v, c in zip(vals, cnts):
            hist[int(v)] += int(c)

    elapsed = time.perf_counter() - start

    sorted_buckets = sorted(hist.items())

    print(f"BUCKET_UNITS={BUCKET_UNITS}")
    print(f"NUM_BUCKETS={len(sorted_buckets)}")
    print(f"TIME={elapsed:.6f} sec")

    print("bucket_index, count (first 10)")
    for b, c in sorted_buckets[:10]:
        print(b, c)

    print("TOTAL_ROWS_COUNTED =", sum(hist.values()))
    print("ARRAY_LENGTH =", n)

if __name__ == "__main__":
    main()
