# read/linux/query5/parquet.py

import time
from collections import Counter
import glob
import os

import pyarrow.dataset as ds
import pyarrow.compute as pc

PARQUET_DIR = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/parquet/1"

TIME_COL = "time_ps"      # matches your Parquet schema
BUCKET_UNITS = 1_000_000  # must match BUCKET_UNITS in the SQLite script

def main():
    # Only .parquet files
    parquet_files = sorted(
        glob.glob(os.path.join(PARQUET_DIR, "*.parquet"))
    )
    if not parquet_files:
        raise RuntimeError(f"No .parquet files found in {PARQUET_DIR}")

    dataset = ds.dataset(parquet_files, format="parquet")

    start_time = time.perf_counter()

    scanner = dataset.scanner(
        columns=[TIME_COL],
        use_threads=True,
    )

    hist = Counter()

    for batch in scanner.to_batches():
        # Get the column as an Array/ChunkedArray
        idx = batch.schema.get_field_index(TIME_COL)
        t = batch.column(idx)

        # bucket = floor(time_ps / BUCKET_UNITS)
        #bucket = pc.floor(
            #pc.divide(t, pc.scalar(BUCKET_UNITS))
        #)

        bucket = pc.floor(
            pc.divide(t, BUCKET_UNITS)
        )


        vc = pc.value_counts(bucket)
        values = vc.field("values").to_pylist()
        counts = vc.field("counts").to_pylist()

        for b, c in zip(values, counts):
            hist[int(b)] += int(c)

    elapsed = time.perf_counter() - start_time

    sorted_buckets = sorted(hist.items())

    print(f"BUCKET_UNITS={BUCKET_UNITS}")
    print(f"NUM_BUCKETS={len(sorted_buckets)}")
    print(f"TIME={elapsed:.6f} sec")

    print("bucket_index, count  (first 20)")
    for b, c in sorted_buckets[:20]:
        print(f"{b}, {c}")

if __name__ == "__main__":
    main()
