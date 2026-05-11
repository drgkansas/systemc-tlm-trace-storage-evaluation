import pyarrow.parquet as pq
import glob

files = sorted(glob.glob("transactions_*_chunk_*.parquet"))

print(f"Found {len(files)} chunk files")

for f in files[:5]:  # show first 5 to avoid flooding
    print("\n=== File:", f, "===")
    table = pq.read_table(f)
    print(table.to_pandas().head())

