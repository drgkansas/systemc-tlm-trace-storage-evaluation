# read/stream/query2/csv.stream.q2.py
import time

csv_path = "/home/gfrazier@wuad.washburn.edu/simoutput/goldenoutput/linux/csv/1/transactions.1242703.csv"

start = time.perf_counter()

fw = 0
bw = 0

with open(csv_path, "r", encoding="utf-8") as f:
    # If there is a header row, uncomment:
    # header = next(f, None)

    for line in f:
        # Split into exactly 5 pieces: st, port, dir, proto, json
        parts = line.rstrip("\n").split(",", 4)
        if len(parts) < 4:
            continue  # skip malformed lines
        st_str, port_str, dir_str, proto_str = parts[:4]

        if dir_str == "fw":
            fw += 1
        elif dir_str == "bw":
            bw += 1

elapsed = time.perf_counter() - start

print(f"FW: {fw}, BW: {bw}, TIME: {elapsed:.6f} sec")

