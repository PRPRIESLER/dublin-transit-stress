# norm_codes/build_freeflow.py
from pathlib import Path
import pandas as pd

HIST   = Path("data_hist")
FILES  = sorted(HIST.glob("2025-07-*/norms/veh_minute_d.parquet"))

df = pd.concat(
        [pd.read_parquet(f, columns=["route_id","direction_id","speed_kph"])
         for f in FILES],
        ignore_index=True
     )

df = df[df["speed_kph"] >= 10]
freeflow = (df.groupby(["route_id","direction_id"])["speed_kph"]
              .quantile(0.95)
              .reset_index()
              .rename(columns={"speed_kph":"free_kph"}))
freeflow.to_parquet("norm_codes/freeflow_95pct.parquet", compression="zstd")
print("Rebuilt free-flow with speed ≥10 kph filter")

out = Path("norm_codes/freeflow_95pct.parquet")
freeflow.to_parquet(out, compression="zstd")
print(f"✅ wrote {len(freeflow)} free-flow rows → {out}")
