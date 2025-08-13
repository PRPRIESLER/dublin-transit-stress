#!/usr/bin/env python3
"""
join_minute_with_weather.py
───────────────────────────
Merge 1‑minute vehicle data with 15‑min zone weather.

Inputs   : data_hist/<DAY>/veh_minute.parquet
           data_hist/<DAY>/weather.parquet
Output    : data_hist/<DAY>/veh_minute_w.parquet

Changes vs v1:
  • file names use *veh_*  (not bus_)
  • nearest merge (±7 min 30 s)
  • convert UTC → Irish local time column `datetime_ie`
  • drop merge helper columns (…_w)
"""
from __future__ import annotations
import argparse, datetime as dt
from pathlib import Path
import numpy as np
import pandas as pd

DATA_ROOT = Path("data_hist")
ZONES = {"north":  (53.38, -6.26),
         "centre": (53.34, -6.26),
         "south":  (53.29, -6.26)}

# ── helpers ────────────────────────────────────────────────────────────────
def hav_km(lat1, lon1, lat2, lon2):
    R = 6_371.0
    lat1 = np.radians(lat1);  lat2 = np.radians(lat2)
    dlat = lat2 - lat1
    dlon = np.radians(lon2)-np.radians(lon1)
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2*R*np.arctan2(np.sqrt(a), np.sqrt(1-a))

def cli() -> argparse.Namespace:
    dflt = (dt.date.today()-dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=dflt, help="YYYY‑MM‑DD (default: y‑day)")
    return ap.parse_args()

# ── main ───────────────────────────────────────────────────────────────────
def main() -> None:
    DAY = cli().date
    day_dir = DATA_ROOT / DAY
    veh_fp  = day_dir / "veh_minute.parquet"
    wea_fp  = day_dir / "weather.parquet"
    out_fp  = day_dir / "veh_minute_w.parquet"

    if out_fp.exists():
        print(f"{out_fp.name} already exists – skip"); return
    if not veh_fp.exists() or not wea_fp.exists():
        raise SystemExit("✗ missing veh_minute or weather parquet")

    v = pd.read_parquet(veh_fp , engine="pyarrow")
    w = pd.read_parquet(wea_fp, engine="pyarrow")

    # zone for each vehicle row ------------------------------------------------
    lat = v["lat"].values;  lon = v["lon"].values
    dist_stack = np.vstack([hav_km(lat, lon, *ZONES[z]) for z in ZONES])
    v["zone"] = pd.Categorical(
        [list(ZONES)[i] for i in dist_stack.argmin(axis=0)],
        categories=list(ZONES), ordered=True)

    # minute stamps ------------------------------------------------------------
    v["minute"] = pd.to_datetime(v["timestamp"], unit="s").dt.floor("min")
    w["minute"] = pd.to_datetime(w["timestamp"], unit="s").dt.floor("min")

    # nearest merge per zone ---------------------------------------------------
    parts = []
    for z, grp in v.groupby("zone", sort=False):
        w_z = w.query("zone == @z").sort_values("minute")
        merged = pd.merge_asof(
            grp.sort_values("minute"), w_z,
            on="minute", suffixes=("", "_w"),
            direction="nearest",
            tolerance=pd.Timedelta("7min30s")
        )
        parts.append(merged)

    df = pd.concat(parts, ignore_index=True)

    # local Irish time column --------------------------------------------------
    df["datetime_ie"] = pd.to_datetime(df["timestamp"], unit="s", utc=True) \
                          .dt.tz_convert("Europe/Dublin") \
                          .dt.tz_localize(None)

    # drop helper columns ------------------------------------------------------
    df = df.drop(columns=["timestamp_w", "zone_w",
                          "lat_w", "lon_w", "datetime_utc_w"], errors="ignore")

    df.to_parquet(out_fp, engine="pyarrow", compression="zstd")
    print(f"✓ wrote {len(df):,} rows  →  {(Path.cwd())}")

# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
