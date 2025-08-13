#!/usr/bin/env python3
"""
join_daily.py  – build one tidy “bus_minute” table per day

Inputs
  data_hist/YYYY-MM-DD/vehicles.parquet
  data_hist/YYYY-MM-DD/delay.parquet
Static lookup
  gtfs_static/dublin_trips.csv   (trip_id ▸ route_id)

Output
  data_hist/YYYY-MM-DD/veh_minute.parquet
    vehicle_id · trip_id · route_id · direction_id
    timestamp  · datetime_utc
    lat · lon · speed_kph · delay_sec
"""

from __future__ import annotations
import argparse, datetime as dt, logging, sys, textwrap
from pathlib import Path
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────
HIST_ROOT = Path("data_hist")
TRIPS_CSV = Path("gtfs_static/dublin_trips.csv")   # trip_id → route_id

# ── CLI ─────────────────────────────────────────────────────────────────
def cli() -> argparse.Namespace:
    default_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Join vehicles + delays (minute-level) and add route_id.
            Produces one veh_minute.parquet per calendar day.
        """),
    )
    ap.add_argument("--date", default=default_date,
                    help="Date to process, YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing veh_minute.parquet if present")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main() -> None:
    args = cli()
    day  = args.date

    day_dir = HIST_ROOT / day
    out_fp  = day_dir / "veh_minute.parquet"

    if out_fp.exists() and not args.overwrite:
        print(f"{out_fp} already exists – use --overwrite to rebuild.")
        return

    veh_fp = day_dir / "vehicles.parquet"
    del_fp = day_dir / "delay.parquet"
    for fp in (veh_fp, del_fp, TRIPS_CSV):
        if not fp.exists():
            sys.exit(f"✗ missing required file: {fp}")

    # 1) load parquet ----------------------------------------------------
    veh   = pd.read_parquet(veh_fp, engine="pyarrow")
    delay = pd.read_parquet(del_fp, engine="pyarrow")
    trips = pd.read_csv(TRIPS_CSV, usecols=["trip_id", "route_id"])

    # 2) attach route_id -------------------------------------------------
    veh = veh.merge(trips, on="trip_id", how="left")

    # 3) attach delay_sec  (minute-level key) ----------------------------
    #    floor both feeds to the same minute
    veh["ts_min"]   = (veh["timestamp"]   // 60) * 60
    delay["ts_min"] = (delay["timestamp"] // 60) * 60

    d_lookup = (
        delay.set_index(["trip_id", "ts_min"])["delay_sec"]
    )
    veh["delay_sec"] = veh.set_index(["trip_id", "ts_min"]) \
                          .index.map(d_lookup)

    # 4) tidy up & save --------------------------------------------------
    order = ["vehicle_id", "trip_id", "route_id", "direction_id",
             "timestamp", "datetime_utc",
             "lat", "lon", "speed_kph", "delay_sec"]
    veh = veh[order]

    veh.to_parquet(out_fp, engine="pyarrow", compression="zstd")
    print(f"✅ wrote {len(veh):,} rows  →  {out_fp}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s  %(message)s")
    main()
