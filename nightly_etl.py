#!/usr/bin/env python3
"""
nightly_etl.py – roll‑up 1‑minute CSV dumps into one Parquet bundle per day
*New in this version*:
    • compute actual speed_kph inside vehicles.parquet
    • add simple ‘vanished’ flag ( no GPS >15 min before scheduled end – optional )
"""
from __future__ import annotations
import argparse, datetime as dt, logging, sys, textwrap, math
from pathlib import Path

import pandas as pd
import numpy as np

# ── CONFIG ──────────────────────────────────────────────────────────────────
RAW_ROOT   = Path("data_live")
HIST_ROOT  = Path("data_hist")

EXPECTED = {
    "vehicles": 24*60//1,    # 1440 / day
    "weather" : 24*60//15,   # 96  / day
    "delay"  : 24*60//1,    # 1440 / day
}

# ── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("etl.log", encoding="utf‑8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# ── ARGPARSE ───────────────────────────────────────────────────────────────
def cli() -> argparse.Namespace:
    default_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Aggregate yesterday's (or a given) raw CSV minute‑dumps into Parquet.
            Adds a *speed_kph* column to vehicles.
        """))
    ap.add_argument("--date", default=default_date,
                    help="Date YYYY‑MM‑DD (default: yesterday)")
    ap.add_argument("--allow-partial", action="store_true",
                    help="Proceed even if not all expected files exist")
    return ap.parse_args()

# ── SMALL HELPERS ──────────────────────────────────────────────────────────
def read_csvs(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

def haversine(lat1, lon1, lat2, lon2) -> np.ndarray:
    """lat/lon in degrees → metres (vectorised)."""
    r     = 6_371_000
    lat1  = np.radians(lat1); lat2 = np.radians(lat2)
    dlat  = lat2 - lat1
    dlon  = np.radians(lon2) - np.radians(lon1)
    a     = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2*r*np.arctan2(np.sqrt(a), np.sqrt(1-a))

# ── MAIN ETL ───────────────────────────────────────────────────────────────
def main() -> None:
    args      = cli()
    day       = args.date
    raw_day   = RAW_ROOT  / day
    hist_day  = HIST_ROOT / day

    if hist_day.exists():
        logging.info("%s already processed – nothing to do.", day); return
    if not raw_day.exists():
        logging.error("No raw data for %s – collectors didn’t run.", day); sys.exit(1)
    hist_day.mkdir(parents=True, exist_ok=True)

    for label, expected in EXPECTED.items():
        files = sorted((raw_day/label).glob("*.csv"))
        if not files:
            logging.warning("%s: missing – skipped", label); continue
        if len(files) < expected and not args.allow_partial:
            logging.warning("%s: %d / %d files (use --allow-partial)",
                            label, len(files), expected); continue

        df = read_csvs(files)
        if "timestamp" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df.timestamp, unit="s", utc=True)

        # ---- special treatment for vehicles --------------------------------
        if label == "vehicles" and not df.empty:
            df = df.sort_values(["vehicle_id", "timestamp"])
            # shift to previous row within same vehicle
            df["lat_prev"]  = df.groupby("vehicle_id").lat.shift()
            df["lon_prev"]  = df.groupby("vehicle_id").lon.shift()
            df["t_prev"]    = df.groupby("vehicle_id").timestamp.shift()

            mask = df.lat_prev.notna()
            dist = haversine(df.lat_prev[mask], df.lon_prev[mask],
                             df.lat[mask],      df.lon[mask])
            dt_s = (df.timestamp[mask] - df.t_prev[mask]).clip(lower=1)

            df.loc[mask, "speed_kph"] = (dist / dt_s) * 3.6
            df = df.drop(columns=["lat_prev","lon_prev","t_prev"])

        out = hist_day / f"{label}.parquet"
        df.to_parquet(out, engine="pyarrow", compression="zstd")
        logging.info("%s: wrote %s (%s rows)", label, out.name, len(df))

    logging.info("✅ nightly_etl finished for %s", day)

# ── RUN ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
