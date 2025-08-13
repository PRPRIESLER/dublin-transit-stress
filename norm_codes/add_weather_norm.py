#!/usr/bin/env python3
"""
add_weather_norm.py
────────────────────────────────────────────
Adds weather-based normalised columns to the daily parquet.

Input  : data_hist/<DAY>/norms/veh_minute_ds.parquet
         (must already contain delay_norm & speed_norm)
Output : data_hist/<DAY>/norms/veh_minute_dsw.parquet
         (d = delay, s = speed, w = weather)

Weather norms
-------------
rain_norm : drizzle 0 → heavy (≥3 mm/h) 1
heat_norm : 18 °C comfort → 0;
            18-21 °C → 0-0.30;
            21-24 °C → 0.30-0.70;
            24-27 °C → 0.70-1.00; ≥27 °C = 1
cold_norm : ≥10 °C 0;
            10-5 °C  0-0.30;
            5-0 °C   0.30-0.70;
            ≤0 °C    0.70-1.00 (capped at –5 °C)
"""

from __future__ import annotations
import argparse, datetime as dt, logging, sys, textwrap
from pathlib import Path

import numpy as np
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────
DATA_ROOT = Path("data_hist")
IN_FILE   = "veh_minute_ds.parquet"      # delay + speed already present
OUT_FILE  = "veh_minute_dsw.parquet"     # add weather norms

# ── normalisation helpers ───────────────────────────────────────────────
def rain_norm(r_mm: pd.Series) -> pd.Series:
    """Drizzle 0 … heavy (≥3 mm/h) 1."""
    return np.clip(r_mm.fillna(0) / 3.0, 0, 1)

def heat_norm(temp_c: pd.Series) -> pd.Series:
    """18 °C baseline; above 21 ramps sharply, full at 27+."""
    t = temp_c.fillna(18)
    out = np.zeros_like(t, dtype="float32")

    m1 = (t > 18) & (t <= 21)
    m2 = (t > 21) & (t <= 24)
    m3 = (t > 24) & (t <= 27)
    m4 =  t > 27

    out[m1] = 0.30 * (t[m1] - 18) / 3             # 0 → 0.30
    out[m2] = 0.30 + 0.40 * (t[m2] - 21) / 3      # 0.30 → 0.70
    out[m3] = 0.70 + 0.30 * (t[m3] - 24) / 3      # 0.70 → 1.00
    out[m4] = 1.00
    return out

def cold_norm(temp_c: pd.Series) -> pd.Series:
    """Discomfort below 10 °C, full at ≤ –5 °C."""
    t = temp_c.fillna(15)
    out = np.zeros_like(t, dtype="float32")

    mask1 = (t < 10) & (t >= 5)      # 0-0.3
    mask2 = (t < 5)  & (t >= 0)      # 0.3-0.7
    mask3 =  t < 0                  # 0.7-1.0

    out[mask1] = 0.30 * (10 - t[mask1]) / 5
    out[mask2] = 0.30 + 0.40 * (5 - t[mask2]) / 5
    out[mask3] = 0.70 + 0.30 * np.minimum(-t[mask3], 5) / 5
    return out

# ── CLI ─────────────────────────────────────────────────────────────────
def cli() -> argparse.Namespace:
    default_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Add rain_norm, heat_norm, cold_norm to veh-minute parquet.
            Saves result in the same 'norms' subfolder.
        """),
    )
    ap.add_argument("--date", default=default_date, help="YYYY-MM-DD")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing file if present")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main() -> None:
    args = cli()
    day_dir = DATA_ROOT / args.date
    in_fp   = day_dir / "norms" / IN_FILE
    out_fp  = day_dir / "norms" / OUT_FILE

    if out_fp.exists() and not args.overwrite:
        print(f"{out_fp} exists – use --overwrite to rebuild."); return
    if not in_fp.exists():
        sys.exit(f"✗ missing {in_fp}")

    df = pd.read_parquet(in_fp, engine="pyarrow")

    df["rain_norm"] = rain_norm(df["rain_mm"])
    df["heat_norm"] = heat_norm(df["temp_c"])
    df["cold_norm"] = cold_norm(df["temp_c"])

    df.to_parquet(out_fp, engine="pyarrow", compression="zstd")
    print(f"✅ wrote {len(df):,} rows → {(Path.cwd())}")

# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%H:%M:%S")
    main()
