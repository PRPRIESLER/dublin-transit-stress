#!/usr/bin/env python3
"""
add_stress_score.py
────────────────────────────────────────────
Combines delay, speed, weather and vanished into `row_stress`.

Weights (summer version – July):
    delay_norm   0.40
    speed_norm   0.30
    rain_norm    0.10
    heat_norm    0.08
    cold_norm    0.02
    vanished     0.10
Total = 1.00  → result is naturally 0-to-1 without clipping.
"""

from __future__ import annotations
import argparse, datetime as dt, logging, sys
from pathlib import Path
import pandas as pd

DATA_ROOT = Path("data_hist")
IN_FILE   = "veh_minute_dsw.parquet"      # contains delay, speed, weather norms
OUT_FILE  = "veh_minute_scored.parquet"

W = dict(delay  = 0.30,
         speed  = 0.30,
         rain   = 0.00,
         heat   = 0.15,   
         cold   = 0.00,
         vanish = 0.25)                  # sum = 1.00

# ── CLI ─────────────────────────────────────────────────────────────────
def cli():
    default = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(description="compute row_stress 0–1 (summer weights)")
    ap.add_argument("--date", default=default, help="YYYY-MM-DD")
    ap.add_argument("--overwrite", action="store_true")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main():
    args     = cli()
    day_dir  = DATA_ROOT / args.date
    in_fp    = day_dir / "norms" / IN_FILE
    out_fp   = day_dir / "norms" / OUT_FILE

    if out_fp.exists() and not args.overwrite:
        print(f"{out_fp} exists – use --overwrite to rebuild."); return
    if not in_fp.exists():
        sys.exit(f"✗ missing {in_fp}")

    df = pd.read_parquet(in_fp, engine="pyarrow")

    df["row_stress"] = (
      W["delay"]  * df["delay_norm"]
    + W["speed"]  * df["speed_norm"]
    + W["rain"]   * df["rain_norm"]
    + W["heat"]   * df["heat_norm"]
    + W["cold"]   * df["cold_norm"]
    + W["vanish"] * df["vanish_anchor"].astype(float)   # ← use anchor
)

    df.to_parquet(out_fp, compression="zstd")
    print(f"✅ wrote {len(df):,} rows → {(Path.cwd())}")

# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")
    main()
