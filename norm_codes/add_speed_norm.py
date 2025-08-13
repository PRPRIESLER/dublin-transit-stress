#!/usr/bin/env python3
"""
add_speed_norm.py  – Step 2 of stress metric.

Input  : data_hist/<DAY>/norms/veh_minute_d.parquet
Lookup : norm_codes/freeflow_95pct.parquet
Output : data_hist/<DAY>/norms/veh_minute_ds.parquet
"""

from __future__ import annotations
import argparse, datetime as dt, logging, sys, textwrap
from pathlib import Path
import numpy as np
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────
DATA_ROOT = Path("data_hist")
FREE_PQT  = Path("norm_codes/freeflow_95pct.parquet")
IN_FILE   = "veh_minute_d.parquet"     # delay_norm already added
OUT_FILE  = "veh_minute_ds.parquet"    # d + s = delay & speed norm

# ── CLI ─────────────────────────────────────────────────────────────────
def cli():
    default = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Add `speed_norm` to daily veh-minute file.
            Only rows with speed ≥ 5 kph count toward congestion;
            dwell rows get speed_norm = 0.
        """),
    )
    ap.add_argument("--date", default=default, help="YYYY-MM-DD")
    ap.add_argument("--overwrite", action="store_true")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main():
    args = cli()
    day_dir = DATA_ROOT / args.date
    in_fp   = day_dir / "norms" / IN_FILE
    out_fp  = day_dir / "norms" / OUT_FILE

    if out_fp.exists() and not args.overwrite:
        print(f"{out_fp} exists – use --overwrite to rebuild."); return
    if not in_fp.exists() or not FREE_PQT.exists():
        sys.exit("✗ missing input parquet or free-flow lookup")

    df   = pd.read_parquet(in_fp,  engine="pyarrow")
    free = pd.read_parquet(FREE_PQT, engine="pyarrow")

    df = df.merge(free, on=["route_id","direction_id"], how="left")

    # ── speed_norm calculation ─────────────────────────────────────────
    moving = df["speed_kph"] >= 5                 # rows we evaluate
    df["speed_norm"] = 0.0                        # default for dwell rows

    ratio = 1 - (df.loc[moving, "speed_kph"] /
                 df.loc[moving, "free_kph"].clip(lower=1e-3))
    df.loc[moving, "speed_norm"] = ratio.clip(0, 1)

    # ── write output ───────────────────────────────────────────────────
    df.to_parquet(out_fp, compression="zstd")
    print(f"✅ wrote {len(df):,} rows → {(Path.cwd())}")

# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%H:%M:%S")
    main()
