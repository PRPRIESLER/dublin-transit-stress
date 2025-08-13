#!/usr/bin/env python3
"""
add_delay_norm.py
───────────────────────────────────────────
Step 1 of stress metric: add `delay_norm`.

Input  : data_hist/<DAY>/veh_minute_w_vanished.parquet
Output : data_hist/<DAY>/norms/veh_minute_d.parquet
"""

from __future__ import annotations
import argparse, datetime as dt, logging, sys, textwrap
from pathlib import Path
import numpy as np
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────
DATA_ROOT = Path("data_hist")                 # leave as-is
IN_FILE   = "veh_minute_w_vanished.parquet"   # expects weather + vanished already
OUT_FILE  = "veh_minute_d.parquet"            # _d = delay-norm

# piece-wise curve -------------------------------------------------------
def delay_norm_vec(delay_sec: pd.Series) -> pd.Series:
    d = delay_sec.fillna(0).astype("float32") / 60.0    # seconds → minutes
    out = np.zeros_like(d, dtype="float32")

    m1 = d <= 5
    m2 = (d > 5) & (d <= 15)
    m3 = d > 15

    out[m1] = 0.30 * (d[m1] / 5)                                    # 0-5 →
    out[m2] = 0.30 + 0.40 * ((d[m2] - 5) / 10)                      # 5-15 →
    out[m3] = 0.70 + 0.30 * (np.minimum(d[m3] - 15, 45) / 45)       # 15-60 →
    return out.clip(0, 1)          #  ← keep this!


# ── CLI ─────────────────────────────────────────────────────────────────
def cli() -> argparse.Namespace:
    default_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
           Add `delay_norm` to vehicle-minute file.
           Output goes into a 'norms' sub-folder.
        """),
    )
    ap.add_argument("--date", default=default_date,
                    help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing norm file if present")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main() -> None:
    args = cli()
    day_dir = DATA_ROOT / args.date
    in_fp   = day_dir / IN_FILE
    out_dir = day_dir / "norms"
    out_fp  = out_dir / OUT_FILE

    if out_fp.exists() and not args.overwrite:
        print(f"{out_fp} exists – use --overwrite to rebuild.")
        return
    if not in_fp.exists():
        sys.exit(f"✗ missing {in_fp}")

    df = pd.read_parquet(in_fp, engine="pyarrow")
    df["delay_norm"] = delay_norm_vec(df["delay_sec"])

    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_fp, engine="pyarrow", compression="zstd")
    print(f"✅ wrote {len(df):,} rows → {(Path.cwd())}")

# ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%H:%M:%S")
    main()
