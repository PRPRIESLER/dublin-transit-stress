#!/usr/bin/env python3
"""
join_points_to_ed.py
────────────────────────────────────────────
Adds ED_ID_STR (census polygon) to every vehicle-minute row.

Usage examples
--------------
# one day (default = yesterday)
python norm_codes/join_points_to_ed.py --date 2025-07-14

# loop over a date range
for d in 2025-07-{14..20}; do
  python norm_codes/join_points_to_ed.py --date $d
done
"""

from __future__ import annotations
import argparse, datetime as dt, sys
from pathlib import Path
import geopandas as gpd, pandas as pd
import pyarrow.parquet as pq

# ── CONFIG ──────────────────────────────────────────────────────────────
DATA_ROOT = Path("data_hist")
GEO_FILE  = Path("gtfs_static/census/dublin_ed_trimmed.geojson")
OUT_NAME  = "veh_minute_ed.parquet"        # saved beside each day's folder

# ── CLI ─────────────────────────────────────────────────────────────────
def cli():
    default_d = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=default_d, help="YYYY-MM-DD")
    ap.add_argument("--overwrite", action="store_true")
    return ap.parse_args()

# ── MAIN ────────────────────────────────────────────────────────────────
def main():
    args   = cli()
    daydir = DATA_ROOT / args.date
    in_fp  = daydir / "norms" / "veh_minute_scored.parquet"
    out_fp = daydir / OUT_NAME

    if out_fp.exists() and not args.overwrite:
        print(out_fp, "exists – use --overwrite"); return
    if not in_fp.exists():
        sys.exit(f"✗ missing {in_fp}")

    # 1) read polygons & build spatial index
    gdf_poly = (gpd.read_file(GEO_FILE)[["ED_ID_STR", "geometry"]]
                    .set_index("ED_ID_STR")
                    .to_crs("EPSG:4326"))

    # 2) load minute rows we need
    cols = ["timestamp", "lat", "lon", "row_stress", "route_id", "direction_id"]
    df = pq.read_table(in_fp, columns=cols).to_pandas()

    # 3) convert to GeoDataFrame points
    points = gpd.GeoDataFrame(df,
                              geometry=gpd.points_from_xy(df.lon, df.lat),
                              crs="EPSG:4326")

    # 4) spatial join
    joined = (gpd.sjoin(points, gdf_poly, how="inner", predicate="within")
                .drop(columns="geometry"))

    joined.to_parquet(out_fp, compression="zstd")
    print("✓ wrote", (Path.cwd()),
          f"({len(joined):,} rows, {joined['ED_ID_STR'].nunique()} polygons)")

if __name__ == "__main__":
    main()
