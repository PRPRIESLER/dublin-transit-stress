# norm_codes/geo/segment_utils.py
# ────────────────────────────────────────────────────────────────
"""
Build (or load) shape-segments and snap vehicle-minutes
to the nearest segment for corridor ranking.
"""
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Point

# ─── Paths ------------------------------------------------------
ROOT    = Path(__file__).resolve().parent.parent.parent       # project root
SHAPES  = ROOT / "gtfs_static/dublin_shapes.parquet"
CACHE   = ROOT / "cache/segments.parquet"
CACHE.parent.mkdir(parents=True, exist_ok=True)               # ensure folder

# ────────────────────────────────────────────────────────────────
def build_segments() -> gpd.GeoDataFrame:
    """Return GeoDataFrame of all (shape_id, seq→seq+1) segments."""
    if CACHE.exists():
        return gpd.read_parquet(CACHE)

    shp = (pd.read_parquet(SHAPES)
             .sort_values(["shape_id", "seq"]))               # already lat / lon / seq

    rows = []
    for sid, g in shp.groupby("shape_id"):
        pts = g[["lon", "lat"]].to_numpy()
        for i in range(len(pts) - 1):
            rows.append({
                "segment_id": f"{sid}_{i}",
                "shape_id"  : sid,
                "seq"       : i,
                "geometry"  : LineString([pts[i], pts[i+1]])
            })

    seg = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    seg.to_parquet(CACHE, compression="zstd")
    return seg


def snap_minutes(df_minute: pd.DataFrame,
                 segments   : gpd.GeoDataFrame,
                 max_dist_m : float = 50) -> pd.DataFrame:
    """
    Attach the nearest `segment_id` to every vehicle-minute row
    (only if it is ≤ `max_dist_m` away).
    """
    pts = gpd.GeoDataFrame(
        df_minute.copy(),
        geometry=gpd.points_from_xy(df_minute.lon, df_minute.lat),
        crs="EPSG:4326")

    joined = gpd.sjoin_nearest(
        pts,
        segments[["segment_id", "geometry"]],
        how="left",
        distance_col="dist_m")

    return joined[joined["dist_m"] <= max_dist_m]


# ─── CLI helper -------------------------------------------------
if __name__ == "__main__":
    print("Building segment cache …")
    seg = build_segments()
    print(f"✓ wrote {len(seg):,} segments → {CACHE.relative_to(ROOT)}")
