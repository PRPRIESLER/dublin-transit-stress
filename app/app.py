# ────────────────────────────────────────────────────────────────
# Dublin Transit Stress Analysis – Deploy-ready Streamlit app
# ────────────────────────────────────────────────────────────────

# ❶ Make package import work from anywhere
import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt
import os, time
from pathlib import Path

# corridor helpers
from norm_codes.geo.segment_utils import build_segments, snap_minutes

# ─── Mapbox token ───────────────────────────────────────────────
os.environ["MAPBOX_API_KEY"] = (
    "pk.eyJ1IjoicGluYWtpcGFuaSIsImEiOiJjbWRscGZsbGQxYWg0MmtzZHZ5YTY1Mjk4In0."
    "ku2LVM93BTD2tkhwel-qZg"
)

# ─── Optional Geo stack for ED polygons ─────────────────────────
try:
    import geopandas as gpd
    from shapely.geometry import Point
    GEO_OK = True
except Exception:
    GEO_OK = False

# ─── Paths ──────────────────────────────────────────────────────
ROOT_DIR    = Path(__file__).resolve().parents[1]
DATA_ROOT   = ROOT_DIR / "data_hist"
ROUTES_PATH = ROOT_DIR / "gtfs_static/dublin_routes.csv"
TRIPS_PATH  = ROOT_DIR / "gtfs_static/dublin_trips.csv"
SHAPES_PATH = ROOT_DIR / "gtfs_static/dublin_shapes.parquet"
ED_PATH     = ROOT_DIR / "gtfs_static" / "census" / "dublin_ed_trimmed.geojson"

st.set_page_config(
    page_title="Dublin Transit Stress Analysis",
    page_icon="🚌",
    layout="wide",
)

# ─── Title bar ──────────────────────────────────────────────────
st.title("Dublin Transit Stress Analysis")
st.caption("Exploring speed, delay, weather and vanishing trips across Dublin’s bus network")

# --- tiny popover icon, placed next to titles
def section_header(title: str, help_md: str):
    c1, c2 = st.columns([0.99, 0.04])
    with c1:
        st.header(title)
    with c2:
        try:
            with st.popover("ℹ️", use_container_width=True):
                st.markdown(help_md)
        except Exception:
            with st.expander("ℹ️", expanded=False):
                st.markdown(help_md)

# ─── Cached loaders ─────────────────────────────────────────────
@st.cache_data
def list_dates():
    return sorted(p.name for p in DATA_ROOT.iterdir()
                  if (p / "norms/veh_minute_scored.parquet").exists())

@st.cache_data
def load_scored(folder: str) -> pd.DataFrame:
    df = pd.read_parquet(DATA_ROOT / folder / "norms" / "veh_minute_scored.parquet")
    # derive convenience columns
    if "datetime_ie" in df.columns:
        dt = pd.to_datetime(df["datetime_ie"])
    elif "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"])
    elif "timestamp" in df.columns:
        dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    else:
        dt = pd.NaT
    df["datetime"]     = dt
    df["hour"]         = getattr(dt.dt, "hour", pd.Series([None]*len(df)))
    df["minute"]       = getattr(dt.dt, "minute", pd.Series([None]*len(df)))
    df["datetime_str"] = getattr(dt.dt, "strftime", lambda *_: pd.Series([""]*len(df)))("%Y-%m-%d %H:%M:%S")
    return df

@st.cache_data
def load_routes():
    r   = pd.read_csv(ROUTES_PATH)
    dup = r.duplicated("route_short_name", keep=False)
    r["display_name"] = r["route_short_name"]
    r.loc[dup, "display_name"] = r["route_short_name"] + " (" + r["route_long_name"] + ")"
    return r[["route_id", "display_name"]]

@st.cache_data
def load_trips():
    return pd.read_csv(TRIPS_PATH)[["trip_id", "shape_id"]]

@st.cache_data
def load_shapes():
    shp = pd.read_parquet(SHAPES_PATH)
    return shp.rename(columns={"shape_pt_lat": "lat",
                               "shape_pt_lon": "lon",
                               "shape_pt_sequence": "seq"})

# ─── Corridor helpers ───────────────────────────────────────────
SEGMENTS = build_segments()

@st.cache_data(show_spinner="Calculating corridors…")
def corridor_stats_filtered(filtered_df: pd.DataFrame, thr: float):
    segs    = build_segments()
    snapped = snap_minutes(filtered_df, segs)     # only rows user kept
    stats = (snapped.groupby('segment_id')
                     .agg(avg_stress=('row_stress','mean'),
                          pct_hi     =('row_stress',lambda s: (s>thr).mean()))
                     .reset_index()
                     .merge(segs, on='segment_id'))
    return stats

# ─── Stress trend ───────────────────────────────────────────────
@st.cache_data
def daily_trend(th: float) -> pd.DataFrame:
    rows = []
    for day in list_dates():
        df = pd.read_parquet(DATA_ROOT / day / "norms" / "veh_minute_scored.parquet",
                             columns=["row_stress"])
        share = (df["row_stress"].astype("float32") > th).mean()  # 0–1
        rows.append({"date": pd.to_datetime(day), "share": share})
    out = (pd.DataFrame(rows)
             .sort_values("date")
             .assign(day=lambda d: d["date"].dt.strftime("%b-%d")))
    return out

# ─── Vanish helpers (for KPIs, heatmap) ─────────────────────────
def _count_anchor_trips(df_in: pd.DataFrame) -> int:
    d = df_in[df_in["vanish_anchor"]]
    if "vehicle_id" in d.columns and "trip_id" in d.columns:
        return int(d.drop_duplicates(["vehicle_id","trip_id"]).shape[0])
    if "trip_id" in d.columns:
        return int(d["trip_id"].nunique())
    return int(d.shape[0])

@st.cache_data
def load_day_for_kpis(day: str) -> pd.DataFrame:
    fp = DATA_ROOT / day / "norms" / "veh_minute_scored.parquet"
    tried = [
        ["vanish_anchor","datetime_ie","hour","vehicle_id","trip_id"],
        ["vanish_anchor","datetime","hour","vehicle_id","trip_id"],
        ["vanish_anchor","timestamp","vehicle_id","trip_id"],
        ["vanish_anchor","hour","vehicle_id","trip_id"],
        ["vanish_anchor","hour"],
        ["vanish_anchor"],
    ]
    df_local = None
    for cols in tried:
        try:
            df_local = pd.read_parquet(fp, columns=cols); break
        except Exception:
            continue
    if df_local is None:
        df_local = pd.read_parquet(fp)

    if "hour" not in df_local.columns:
        if "datetime_ie" in df_local.columns:
            df_local["hour"] = pd.to_datetime(df_local["datetime_ie"]).dt.hour
        elif "datetime" in df_local.columns:
            df_local["hour"] = pd.to_datetime(df_local["datetime"]).dt.hour
        elif "timestamp" in df_local.columns:
            df_local["hour"] = pd.to_datetime(df_local["timestamp"], unit="s", utc=True).dt.hour
        else:
            df_local["hour"] = pd.NA

    if "vanish_anchor" not in df_local.columns and "vanished" in df_local.columns:
        df_local["vanish_anchor"] = df_local["vanished"].astype(bool)
    elif "vanish_anchor" not in df_local.columns:
        df_local["vanish_anchor"] = False

    return df_local[["vanish_anchor","hour"] + [c for c in ["vehicle_id","trip_id"] if c in df_local.columns]]

@st.cache_data
def vanish_kpis_today(day: str) -> dict:
    d = load_day_for_kpis(day)
    anchors   = _count_anchor_trips(d)
    total_vm  = len(d)
    rate      = (anchors / total_vm * 1000) if total_vm else 0.0
    worst_hr_series = (d.groupby("hour")["vanish_anchor"].sum()
                         .sort_values(ascending=False).head(1))
    worst_hour = (int(worst_hr_series.index[0]) if len(worst_hr_series) and
                  pd.notna(worst_hr_series.index[0]) else None)
    return dict(anchors=anchors, rate=rate, worst_hour=worst_hour)

@st.cache_data
def vanish_baseline(last_n=7) -> pd.DataFrame:
    rows = []
    for d in list_dates()[-last_n:]:
        dd = load_day_for_kpis(d)
        rows.append({"date": pd.to_datetime(d),
                     "anchors": _count_anchor_trips(dd),
                     "vm": len(dd)})
    base = pd.DataFrame(rows)
    base["rate"] = base["anchors"] / base["vm"] * 1000
    return base

@st.cache_data
def vanish_heatmap_last7() -> pd.DataFrame:
    days = list_dates()[-7:]
    rows = []
    for d in days:
        dd = load_day_for_kpis(d)
        dd = dd[dd["vanish_anchor"] & dd["hour"].notna()]
        if dd.empty: continue
        rows.append(dd.assign(weekday=pd.to_datetime(d).day_name(),
                              hour=dd["hour"].astype(int))[["weekday","hour","vanish_anchor"]])
    if not rows:
        return pd.DataFrame(columns=["weekday","hour","count"])
    tall = pd.concat(rows, ignore_index=True)
    agg = (tall.groupby(["weekday","hour"])["vanish_anchor"].sum()
              .reset_index(name="count"))
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    agg["weekday"] = pd.Categorical(agg["weekday"], categories=order, ordered=True)
    return agg.sort_values(["weekday","hour"])

# ─── TTV helpers ────────────────────────────────────────────────
@st.cache_data
def _load_min_cols_for_ttv(day: str) -> pd.DataFrame:
    fp = DATA_ROOT / day / "norms" / "veh_minute_scored.parquet"
    tried = [
        ["vehicle_id","trip_id","route_id","vanish_anchor","datetime_ie"],
        ["vehicle_id","trip_id","route_id","vanish_anchor","datetime"],
        ["vehicle_id","trip_id","route_id","vanish_anchor","timestamp"],
        ["vehicle_id","trip_id","route_id","vanish_anchor"],
    ]
    dfl = None
    for cols in tried:
        try:
            dfl = pd.read_parquet(fp, columns=cols); break
        except Exception:
            continue
    if dfl is None:
        dfl = pd.read_parquet(fp)

    if "datetime_ie" in dfl.columns:
        dfl["ts"] = pd.to_datetime(dfl["datetime_ie"])
    elif "datetime" in dfl.columns:
        dfl["ts"] = pd.to_datetime(dfl["datetime"])
    elif "timestamp" in dfl.columns:
        dfl["ts"] = pd.to_datetime(dfl["timestamp"], unit="s", utc=True)
    else:
        dfl["ts"] = pd.NaT

    if "vanish_anchor" not in dfl.columns and "vanished" in dfl.columns:
        dfl["vanish_anchor"] = dfl["vanished"].astype(bool)
    elif "vanish_anchor" not in dfl.columns:
        dfl["vanish_anchor"] = False

    return dfl[["vehicle_id","trip_id","route_id","vanish_anchor","ts"]].dropna(subset=["ts"])

@st.cache_data(show_spinner=False)
def ttv_minutes(days: tuple) -> pd.DataFrame:
    frames = [_load_min_cols_for_ttv(d) for d in days]
    if not frames:
        return pd.DataFrame(columns=["mins","route_id"])
    dfa = pd.concat(frames, ignore_index=True)
    keys = ["vehicle_id","trip_id"]
    first_ts  = dfa.groupby(keys)["ts"].min().rename("first_ts")
    anchor_ts = (dfa.loc[dfa["vanish_anchor"]]
                   .groupby(keys)["ts"].max()
                   .rename("anchor_ts"))
    anchor_rt = (dfa.loc[dfa["vanish_anchor"]]
                   .groupby(keys)["route_id"].agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
                   .rename("route_id"))
    out = (pd.concat([first_ts, anchor_ts, anchor_rt], axis=1).dropna())
    if out.empty:
        return pd.DataFrame(columns=["mins","route_id"])
    out["mins"] = (out["anchor_ts"] - out["first_ts"]).dt.total_seconds() / 60.0
    out = out[(out["mins"] >= 0) & (out["mins"] < 6*60)]
    return out.reset_index(drop=True)[["mins","route_id"]]

# ─── ED Geo helpers ─────────────────────────────────────────────
@st.cache_data
def load_eds():
    if not GEO_OK:
        return None
    eds = gpd.read_file(ED_PATH)
    cols = ["geometry"]
    if "ED_ID_STR" in eds.columns: cols.append("ED_ID_STR")
    if "ED_ENGLISH" in eds.columns: cols.append("ED_ENGLISH")
    eds = eds[cols].rename(columns={"ED_ID_STR":"ed_id", "ED_ENGLISH":"ed_name"})
    if eds.crs is None: eds = eds.set_crs(4326)
    else:               eds = eds.to_crs(4326)
    if "ed_name" not in eds.columns:
        eds["ed_name"] = eds.get("ed_id", pd.Series(dtype="object")).astype(str)
    return eds

def _concat_scored_days(days: tuple, cols: list[str]) -> pd.DataFrame:
    frames = []
    for d in days:
        try:
            f = load_scored(d)[cols]; frames.append(f)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)

@st.cache_data
def ed_stress_agg(days: tuple, metric: str, thr: float|None):
    eds = load_eds()
    if eds is None:
        return None

    cols = ["lat","lon","row_stress"]
    df = _concat_scored_days(days, cols).dropna(subset=["lat","lon"])
    if df.empty:
        out = eds.copy()
        out["value"]=0.0; out["n"]=0; out["count_hi"]=0; out["share_hi"]=0.0
        return out

    pts  = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326)
    join = gpd.sjoin(pts[["row_stress","geometry"]], eds[["ed_id","geometry"]],
                     how="left", predicate="within")

    g = join.groupby("ed_id")["row_stress"]
    n = join.groupby("ed_id").size().rename("n")

    if metric == "Avg":
        val = g.mean().rename("value")
        out = eds.merge(val, left_on="ed_id", right_index=True, how="left") \
                 .merge(n, left_on="ed_id", right_index=True, how="left")
        out[["value","n"]] = out[["value","n"]].fillna({"value":0.0,"n":0})
        out["count_hi"]=0; out["share_hi"]=0.0
        return out

    if metric == "Max":
        val = g.max().rename("value")
        out = eds.merge(val, left_on="ed_id", right_index=True, how="left") \
                 .merge(n, left_on="ed_id", right_index=True, how="left")
        out[["value","n"]] = out[["value","n"]].fillna({"value":0.0,"n":0})
        out["count_hi"]=0; out["share_hi"]=0.0
        return out

    # metric == "%>threshold"  → count + share above thr
    t = 0.50 if thr is None else float(thr)
    count_hi = join.groupby("ed_id").apply(lambda s: (s["row_stress"] > t).sum()).rename("count_hi")
    out = (eds.merge(n, left_on="ed_id", right_index=True, how="left")
              .merge(count_hi, left_on="ed_id", right_index=True, how="left"))
    out[["n","count_hi"]] = out[["n","count_hi"]].fillna(0)
    out["share_hi"] = (out["count_hi"] / out["n"]).fillna(0.0)
    out["value"] = out["share_hi"]  # compatibility
    return out

@st.cache_data(show_spinner=False)
def ed_vanish_counts(days: tuple):
    eds = load_eds()
    if eds is None:
        return None
    cols = ["lat","lon","vanish_anchor","vehicle_id","trip_id"]
    df = _concat_scored_days(days, cols).dropna(subset=["lat","lon"])
    if df.empty:
        out = eds.copy(); out["vanish_count"]=0; return out
    if "vanish_anchor" not in df.columns:
        df["vanish_anchor"] = df.get("vanished", False).astype(bool)
    df = df[df["vanish_anchor"]]
    if df.empty:
        out = eds.copy(); out["vanish_count"]=0; return out
    df["pair"] = df["vehicle_id"].astype(str) + "|" + df["trip_id"].astype(str)
    pts  = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326)
    join = gpd.sjoin(pts[["pair","geometry"]], eds[["ed_id","geometry"]], how="left", predicate="within")
    counts = join.dropna(subset=["ed_id"]).groupby("ed_id")["pair"].nunique()
    out = eds.merge(counts.rename("vanish_count"), left_on="ed_id", right_index=True, how="left")
    out["vanish_count"] = out["vanish_count"].fillna(0).astype(int)
    return out

# ─── Color helper ───────────────────────────────────────────────
def stress_rgb(s: float):
    if s < .35: return [200, 255, 0, 170]
    if s < .4:  return [255, 255, 0, 180]
    if s < .45: return [255, 200, 0, 190]
    if s < .5:  return [255, 140, 0, 200]
    if s < .55: return [255,  80, 0, 220]
    if s < .6:  return [255,   0, 0, 240]
    if s < .7:  return [180,   0, 0, 255]
    if s < .8:  return [180,   0, 0, 255]
    return              [120,  0, 0, 255]

# ───── Sidebar filters (for Stress map + leaderboards) ──────────
map_mode = st.sidebar.radio("🗺️ Map view", ["Dots", "Aggregate", "Corridors"], key="map_mode")
dates_all = list_dates()
date_sel = st.sidebar.selectbox("📅 Select date", dates_all, index=len(dates_all)-1, key="date_sel")

st.sidebar.title("🚦 Stress filters")
thr      = st.sidebar.slider("Stress threshold", 0.3, 1.0, 0.5, 0.05, key="thr_main")
hr_from, hr_to = st.sidebar.slider("Hour range", 0, 23, (0, 23), key="hr_range")

anim_hr = st.sidebar.selectbox("⏱️ Select Hour", [None]+list(range(24)), key="anim_hr")
if anim_hr is not None:
    if "tick" not in st.session_state: st.session_state.tick = 0
    anim_min = st.sidebar.slider("Minute", 0, 59, st.session_state.tick, key="anim_min")
else:
    anim_min = None

df         = load_scored(date_sel)
df         = df.merge(load_routes(), on="route_id", how="left")
trips_lu   = load_trips()
shapes_df  = load_shapes()

route_opts = sorted(df["display_name"].dropna().unique())
route_pick = st.sidebar.multiselect("🚌 Select route(s)", route_opts, key="route_pick")
dir_pick   = st.sidebar.multiselect("Direction",
                                    sorted(df["direction_id"].unique().astype(int)),
                                    default=sorted(df["direction_id"].unique().astype(int)),
                                    key="dir_pick")
flag_van   = st.sidebar.checkbox("🚩 Vanished only", key="flag_van")
#flag_stuck = st.sidebar.checkbox("🛑 Stuck only (speed<2 kph & delay>5 m)", key="flag_stuck")

# >>> VANISH DOTS EXTRAS (from dash8): special controls only when Dots + Vanished only
show_last_minute    = False
ignore_thr_for_last = False
if flag_van and map_mode == "Dots":
    show_last_minute = st.sidebar.checkbox(
        "Show only last known minute (vanish anchors)",
        value=False, key="last_minute_only",
        help="Filters to the final observed minute for each vanished trip."
    )
    if show_last_minute:
        ignore_thr_for_last = st.sidebar.checkbox(
            "Ignore stress threshold for last minutes",
            value=False, key="override_thr_last",
            help="Show all vanish anchors regardless of the stress slider."
        )
# <<< end extras

# ────────────────────────────────────────────────────────────────
# SECTION 1 — TRANSIT STRESS
# ────────────────────────────────────────────────────────────────
section_header("Transit Stress", """
The **stress score (0–1)** blends delay, speed, heat and vanish risk (weighted).  
Below: trend of the **share of vehicle-minutes above a chosen threshold**.
Leaderboards.
Stress Map (The filters on the Left are for this Map only!).
""")

# Scoreboard (with human-readable axes)
left, right = st.columns([6, 2])
with right:
    metric_thr = st.selectbox("Stress threshold for metric",
                              [round(x,2) for x in (0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90)],
                              index=4, key="sb_metric_thr")
with left:
    st.subheader("Performance scoreboard")
    st.altair_chart(
        alt.Chart(daily_trend(metric_thr))
        .mark_line(point=True)
        .encode(
            x=alt.X("day:N", title="Day"),
            y=alt.Y("share:Q", title="% of minutes > threshold", axis=alt.Axis(format="%")),
            tooltip=[alt.Tooltip("date:T", title="Date"),
                     alt.Tooltip("share:Q", title="Above-threshold", format=".1%")]
        )
        .properties(height=240),
        use_container_width=True
    )

# Filters → df_view
mask = df["hour"].between(hr_from, hr_to)
if anim_hr is not None:
    mask &= (df["hour"] == anim_hr) & (df["minute"] == anim_min)
if route_pick: mask &= df["display_name"].isin(route_pick)
if dir_pick:   mask &= df["direction_id"].isin(dir_pick)

if flag_van:
    if map_mode == "Dots" and show_last_minute:
        mask &= df.get("vanish_anchor", False)
        if not ignore_thr_for_last:
            mask &= df["row_stress"] >= thr
    else:
        mask &= df.get("vanished", False)
        mask &= df["row_stress"] >= thr
else:
    mask &= df["row_stress"] >= thr

df_view = df[mask]

st.subheader(f"Showing {len(df_view):,} vehicle-minutes")
subL, subR = st.columns(2)
with subL:
    st.markdown("#### 🥇 Worst routes – *average* stress")
    lb_avg = (df_view.groupby("display_name")["row_stress"].mean()
                .round(3).reset_index(name="avg")
                .nlargest(10, "avg").rename(columns={"display_name":"route"}))
    st.dataframe(lb_avg, use_container_width=True, height=310)
with subR:
    st.markdown("#### 🔺 Worst routes – *maximum* stress")
    lb_max = (df_view.groupby("display_name")["row_stress"].max()
                .round(3).reset_index(name="max")
                .nlargest(10, "max").rename(columns={"display_name":"route"}))
    st.dataframe(lb_max, use_container_width=True, height=310)

# Map
layers, tooltip = [], None
if map_mode == "Dots":
    dots = df_view.copy()
    dots["color"]  = dots["row_stress"].apply(stress_rgb)
    dots["radius"] = 150
    layers.append(pdk.Layer("ScatterplotLayer",
                            data=dots,
                            get_position='[lon, lat]',
                            get_fill_color='color',
                            get_radius='radius',
                            radius_scale=1,
                            pickable=True))
    tooltip = {"html": (
        "<b>{display_name}</b><br/>Trip {trip_id}<br/>Vehicle {vehicle_id}"
        "<br/>Stress {row_stress}<br/>Delay {delay_sec} s"
        "<br/>Speed {speed_kph} kph"
        "<br/>{datetime_str}")}

elif map_mode == "Corridors":
    seg_df = corridor_stats_filtered(df_view, thr).nlargest(10000, "avg_stress")
    seg_df["path"]  = seg_df["geometry"].apply(lambda g: list(map(list, g.coords)))
    seg_df["color"] = seg_df["avg_stress"].apply(lambda s: stress_rgb(s)[:3])
    seg_df          = seg_df.drop(columns="geometry")
    layers.append(pdk.Layer("PathLayer",
                            data=seg_df,
                            get_path="path",
                            get_color="color",
                            width_units="pixels",
                            get_width=4,
                            opacity=0.7,
                            width_min_pixels=6,
                            pickable=True))
    tooltip = {"html": "<b>Avg stress:</b> {avg_stress}"}

else:  # Aggregate by shape
    agg = (df_view.groupby(["route_id","direction_id"])
                  .agg(avg=("row_stress","mean")).reset_index())
    rep_trip = (df_view[["route_id","direction_id","trip_id"]]
                .drop_duplicates()
                .groupby(["route_id","direction_id"]).first().reset_index())
    agg = (agg.merge(rep_trip,on=["route_id","direction_id"],how="left")
              .merge(trips_lu,on="trip_id",how="left")
              .merge(shapes_df,on="shape_id",how="left")
              .sort_values("seq"))
    for (_,_,sid), g in agg.groupby(["route_id","direction_id","shape_id"]):
        pts = g[["lon","lat"]].dropna().values.tolist()
        if len(pts) < 2: continue
        layers.append(pdk.Layer("PathLayer",
                                data=pd.DataFrame({"path":[pts]}),
                                get_path="path",
                                get_color=stress_rgb(g["avg"].iloc[0])[:3],
                                width_units="pixels",
                                get_width=4,
                                opacity=0.7))

st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/streets-v11",
    initial_view_state=pdk.ViewState(latitude=53.35, longitude=-6.26, zoom=10.5),
    layers=layers,
    tooltip=tooltip
))

# ────────────────────────────────────────────────────────────────
# SECTION 2 — VANISHED TRANSIT
# ────────────────────────────────────────────────────────────────
section_header("Vanished Transit", """
A **vanished trip** = vehicle goes silent ≥30 min **before scheduled end**  
(ignoring the final 2 min near terminus). Rate shown per 1,000 vehicle-minutes.
""")

k    = vanish_kpis_today(date_sel)
base = vanish_baseline(7)
rate_mu    = float(base["rate"].mean()) if not base.empty else 0.0
count_mu   = float(base["anchors"].mean()) if not base.empty else 0.0
delta_rate = k["rate"] - rate_mu
delta_cnt  = k["anchors"] - count_mu

cA, cB, cC, cD, cE = st.columns(5)
cA.metric("Vanished transits", f"{k['anchors']:,}", delta=f"{delta_cnt:+.0f} vs 7-day avg")
cB.metric("Vanish rate / 1k min", f"{k['rate']:.2f}", delta=f"{delta_rate:+.2f} vs 7-day avg")
cC.metric("Worst hour", f"{k['worst_hour']:02d}:00" if k["worst_hour"] is not None else "—")
cD.metric("7-day avg rate", f"{rate_mu:.2f}")
cE.metric("7-day avg count", f"{count_mu:.0f}")

st.markdown("#### Vanish heatmap (last 7 days)")
hm = vanish_heatmap_last7()
if hm.empty:
    st.info("No vanish anchors found in the last 7 days.")
else:
    heat = (alt.Chart(hm)
              .mark_rect()
              .encode(
                  x=alt.X("hour:O", title="Hour of day"),
                  y=alt.Y("weekday:N", title=None,
                          sort=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]),
                  color=alt.Color("count:Q", title="Vanished", scale=alt.Scale(scheme="inferno")),
                  tooltip=["weekday:N","hour:O","count:Q"]
              ).properties(height=240))
    st.altair_chart(heat, use_container_width=True)

# TTV block
st.markdown("#### Time-to-vanish (minutes from trip start to vanish)")
scope = st.radio("Scope", ["Selected Date", "Last 7 days"], horizontal=True, key="ttv_scope")
if scope == "Selected Date":
    ttv = ttv_minutes((date_sel,))
else:
    days7 = tuple(list_dates()[-7:])
    ttv = ttv_minutes(days7)

if ttv.empty:
    st.info("No vanished trips in the selected scope.")
else:
    med = float(ttv["mins"].median())
    p25 = float(ttv["mins"].quantile(0.25))
    p75 = float(ttv["mins"].quantile(0.75))
    early = (ttv["mins"] <= 2).mean()*100
    late  = (ttv["mins"] > 30).mean()*100

    left_col, right_col = st.columns([3, 2])

    with left_col:
        k1,k2,k3,k4 = st.columns(4)
        k1.metric("Vanished trips", f"{len(ttv):,}")
        k2.metric("Median minutes", f"{med:.1f}", f"P25 {p25:.1f} / P75 {p75:.1f}")
        k3.metric("≤ 2 minutes", f"{early:.1f}%")
        k4.metric("> 30 minutes", f"{late:.1f}%")

        hist = (alt.Chart(ttv[ttv["mins"] <= 120])
                  .mark_bar()
                  .encode(
                      x=alt.X("mins:Q", bin=alt.Bin(step=2),
                              title="Minutes from first observed minute to vanish"),
                      y=alt.Y("count():Q", title="Vanished trips (count)"),
                      tooltip=[alt.Tooltip("count():Q", title="trips")]
                  ).properties(height=230))
        rule = alt.Chart(pd.DataFrame({"m":[med]})).mark_rule(color="red").encode(x="m:Q")
        st.altair_chart(hist + rule, use_container_width=True)

    with right_col:
        routes = load_routes()
        ttv_routes = (ttv.merge(routes, on="route_id", how="left")
                        .groupby("display_name")
                        .size()
                        .reset_index(name="trips")
                        .sort_values("trips", ascending=False)
                        .head(10)
                        .rename(columns={"display_name":"route"}))
        st.markdown("**Top routes by vanished trips (scope)**")
        st.dataframe(ttv_routes[["route","trips"]],
                     use_container_width=True, height=330)

# ────────────────────────────────────────────────────────────────
# SECTION 3 — CITYWIDE DISTRIBUTION (ED polygons)
# ────────────────────────────────────────────────────────────────
section_header("Citywide Distribution (Census Boundary Polygons)", """
**Left:** ED stress aggregation (Avg / Max / %>threshold).  
For **%>threshold**, polygons with **no above-threshold minutes** are muted green/gray; others use a red ramp scaled by the city’s 95th percentile **count** of above-threshold minutes.  
**Right:** unique vanished-trip counts per ED. This block ignores the sidebar filters; choose *Selected Date* vs *Last 7 days* below.
""")

if not GEO_OK:
    st.info("ED view requires GeoPandas/Shapely. Install with: `pip install geopandas shapely`")
else:
    ed_scope = st.radio("Scope", ["Selected Date", "Last 7 days"], horizontal=True, key="ed_scope")
    days_for_maps = (date_sel,) if ed_scope == "Selected Date" else tuple(list_dates()[-7:])

    

    left_col, right_col = st.columns(2)

    # LEFT: stress polygons
    with left_col:
        st.markdown("**Stress (Avg / Max / %>threshold)**")
        ed_metric = st.radio("Metric", ["Max", "Avg", "%>threshold"], horizontal=True, key="ed_metric_left")
        ed_thr = None
        if ed_metric == "%>threshold":
            ed_thr = st.selectbox("Threshold", [0.30,0.40,0.50,0.60,0.70,0.80,0.90], index=2, key="ed_thr_left")

        st.caption("Citywide stress (ED polygons)")
        gdf_stress = ed_stress_agg(days_for_maps, ed_metric, ed_thr)
        if gdf_stress is None:
            st.info("Could not load ED polygons.")
        else:
            gdf = gdf_stress.explode(index_parts=False).reset_index(drop=True)

            def to_ring(geom):
                try:    return [list(x) for x in geom.exterior.coords]
                except: return None

            gdf["polygon"] = gdf.geometry.apply(to_ring)
            gdf = gdf.dropna(subset=["polygon"])

            if ed_metric == "%>threshold":
                q95 = max(1.0, float(gdf["count_hi"].quantile(0.95))) if not gdf.empty else 1.0
                def color_from_count(c):
                    if c <= 0:
                        return [180, 210, 180, 140]     # muted green/grey for zero
                    x = min(float(c)/q95, 1.0)
                    return stress_rgb(0.3 + 0.7*x)
                gdf["color"] = gdf["count_hi"].apply(color_from_count)
                tip = {"html": "<b>{ed_name}</b><br/>Above-thr minutes: {count_hi}<br/>All minutes: {n}<br/>Share: {share_hi}"}
                legend = "Legend: grey/green = none; deeper red = more above-threshold minutes (scaled to city’s 95th %ile)."
            else:
                gdf["color"] = gdf["value"].apply(lambda v: stress_rgb(float(v)))
                tip = {"html": "<b>{ed_name}</b><br/>Value: {value}<br/>Minutes: {n}"}
                legend = "Legend: greener = lower stress; deeper red = higher."

            layer = pdk.Layer(
                "PolygonLayer",
                data=gdf,
                get_polygon="polygon",
                get_fill_color="color",
                get_line_color=[60,60,60],
                line_width_min_pixels=1,
                pickable=True, stroked=True, opacity=0.6
            )
            st.pydeck_chart(pdk.Deck(
                map_style="mapbox://styles/mapbox/streets-v11",
                initial_view_state=pdk.ViewState(latitude=53.35, longitude=-6.26, zoom=10.5),
                layers=[layer], tooltip=tip))
            st.caption(legend)
            st.download_button("Download ED stress CSV",
                               gdf_stress.drop(columns="geometry").to_csv(index=False).encode("utf-8"),
                               file_name=f"ed_stress_{ed_metric}_{ed_scope}_{date_sel}.csv",
                               mime="text/csv")

    # RIGHT: vanished counts polygons
    with right_col:
        st.markdown("**Vanished trips — count**")
        space_px = 84 if ed_metric != "%>threshold" else 168
        st.markdown(f"<div style='height:{space_px}px'></div>", unsafe_allow_html=True)
        st.caption("Citywide vanish counts")
        gdf_vanish = ed_vanish_counts(days_for_maps)
        if gdf_vanish is None:
            st.info("Could not load ED polygons.")
        else:
            draw = gdf_vanish.explode(index_parts=False).reset_index(drop=True)

            def to_ring(geom):
                try:    return [list(x) for x in geom.exterior.coords]
                except: return None

            draw["polygon"] = draw.geometry.apply(to_ring)
            draw = draw.dropna(subset=["polygon"])
            q95 = max(1, float(draw["vanish_count"].quantile(0.95))) if not draw.empty else 1.0
            draw["color"] = draw["vanish_count"].apply(lambda c: stress_rgb(0.3 + 0.7*min(float(c)/q95, 1.0)))
            layer = pdk.Layer("PolygonLayer", data=draw, get_polygon="polygon",
                              get_fill_color="color", get_line_color=[60,60,60],
                              line_width_min_pixels=1, pickable=True, stroked=True, opacity=0.6)
            tip = {"html": "<b>{ed_name}</b><br/>Vanished trips: {vanish_count}"}
            st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/streets-v11",
                                     initial_view_state=pdk.ViewState(latitude=53.35, longitude=-6.26, zoom=10.5),
                                     layers=[layer], tooltip=tip))
            st.caption("Legend: greener = fewer vanishes; deeper red = more (scaled to the city’s 95th percentile).")
            st.download_button("Download ED vanish CSV",
                               gdf_vanish.drop(columns="geometry").to_csv(index=False).encode("utf-8"),
                               file_name=f"ed_vanish_counts_{ed_scope}_{date_sel}.csv",
                               mime="text/csv")

# ─── Methodology footer ─────────────────────────────────────────
with st.expander("📘 Methodology & Glossary", expanded=False):
    st.markdown("""
**row_stress (0–1)** = weighted blend  
• `delay_norm` 0.30 • `speed_norm` 0.30 • `heat_norm` 0.15 • `vanish_anchor` 0.25  
*(rain/cold currently 0 in weights; seasonal swap is easy).*

**delay_norm:** piece-wise curve (0–5, 5–15, 15–60 min) rising to 1.  
**speed_norm:** `1 − (speed / free_flow_95pct)` for moving minutes (speed ≥ 5 kph).  
**vanish anchor:** last minute ≥30 min before scheduled end (ignoring final 2 min).  
**Rate / 1k min:** vanished trips per 1,000 vehicle-minutes.  
**Citywide ED:** aggregation ignores sidebar filters.
""")

st.caption(f"Data day: **{date_sel}** • Updated nightly")
