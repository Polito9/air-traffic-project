"""Phase 5 - Streamlit dashboard (runs locally, NOT in Docker).

Reads airtraffic_db in MongoDB (localhost:27017) and shows:
  * Map (Kepler.gl): flight trajectories, TMA areas, holding events
  * Congestion per TMA ($geoIntersects results from phase4_congestion.py)
  * Trends per dataset (flight volume and fleet utilisation)
  * Holding events table

Run from the project root with the venv active:
    pip install streamlit keplergl pymongo pandas pydeck
    streamlit run src/dashboard.py
Env: MONGO_HOST (default localhost), MONGO_PORT (27017).
"""
import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from pymongo import MongoClient

st.set_page_config(page_title="European Air Traffic", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient(os.getenv("MONGO_HOST", "localhost"), int(os.getenv("MONGO_PORT", "27017")),
                       serverSelectionTimeoutMS=4000)["airtraffic_db"]


db = get_db()
try:
    all_datasets = sorted(db["flight_trajectories"].distinct("dataset"))
except Exception as exc:
    st.error(f"Cannot reach MongoDB on localhost:27017 - is `docker compose up -d` running?\n\n{exc}")
    st.stop()


@st.cache_data(show_spinner=False)
def load_flights(datasets, limit):
    cur = db["flight_trajectories"].find(
        {"dataset": {"$in": list(datasets)}},
        {"geometry": 1, "callsign": 1, "icao24": 1, "dataset": 1, "n_points": 1}).limit(limit)
    return [{"type": "Feature",
             "properties": {"callsign": d.get("callsign") or "", "icao24": d["icao24"],
                            "dataset": d["dataset"], "n_points": d["n_points"]},
             "geometry": d["geometry"]} for d in cur]


@st.cache_data(show_spinner=False)
def load_tmas():
    return list(db["tma_boundaries"].find({}))


@st.cache_data(show_spinner=False)
def load_holdings():
    rows = []
    for d in db["holding_events"].find({}):
        lon, lat = d["geometry"]["coordinates"]
        rows.append({"dataset": d.get("dataset"), "callsign": d.get("callsign"), "icao24": d.get("icao24"),
                     "lat": lat, "lon": lon, "n_points": d.get("n_points"),
                     "duration_s": d.get("duration_s"), "max_turn_deg": round(d.get("max_turn_deg", 0)),
                     "start_utc": datetime.fromtimestamp(d["start_time"], timezone.utc).strftime("%Y-%m-%d %H:%M")
                     if d.get("start_time") else None})
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def load_congestion():
    return pd.DataFrame(list(db["tma_congestion"].find({})))


@st.cache_data(show_spinner=False)
def load_summary():
    rows = db["flight_trajectories"].find({}, {"dataset": 1, "icao24": 1, "n_points": 1})
    df = pd.DataFrame(list(rows))
    if df.empty:
        return df
    g = df.groupby("dataset").agg(flights=("icao24", "size"), aircraft=("icao24", "nunique"),
                                  avg_points=("n_points", "mean")).reset_index()
    g["flights_per_aircraft"] = (g["flights"] / g["aircraft"]).round(2)
    return g


@st.cache_data(show_spinner=False)
def load_hourly():
    df = pd.DataFrame(list(db["hourly_traffic"].find({}).sort("hour_start", 1)))
    if not df.empty:
        df["hour"] = pd.to_datetime(df["hour_start"], unit="s", utc=True).dt.strftime("%d %b %H:00")
    return df


def load_airport_daily():
    df = pd.DataFrame(list(db["airport_daily"].find({})))
    return df


def kepler_html(flights, tmas, holdings):
    from keplergl import KeplerGl
    m = KeplerGl(height=650)
    if flights:
        m.add_data(data={"type": "FeatureCollection", "features": flights}, name="Trajectories")
    if tmas:
        m.add_data(data={"type": "FeatureCollection", "features": [
            {"type": "Feature", "properties": {"tma": t["_id"], "name": t.get("name", "")},
             "geometry": t["geometry"]} for t in tmas]}, name="TMA areas")
    if holdings is not None and not holdings.empty:
        m.add_data(data=holdings, name="Holding events")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "map.html")
        m.save_to_html(file_name=path)
        with open(path, encoding="utf-8") as f:
            return f.read()


def pydeck_map(flights, tmas, holdings):
    import pydeck as pdk
    layers = [
        pdk.Layer("GeoJsonLayer", {"type": "FeatureCollection", "features": flights},
                  get_line_color=[30, 120, 255, 150], line_width_min_pixels=1, pickable=True),
        pdk.Layer("GeoJsonLayer", {"type": "FeatureCollection", "features": [
            {"type": "Feature", "properties": {}, "geometry": t["geometry"]} for t in tmas]},
            get_fill_color=[255, 140, 0, 60], get_line_color=[230, 90, 0], line_width_min_pixels=2)]
    if holdings is not None and not holdings.empty:
        layers.append(pdk.Layer("ScatterplotLayer", holdings, get_position="[lon, lat]",
                                get_fill_color=[220, 30, 30], get_radius=12000))
    st.pydeck_chart(pdk.Deck(layers=layers, map_provider="carto", map_style="light",
                             initial_view_state=pdk.ViewState(latitude=49, longitude=8, zoom=3.6),
                             tooltip={"text": "{callsign} {dataset}"}), height=650)


st.title("Large-Scale Analysis of European Air Traffic")
st.caption("`live` = real OpenSky ADS-B data (short capture window). `synthetic_*` = SIMULATED data used to "
           "validate the pipeline - not real traffic.")

with st.sidebar:
    st.header("Filters")
    default = ["live"] if "live" in all_datasets else all_datasets[:1]
    sel = st.multiselect("Datasets", all_datasets, default=default)
    limit = st.slider("Max trajectories on the map", 100, 5000, 1500, 100)
    show_tma = st.checkbox("Show TMA areas", True)
    show_hold = st.checkbox("Show holding events", True)
    engine = st.radio("Map engine", ["Basemap (pydeck)", "Kepler.gl"],
                      help="Kepler.gl needs internet and may show no basemap behind the data.")

tab_map, tab_cong, tab_trend, tab_hold = st.tabs(["Map", "TMA congestion", "Traffic trends", "Holding events"])

with tab_map:
    flights = load_flights(tuple(sel), limit) if sel else []
    tmas = load_tmas() if show_tma else []
    hold = load_holdings() if show_hold else None
    if hold is not None and not hold.empty and sel:
        hold = hold[hold["dataset"].isin(sel)]
    st.write(f"{len(flights)} trajectories, {len(tmas)} TMAs, "
             f"{0 if hold is None else len(hold)} holding events")
    if engine == "Kepler.gl":
        try:
            components.html(kepler_html(flights, tmas, hold), height=680, scrolling=False)
        except Exception as exc:
            st.warning(f"Kepler.gl could not be rendered here ({type(exc).__name__}); showing a pydeck map instead.")
            pydeck_map(flights, tmas, hold)
    else:
        pydeck_map(flights, tmas, hold)

with tab_cong:
    cong = load_congestion()
    if cong.empty:
        st.info("Run `python src/phase4_congestion.py` first.")
    else:
        ds = st.selectbox("Dataset", sorted(cong["dataset"].unique()),
                          index=sorted(cong["dataset"].unique()).index("live") if "live" in set(cong["dataset"]) else 0)
        c = cong[cong["dataset"] == ds].copy()
        peak = c.groupby("tma_name")["active_flights"].max().sort_values(ascending=False)
        st.subheader("Peak number of aircraft in each TMA")
        st.bar_chart(peak)
        c["time_utc"] = pd.to_datetime(c["bucket_start"], unit="s", utc=True)
        st.subheader(f"Active aircraft over time ({int(c['bucket_s'].iloc[0]) // 60}-minute buckets)")
        st.line_chart(c.pivot_table(index="time_utc", columns="tma_name", values="active_flights", aggfunc="max"))

with tab_trend:
    st.subheader("Traffic by time of day (real live data, UTC)")
    hr = load_hourly()
    if hr.empty:
        st.info("Run `python src/phase4_hourly.py` first.")
    else:
        st.caption("Average number of distinct aircraft seen per 10 minutes in each hour, over Europe. Hours with no bar "
                   "were not captured (night and afternoon gaps).")
        a, b = st.columns(2)
        a.markdown("**Aircraft per 10 min (hourly average)**")
        a.bar_chart(hr.set_index("hour")["avg_aircraft_10min"])
        b.markdown("**Holding events started per hour**")
        b.bar_chart(hr.set_index("hour")["holdings"])
        st.dataframe(hr[["hour", "buckets", "avg_aircraft_10min", "max_aircraft_10min", "holdings"]],
                     hide_index=True)
    st.divider()
    st.subheader("Real historical comparison: April 2019 vs 2020 vs 2022 (OpenSky flight lists)")
    ad = load_airport_daily()
    if ad.empty:
        st.info("Run `src/upload_flightlists.py` and `src/phase4_flightlists.py` (README, section 11) to see this.")
    else:
        st.caption("Flights landing per day. Source: OpenSky flight lists (Zenodo), real data. No 2023 is available in that dataset.")
        eu = ad[ad["airport"] == "EUROPE"].pivot_table(index="day_of_month", columns="year", values="arrivals", aggfunc="sum")
        st.markdown("**Daily arrivals at all European airports (ICAO prefix E/L)**")
        st.line_chart(eu)
        hubs = ad[ad["airport"] != "EUROPE"].groupby(["airport", "year"])["arrivals"].sum().unstack("year")
        base = hubs.get(2019)
        if base is not None and 2020 in hubs.columns:
            hubs["2020 vs 2019 (%)"] = ((hubs[2020] / base - 1) * 100).round(0)
        st.markdown("**April arrivals at the six TMA airports**")
        st.dataframe(hubs)

    st.divider()
    s = load_summary()
    with st.expander("Simulated trajectories 2019 / 2020 / 2023 (pipeline validation only, not real traffic)"):
        if s.empty:
            st.info("No trajectories found.")
        else:
            a, b = st.columns(2)
            a.subheader("Total flights (trajectories)")
            a.bar_chart(s.set_index("dataset")["flights"])
            b.subheader("Fleet utilisation (flights per aircraft)")
            b.bar_chart(s.set_index("dataset")["flights_per_aircraft"])
            st.dataframe(s, hide_index=True)

with tab_hold:
    h = load_holdings()
    if h.empty:
        st.info("Run `src/phase4_holdings.py` first.")
    else:
        st.write(f"{len(h)} holding events (aircraft turning >= 360 deg within a ~50 km box, "
                 "speed >= 75 m/s, >= 2 min). Many are military training orbits.")
        st.dataframe(h.sort_values("max_turn_deg", ascending=False), hide_index=True)
