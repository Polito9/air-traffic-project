"""Prints the numbers needed for the report from MongoDB (airtraffic_db).
Run from the project root with the venv active:  python src/report_stats.py
"""
import statistics as st
from datetime import datetime, timezone

from pymongo import MongoClient

db = MongoClient("localhost", 27017)["airtraffic_db"]
T, H, C = db["flight_trajectories"], db["holding_events"], db["tma_congestion"]
utc = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M")

print("=== TRAJECTORIES ===")
for ds in sorted(T.distinct("dataset")):
    rows = list(T.find({"dataset": ds}, {"icao24": 1, "n_points": 1, "start_time": 1, "end_time": 1}))
    dur = [(r["end_time"] - r["start_time"]) / 60 for r in rows]
    print(f"{ds}: {len(rows)} trajectories, {len({r['icao24'] for r in rows})} aircraft, "
          f"{st.mean(r['n_points'] for r in rows):.0f} avg points, "
          f"duration min/median/max = {min(dur):.0f}/{st.median(dur):.0f}/{max(dur):.0f} min")
    if ds == "live":
        print(f"   live window (UTC): {utc(min(r['start_time'] for r in rows))} -> {utc(max(r['end_time'] for r in rows))}")
        print(f"   trajectories >= 30 min: {sum(d >= 30 for d in dur)} ; >= 2 h: {sum(d >= 120 for d in dur)}")

print("\n=== HOLDINGS ===")
hs = list(H.find({}))
print(f"{len(hs)} events, {len({h['icao24'] for h in hs})} distinct aircraft, "
      f"median duration {st.median(h['duration_s'] for h in hs) / 60:.1f} min")
print("top 5 by turn:", [(h.get('callsign'), round(h['max_turn_deg'])) for h in sorted(hs, key=lambda h: -h['max_turn_deg'])[:5]])
print("by hour UTC:", dict(sorted(__import__('collections').Counter(utc(h['start_time'])[11:13] for h in hs).items())))

print("\n=== CONGESTION (live, peak aircraft per 10-min bucket) ===")
for tma in db["tma_boundaries"].find({}, {"geometry": 0}):
    rows = list(C.find({"dataset": "live", "tma": tma["_id"]}))
    if rows:
        top = max(rows, key=lambda r: r["active_flights"])
        print(f"{tma['_id']} {tma.get('name')}: peak {top['active_flights']} at {utc(top['bucket_start'])} UTC")
