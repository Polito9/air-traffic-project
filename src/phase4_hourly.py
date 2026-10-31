"""Phase 4c - Traffic by time of day (REAL live data only).

For every 10-minute bucket counts the distinct aircraft with a ping inside it,
then averages the buckets of each hour (UTC). Averaging buckets makes partially
covered hours (start/end of a capture block) comparable with full hours. Holding
events are counted per hour too. Result -> airtraffic_db.hourly_traffic.

Run from the project root with the venv active:  python src/phase4_hourly.py
Re-running is safe (the collection is rebuilt).
"""
from collections import Counter, defaultdict

from pymongo import MongoClient

BUCKET_S = 600
MIN_BUCKETS = 3          # ignore hours seen in fewer than 3 ten-minute buckets (e.g. short tests)
db = MongoClient("localhost", 27017)["airtraffic_db"]


def hourly_rows(trajectories, holdings):
    """trajectories: iterable of {icao24, times}; holdings: iterable of {start_time}."""
    buckets = defaultdict(set)
    for d in trajectories:
        for t in d.get("times") or []:
            buckets[int(t) // BUCKET_S].add(d["icao24"])
    per_hour = defaultdict(list)
    for k, ac in buckets.items():
        per_hour[(k * BUCKET_S) // 3600].append(len(ac))
    hold = Counter(int(h["start_time"]) // 3600 for h in holdings if h.get("start_time"))
    rows = []
    for hr in sorted(per_hour):
        v = per_hour[hr]
        if len(v) < MIN_BUCKETS:
            continue
        rows.append({"_id": f"live:{hr * 3600}", "dataset": "live", "hour_start": hr * 3600,
                     "utc_hour": hr % 24, "buckets": len(v), "avg_aircraft_10min": round(sum(v) / len(v), 1),
                     "max_aircraft_10min": max(v), "holdings": hold.get(hr, 0)})
    return rows


def main():
    rows = hourly_rows(db["flight_trajectories"].find({"dataset": "live"}, {"icao24": 1, "times": 1}),
                       db["holding_events"].find({}, {"start_time": 1}))
    out = db["hourly_traffic"]
    out.delete_many({})
    if rows:
        out.insert_many(rows)
    print(f"{'hour (UTC)':<18}{'buckets':>8}{'avg aircraft/10min':>20}{'holdings':>10}")
    from datetime import datetime, timezone
    for r in rows:
        print(f"{datetime.fromtimestamp(r['hour_start'], timezone.utc):%d %b %H:00}{'':<7}{r['buckets']:>8}"
              f"{r['avg_aircraft_10min']:>20}{r['holdings']:>10}")
    print(f"\nSaved {len(rows)} hours to airtraffic_db.hourly_traffic")


if __name__ == "__main__":
    main()
