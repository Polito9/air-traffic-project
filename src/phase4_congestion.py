"""Phase 4b - TMA congestion with MongoDB $geoIntersects.

For every TMA polygon (airtraffic_db.tma_boundaries) and every dataset, finds the
trajectories that intersect it ($geoIntersects on flight_trajectories.geometry)
and counts how many aircraft are really INSIDE it (pings within its radius) in each time bucket (10 min for `live`,
1 h for the simulated years). Result -> airtraffic_db.tma_congestion.

Run from the project root with the venv active:  python src/phase4_congestion.py
Re-running is safe (the collection is rebuilt).
"""
import math
from collections import defaultdict

from pymongo import MongoClient, GEOSPHERE

db = MongoClient("localhost", 27017)["airtraffic_db"]
traj, tmas, out = db["flight_trajectories"], db["tma_boundaries"], db["tma_congestion"]


def km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km (haversine)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def buckets_inside(doc, tma, b):
    """Time buckets in which the aircraft is really INSIDE the TMA (pings within its radius).
    Falls back to the whole session interval if the TMA has no centre/radius."""
    c, r = tma.get("center"), tma.get("radius_km")
    coords, times = doc["geometry"]["coordinates"], doc.get("times")
    if not (c and r and times and len(times) == len(coords)):
        return range(int(doc["start_time"]) // b, int(doc["end_time"]) // b + 1)
    return {int(t) // b for (lon, lat), t in zip(coords, times) if km(lat, lon, c[1], c[0]) <= r}


def bucket_for(dataset):
    return 600 if dataset == "live" else 3600


def main():
    try:
        traj.create_index([("geometry", GEOSPHERE)])
    except Exception as exc:                      # index is optional
        print("Warning: could not create 2dsphere index on flight_trajectories:", exc)

    datasets = sorted(traj.distinct("dataset"))
    out.delete_many({})
    rows = []
    print(f"{'TMA':<16}{'dataset':<18}{'flights':>8}{'peak/bucket':>13}")
    for tma in tmas.find({}):
        for ds in datasets:
            cur = traj.find(
                {"dataset": ds, "geometry": {"$geoIntersects": {"$geometry": tma["geometry"]}}},
                {"icao24": 1, "start_time": 1, "end_time": 1, "geometry": 1, "times": 1})
            b = bucket_for(ds)
            active = defaultdict(set)
            n = 0
            for d in cur:
                n += 1
                for k in buckets_inside(d, tma, b):
                    active[k].add(d["icao24"])
            for k, ac in active.items():
                rows.append({"_id": f"{ds}:{tma['_id']}:{k * b}", "dataset": ds, "tma": tma["_id"],
                             "tma_name": tma.get("name", tma["_id"]), "bucket_start": k * b,
                             "bucket_s": b, "active_flights": len(ac)})
            peak = max((len(v) for v in active.values()), default=0)
            print(f"{tma['_id']:<16}{ds:<18}{n:>8}{peak:>13}")
    if rows:
        out.insert_many(rows)
    print(f"\nSaved {len(rows)} buckets to airtraffic_db.tma_congestion")


if __name__ == "__main__":
    main()
