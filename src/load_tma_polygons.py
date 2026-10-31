"""Loads 6 simplified TMA polygons into MongoDB (airtraffic_db.tma_boundaries).

NEW file (does not touch ingest_tma.py). Needed because data/european_tmas.geojson
is not in the repo (.gitignore: data/). Each TMA is approximated by a circle of
RADIUS_KM around the main airport - these are SIMPLIFIED areas, not the official
AIP boundaries.

Run from the project root with the venv active:  python src/load_tma_polygons.py
Re-running is safe (the collection is emptied first).
"""
import json
import math
import os

from pymongo import MongoClient, GEOSPHERE

RADIUS_KM = 60.0
N_POINTS = 48
TMAS = [  # icao FIR code, name, airport lat, lon
    ("EGTT", "London TMA", 51.4706, -0.4619),
    ("LFFF", "Paris TMA", 49.0097, 2.5479),
    ("EDGG", "Frankfurt TMA", 50.0379, 8.5622),
    ("EHAA", "Amsterdam TMA", 52.3105, 4.7683),
    ("LECM", "Madrid TMA", 40.4936, -3.5668),
    ("EDMM", "Munich TMA", 48.3538, 11.7861),
]


def circle(lat, lon, radius_km=RADIUS_KM, n=N_POINTS):
    pts = []
    for i in range(n):
        a = 2 * math.pi * i / n          # counter-clockwise (valid GeoJSON ring)
        dlat = (radius_km / 111.32) * math.sin(a)
        dlon = (radius_km / (111.32 * math.cos(math.radians(lat)))) * math.cos(a)
        pts.append([round(lon + dlon, 5), round(lat + dlat, 5)])
    pts.append(pts[0])
    return [pts]


def main():
    feats = [{"type": "Feature",
              "properties": {"icao": icao, "name": name, "center": [lon, lat], "radius_km": RADIUS_KM},
              "geometry": {"type": "Polygon", "coordinates": circle(lat, lon)}}
             for icao, name, lat, lon in TMAS]
    os.makedirs("data", exist_ok=True)
    with open("data/european_tmas_simplified.geojson", "w") as f:
        json.dump({"type": "FeatureCollection", "features": feats}, f)

    coll = MongoClient("localhost", 27017)["airtraffic_db"]["tma_boundaries"]
    coll.delete_many({})
    coll.insert_many([{"_id": ft["properties"]["icao"], **ft["properties"], "geometry": ft["geometry"]}
                      for ft in feats])
    coll.create_index([("geometry", GEOSPHERE)])
    print(f"Loaded {coll.count_documents({})} TMAs into airtraffic_db.tma_boundaries")
    for d in coll.find({}, {"geometry": 0}):
        print("  ", d["_id"], d["name"])


if __name__ == "__main__":
    main()
