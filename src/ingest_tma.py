"""Phase 2: Load European TMA (Terminal Maneuvering Area) GeoJSON polygons into MongoDB."""
import json
import sys
from pathlib import Path
from pymongo import MongoClient, GEOSPHERE

DEFAULT_GEOJSON = Path(__file__).resolve().parent.parent / "data" / "european_tmas.geojson"

BUILTIN_TMAS = [
    {"name": "London TMA", "icao": "EGTT", "airport": "EGLL", "city": "London", "country": "United Kingdom",
     "coords": [[-1.20, 50.85], [0.95, 50.85], [0.95, 52.10], [-1.20, 52.10], [-1.20, 50.85]]},
    {"name": "Paris TMA", "icao": "LFFF", "airport": "LFPG", "city": "Paris", "country": "France",
     "coords": [[1.45, 48.20], [3.40, 48.20], [3.40, 49.50], [1.45, 49.50], [1.45, 48.20]]},
    {"name": "Frankfurt TMA", "icao": "EDGG", "airport": "EDDF", "city": "Frankfurt", "country": "Germany",
     "coords": [[7.70, 49.50], [9.50, 49.50], [9.50, 50.60], [7.70, 50.60], [7.70, 49.50]]},
    {"name": "Amsterdam Schiphol TMA", "icao": "EHAA", "airport": "EHAM", "city": "Amsterdam", "country": "Netherlands",
     "coords": [[3.80, 51.80], [5.60, 51.80], [5.60, 52.90], [3.80, 52.90], [3.80, 51.80]]},
    {"name": "Madrid TMA", "icao": "LECM", "airport": "LEMD", "city": "Madrid", "country": "Spain",
     "coords": [[-4.60, 39.85], [-2.65, 39.85], [-2.65, 41.15], [-4.60, 41.15], [-4.60, 39.85]]},
    {"name": "Munich TMA", "icao": "EDMM", "airport": "EDDM", "city": "Munich", "country": "Germany",
     "coords": [[10.90, 47.85], [12.60, 47.85], [12.60, 48.85], [10.90, 48.85], [10.90, 47.85]]},
]


def ensure_default_geojson(geojson_path: Path):
    if geojson_path.exists():
        return
    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {k: v for k, v in tma.items() if k != "coords"},
                "geometry": {"type": "Polygon", "coordinates": [tma["coords"]]},
            }
            for tma in BUILTIN_TMAS
        ],
    }
    geojson_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")


def ingest_tma_boundaries(geojson_path: Path = DEFAULT_GEOJSON):
    if geojson_path == DEFAULT_GEOJSON:
        ensure_default_geojson(geojson_path)
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [data] if data.get("type") == "Feature" else [])
    if not features:
        raise ValueError(f"No GeoJSON features found in {geojson_path}")

    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
    db = client["airtraffic_db"]
    collection = db["tma_boundaries"]

    # Ensure 2dsphere index on geometry
    collection.create_index([("geometry", GEOSPHERE)])

    upserted = 0
    for feat in features:
        props = feat.get("properties", {})
        name = props.get("name", "Unknown TMA")
        doc = {
            "name": name,
            "icao": props.get("icao"),
            "airport": props.get("airport"),
            "city": props.get("city"),
            "country": props.get("country"),
            "type": "Feature",
            "properties": props,
            "geometry": feat["geometry"],
        }
        collection.replace_one({"name": name}, doc, upsert=True)
        upserted += 1

    print(f"MongoDB: loaded {upserted} TMA boundary polygons into 'tma_boundaries' (total: {collection.count_documents({})})")


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_GEOJSON
    ingest_tma_boundaries(path)

