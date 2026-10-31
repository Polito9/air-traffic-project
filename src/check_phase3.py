"""Quick check of Phase 3 output in MongoDB (airtraffic_db.flight_trajectories).
Run from the project root with the venv active:  python src/check_phase3.py
"""
from pymongo import MongoClient

coll = MongoClient("localhost", 27017)["airtraffic_db"]["flight_trajectories"]
print("Total trajectories:", coll.count_documents({}))

for row in coll.aggregate([
    {"$group": {"_id": "$dataset", "n": {"$sum": 1}, "avg_points": {"$avg": "$n_points"}}},
    {"$sort": {"_id": 1}},
]):
    print(f"  {row['_id']}: {row['n']} trajectories, {row['avg_points']:.1f} points on average")

doc = coll.find_one({}, {"times": 0, "headings": 0, "velocities": 0, "altitudes": 0})
if doc:
    coords = doc["geometry"]["coordinates"]
    print("\nExample document:")
    print("  _id       :", doc["_id"])
    print("  callsign  :", doc.get("callsign"), "| dataset:", doc["dataset"], "| source:", doc["source"])
    print("  n_points  :", doc["n_points"])
    print("  geometry  :", doc["geometry"]["type"], "first 2 points [lon, lat]:", coords[:2])
