# Databricks notebook source
# MAGIC %md
# MAGIC # Valhalla Map Matching on Databricks
# MAGIC
# MAGIC Map matching snaps raw GPS traces to the road network, recovering the most likely path
# MAGIC a vehicle or person actually travelled. This is essential for:
# MAGIC
# MAGIC - **GPS trace cleaning** — remove signal noise and off-road artefacts
# MAGIC - **Road attribute extraction** — enrich traces with speed limits, road class, toll status, surface type
# MAGIC - **OSM cross-referencing** — recover `way_id` for each matched segment
# MAGIC - **Distributed fleet analytics** — process millions of trips in parallel with Spark
# MAGIC
# MAGIC ## Two endpoints
# MAGIC
# MAGIC | | `trace_route` | `trace_attributes` |
# MAGIC |-|--------------|---------------------|
# MAGIC | **Returns** | Turn-by-turn directions, snapped geometry | Edge attributes per matched segment |
# MAGIC | **Best for** | Navigation / re-routing workflows | Road analytics, data enrichment |
# MAGIC | **Robustness** | Good for dense, clean traces | Better for sparse or noisy traces |
# MAGIC
# MAGIC ## Requirements
# MAGIC - Valhalla init script applied to the cluster (see `valhalla_00_initial_setup.py`)
# MAGIC - Tiles built for France (see `valhalla_01_process_pbf.py`)

# COMMAND ----------

# DBTITLE 1,Parameters
import os

dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_france", "Volume Path")
volume_path = dbutils.widgets.get("VOLUME_PATH")
config_path = f"{volume_path}/tiles/valhalla.json"

# COMMAND ----------

# DBTITLE 1,Load Valhalla
import valhalla
from valhalla import Actor

actor = Actor(config_path)
print("✅ Valhalla actor initialised")
print(actor.status())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: `trace_route` — Snap a GPS trace to the road network
# MAGIC
# MAGIC Given a sequence of raw GPS points (with noise), `trace_route` returns the most likely
# MAGIC road path and turn-by-turn directions.

# COMMAND ----------

# DBTITLE 1,Simulate a noisy GPS trace: Paris 8th arrondissement
import random
import math
import json

random.seed(42)

# True path: Arc de Triomphe → Champs-Élysées → Place de la Concorde
true_path = [
    (48.8738, 2.2950),  # Arc de Triomphe
    (48.8718, 2.3010),
    (48.8700, 2.3070),
    (48.8686, 2.3130),
    (48.8673, 2.3190),
    (48.8661, 2.3250),
    (48.8655, 2.3305),  # Place de la Concorde
]

def add_gps_noise(lat, lon, noise_m=15):
    """Add realistic GPS noise (~15m radius)."""
    dlat = (random.gauss(0, noise_m) / 111_000)
    dlon = (random.gauss(0, noise_m) / (111_000 * math.cos(math.radians(lat))))
    return round(lat + dlat, 6), round(lon + dlon, 6)

noisy_trace = [add_gps_noise(lat, lon) for lat, lon in true_path]

print("Raw GPS trace (with ~15m noise):")
for i, (lat, lon) in enumerate(noisy_trace):
    print(f"  Point {i+1}: {lat}, {lon}")

# COMMAND ----------

# DBTITLE 1,trace_route: snap to road and get directions
trace_route_request = {
    "shape": [{"lat": lat, "lon": lon} for lat, lon in noisy_trace],
    "costing": "auto",
    "shape_match": "map_snap",   # fuzzy snap to nearest road segment
    "gps_accuracy": 15,          # match our simulated noise radius
    "search_radius": 50,         # max distance to search for candidate road
}

result = actor.trace_route(trace_route_request)

trip = result.get("trip", {})
summary = trip.get("summary", {})
print(f"✅ Route matched successfully")
print(f"   Distance : {summary.get('length', 0):.2f} km")
print(f"   Duration : {summary.get('time', 0) / 60:.1f} min")
print(f"   Confidence: {result.get('confidence_score', 'N/A')}")

print("\nTurn-by-turn:")
for leg in trip.get("legs", []):
    for m in leg.get("maneuvers", []):
        print(f"  → {m.get('instruction', '')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: `trace_attributes` — Extract road attributes per segment
# MAGIC
# MAGIC `trace_attributes` returns each matched **edge** (road segment) with rich OSM-derived
# MAGIC attributes. This is the right endpoint for analytics and data enrichment workflows.

# COMMAND ----------

# DBTITLE 1,trace_attributes: Paris Périphérique segment
# Simulate a trace on the Boulevard Périphérique (Paris ring road)
# Porte de Bercy → Porte de Charenton → Porte de Vincennes
peripherique_trace = [
    (48.8317, 2.3828),  # Porte de Bercy
    (48.8325, 2.3880),
    (48.8330, 2.3940),
    (48.8338, 2.3990),
    (48.8347, 2.4052),
    (48.8356, 2.4090),  # Porte de Vincennes area
]

trace_attr_request = {
    "shape": [{"lat": lat, "lon": lon} for lat, lon in peripherique_trace],
    "costing": "auto",
    "shape_match": "map_snap",
    "gps_accuracy": 10,
    "search_radius": 35,
    "filters": {
        "attributes": [
            "edge.names",
            "edge.length",
            "edge.speed",
            "edge.road_class",
            "edge.surface",
            "edge.use",
            "edge.toll",
            "edge.tunnel",
            "edge.bridge",
            "edge.roundabout",
            "edge.way_id",
            "edge.begin_heading",
            "matched.point",
            "matched.type",
            "matched.edge_index",
            "matched.distance_along_edge",
        ],
        "action": "include"
    }
}

attr_result = actor.trace_attributes(trace_attr_request)

edges = attr_result.get("edges", [])
matched_points = attr_result.get("matched_points", [])

print(f"✅ Matched {len(matched_points)} GPS points → {len(edges)} road segments")
print(f"   Confidence: {attr_result.get('confidence_score', 'N/A')}")

# COMMAND ----------

# DBTITLE 1,Parse edge attributes into a DataFrame
import pandas as pd

rows = []
for edge in edges:
    rows.append({
        "way_id":       edge.get("way_id"),
        "road_name":    edge.get("names", ["(unnamed)"])[0],
        "road_class":   edge.get("road_class"),
        "speed_kmh":    edge.get("speed"),
        "length_m":     round(edge.get("length", 0) * 1000, 1),  # Valhalla returns km
        "surface":      edge.get("surface"),
        "use":          edge.get("use"),
        "is_toll":      edge.get("toll", False),
        "is_tunnel":    edge.get("tunnel", False),
        "is_bridge":    edge.get("bridge", False),
        "is_roundabout":edge.get("roundabout", False),
        "heading_deg":  edge.get("begin_heading"),
    })

edges_df = pd.DataFrame(rows)
print(edges_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Encoded polyline input
# MAGIC
# MAGIC For compact storage and transmission, GPS traces can be passed as a
# MAGIC [Google Polyline](https://developers.google.com/maps/documentation/utilities/polylinealgorithm)
# MAGIC encoded string instead of a raw coordinate array.

# COMMAND ----------

# DBTITLE 1,Encode a trace and call trace_attributes with encoded_polyline
from valhalla.utils import decode_polyline

# Encode our noisy Paris trace as a polyline6 string
# (Valhalla uses precision=6 by default)
def encode_polyline6(coords):
    """Encode list of (lat, lon) tuples as polyline6."""
    import struct
    result = []
    prev_lat = prev_lon = 0
    for lat, lon in coords:
        for val, prev in [(lat, prev_lat), (lon, prev_lon)]:
            delta = round(val * 1e6) - round(prev * 1e6)
            delta = delta << 1
            if delta < 0:
                delta = ~delta
            while delta >= 0x20:
                result.append(chr((0x20 | (delta & 0x1f)) + 63))
                delta >>= 5
            result.append(chr(delta + 63))
        prev_lat, prev_lon = lat, lon
    return "".join(result)

encoded = encode_polyline6(noisy_trace)
print(f"Encoded polyline: {encoded[:60]}...")

result_encoded = actor.trace_attributes({
    "encoded_polyline": encoded,
    "costing": "auto",
    "shape_match": "walk_or_snap",
    "gps_accuracy": 15,
    "search_radius": 50,
    "filters": {
        "attributes": ["edge.names", "edge.road_class", "edge.speed", "edge.way_id"],
        "action": "include"
    }
})

print(f"\n✅ Matched {len(result_encoded.get('edges', []))} edges from encoded polyline")
for e in result_encoded.get("edges", []):
    print(f"  {e.get('names', ['(unnamed)'])[0]:<35} class={e.get('road_class'):<15} {e.get('speed')} km/h")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Distributed map matching with Spark
# MAGIC
# MAGIC Each Spark task initialises its own Valhalla actor and processes a batch of GPS traces.
# MAGIC This scales linearly with workers — suitable for fleet-scale GPS processing.

# COMMAND ----------

# DBTITLE 1,Generate synthetic fleet GPS traces across France
import random
import math

random.seed(0)

# Representative French city centres with short urban drive patterns
CITIES = [
    ("Paris",      48.8566,  2.3522),
    ("Lyon",       45.7640,  4.8357),
    ("Marseille",  43.2965,  5.3698),
    ("Toulouse",   43.6047,  1.4442),
    ("Bordeaux",   44.8378, -0.5792),
    ("Nantes",     47.2184, -1.5536),
    ("Strasbourg", 48.5734,  7.7521),
    ("Nice",       43.7102,  7.2620),
    ("Rennes",     48.1173, -1.6778),
    ("Grenoble",   45.1885,  5.7245),
]

def simulate_trip(city_name, lat, lon, n_points=8, noise_m=12):
    """Simulate a short urban GPS trace starting near a city centre."""
    rng = random.Random(hash(city_name))
    points = []
    cur_lat, cur_lon = lat, lon
    for _ in range(n_points):
        # Small random step (~100m)
        cur_lat += rng.gauss(0, 0.0004)
        cur_lon += rng.gauss(0, 0.0006)
        # GPS noise
        noisy_lat = cur_lat + rng.gauss(0, noise_m / 111_000)
        noisy_lon = cur_lon + rng.gauss(0, noise_m / (111_000 * math.cos(math.radians(cur_lat))))
        points.append((round(noisy_lat, 6), round(noisy_lon, 6)))
    return points

traces = []
for city, lat, lon in CITIES:
    for trip_id in range(10):  # 10 trips per city = 100 trips total
        points = simulate_trip(city, lat, lon)
        # Store as JSON string for Spark serialisation
        shape_json = json.dumps([{"lat": p[0], "lon": p[1]} for p in points])
        traces.append((f"{city}_{trip_id:02d}", city, shape_json))

trips_df = spark.createDataFrame(traces, ["trip_id", "city", "shape_json"]).repartition(20)
print(f"Created {trips_df.count()} GPS traces across {len(CITIES)} French cities")

# COMMAND ----------

# DBTITLE 1,Distributed trace_attributes via mapInPandas
from typing import Iterator, List, Dict, Optional, Tuple
import pandas as pd
import valhalla
from valhalla.utils import decode_polyline
from pyspark.databricks.sql import functions as dbf

_VALHALLA_MAX_POINTS = 16_000
_CHUNK_SIZE = 15_000   # stay below the limit
_CHUNK_OVERLAP = 1     # share one boundary point between chunks for continuity

def _match_shape(
    actor,
    shape: List[Dict],
    costing: str = "auto",
    gps_accuracy: int = 12,
    search_radius: int = 50,
) -> Tuple[List[Dict], List[Tuple], Optional[float]]:
    """
    Run trace_attributes on a shape of any length, automatically splitting into
    chunks of _CHUNK_SIZE when the shape exceeds _VALHALLA_MAX_POINTS.

    Returns (edges, decoded_coords, min_confidence) merged across all chunks.
    Chunks overlap by _CHUNK_OVERLAP points so the matched geometry is continuous.
    """
    filters = {
        "attributes": [
            "edge.names", "edge.road_class", "edge.speed",
            "edge.length", "edge.toll", "edge.way_id",
        ],
        "action": "include",
    }

    # Build chunk windows
    if len(shape) <= _CHUNK_SIZE:
        chunks = [shape]
    else:
        chunks, start = [], 0
        while start < len(shape):
            end = min(start + _CHUNK_SIZE, len(shape))
            chunks.append(shape[start:end])
            if end == len(shape):
                break
            start = end - _CHUNK_OVERLAP

    all_edges: List[Dict] = []
    all_coords: List[Tuple] = []
    min_confidence: Optional[float] = None

    for i, chunk in enumerate(chunks):
        result = actor.trace_attributes({
            "shape": chunk,
            "costing": costing,
            "shape_match": "walk_or_snap",
            "gps_accuracy": gps_accuracy,
            "search_radius": search_radius,
            "filters": filters,
        })

        all_edges.extend(result.get("edges", []))

        coords = decode_polyline(result.get("shape", ""))
        # Drop the duplicated boundary point from subsequent chunks
        all_coords.extend(coords[_CHUNK_OVERLAP:] if i > 0 and coords else coords)

        conf = result.get("confidence_score")
        if conf is not None:
            min_confidence = conf if min_confidence is None else min(min_confidence, conf)

    return all_edges, all_coords, min_confidence


def match_traces(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    actor = valhalla.Actor(config_path)   # one actor per Spark task

    for pdf in batch_iter:
        rows = []
        for _, row in pdf.iterrows():
            shape = json.loads(row["shape_json"])
            try:
                edges, coords, confidence = _match_shape(actor, shape)

                total_length_km = sum(e.get("length", 0) for e in edges)
                avg_speed = (
                    sum(e.get("speed", 0) for e in edges) / len(edges)
                    if edges else None
                )
                has_toll = any(e.get("toll", False) for e in edges)
                road_classes = list({e.get("road_class") for e in edges if e.get("road_class")})

                geometry_wkt = (
                    "LINESTRING ({})".format(", ".join(f"{lon} {lat}" for lat, lon in coords))
                    if coords else None
                )

                rows.append({
                    "trip_id":         row["trip_id"],
                    "city":            row["city"],
                    "n_edges":         len(edges),
                    "total_length_km": round(total_length_km, 3),
                    "avg_speed_kmh":   round(avg_speed, 1) if avg_speed else None,
                    "has_toll_road":   has_toll,
                    "road_classes":    ", ".join(sorted(road_classes)),
                    "confidence":      confidence,
                    "geometry_wkt":    geometry_wkt,
                    "error":           None,
                })
            except Exception as e:
                rows.append({
                    "trip_id":         row["trip_id"],
                    "city":            row["city"],
                    "n_edges":         None,
                    "total_length_km": None,
                    "avg_speed_kmh":   None,
                    "has_toll_road":   None,
                    "road_classes":    None,
                    "confidence":      None,
                    "geometry_wkt":    None,
                    "error":           str(e),
                })
        yield pd.DataFrame(rows)

matched_df = trips_df.mapInPandas(
    match_traces,
    schema="""
        trip_id string, city string,
        n_edges int, total_length_km double, avg_speed_kmh double,
        has_toll_road boolean, road_classes string, confidence double,
        geometry_wkt string, error string
    """
).withColumn(
    "geometry",
    dbf.st_setsrid(dbf.try_to_geometry("geometry_wkt"), 4326)
).drop("geometry_wkt")

matched_df.cache()
matched_df.count()

# COMMAND ----------

# DBTITLE 1,Results: matched trips with road geometry
matched_df.filter(matched_df.error.isNull()).display()

# COMMAND ----------

# DBTITLE 1,Summary statistics by city
from pyspark.sql import functions as F

matched_df.filter(matched_df.error.isNull()) \
    .groupBy("city") \
    .agg(
        F.count("trip_id").alias("trips"),
        F.round(F.avg("total_length_km"), 3).alias("avg_length_km"),
        F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
        F.round(F.avg("confidence"), 3).alias("avg_confidence"),
        F.sum(F.col("has_toll_road").cast("int")).alias("trips_with_toll"),
        F.count_if(matched_df.error.isNotNull()).alias("failures"),
    ) \
    .orderBy("city") \
    .display()

# COMMAND ----------

# DBTITLE 1,Failure rate
total = matched_df.count()
failed = matched_df.filter(matched_df.error.isNotNull()).count()
print(f"Total trips : {total}")
print(f"Matched     : {total - failed}  ({(total - failed) / total * 100:.1f}%)")
print(f"Failed      : {failed}  ({failed / total * 100:.1f}%)")

if failed > 0:
    print("\nSample errors:")
    matched_df.filter(matched_df.error.isNotNull()).select("trip_id", "city", "error").show(5, truncate=80)
