# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Shared Utilities — Valhalla Engine Room
import re
import json
from typing import Iterator, List, Dict, Optional

# ---------------------------------------------------------------------------
# Unity Catalog helpers
# ---------------------------------------------------------------------------

def validate_identifier(name, identifier_type="identifier"):
    """
    Validate a Unity Catalog identifier for SQL injection safety.
    Only allows non-delimited identifiers — no backticks, hyphens, or special chars.
    Reference: https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html
    """
    if not name:
        raise ValueError(f"{identifier_type} cannot be empty")
    if len(name) > 255:
        raise ValueError(f"{identifier_type} too long: {len(name)} chars (max 255)")
    if not re.match(r'^[a-zA-Z_]', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Must start with a letter (A-Z, a-z) or underscore (_)."
        )
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Can only contain letters, digits, and underscores."
        )
    return name


def parse_volume_path(path):
    """
    Parse and validate a Unity Catalog volume path of the form
    /Volumes/catalog/schema/volume.

    Returns (catalog, schema, volume) as validated strings.
    Raises ValueError on invalid format or invalid UC identifiers.
    """
    m = re.match(r"/Volumes/([^/]+)/([^/]+)/([^/]+)", path)
    if not m:
        raise ValueError(
            f"Invalid volume path: {path!r}. "
            f"Expected /Volumes/catalog/schema/volume"
        )
    catalog, schema, volume = m.groups()
    return (
        validate_identifier(catalog, "catalog name"),
        validate_identifier(schema,  "schema name"),
        validate_identifier(volume,  "volume name"),
    )


# ---------------------------------------------------------------------------
# Geo helpers
# ---------------------------------------------------------------------------

import math

def bearing(lat1, lon1, lat2, lon2):
    """
    Compass bearing in degrees (0–360) from point 1 to point 2.

    Useful when building Valhalla shape points from real GPS traces that
    include device heading. Do NOT use this to compute heading from the
    same coordinates you are matching — that adds no independent information.
    Only include heading when it comes from an independent sensor
    (compass / gyroscope on the recording device).
    """
    dlon = math.radians(lon2 - lon1)
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon)
    return round((math.degrees(math.atan2(x, y)) + 360) % 360, 1)


# ---------------------------------------------------------------------------
# Map matching internals
# ---------------------------------------------------------------------------

_VALHALLA_MAX_POINTS = 16_000
_CHUNK_SIZE          = 15_000   # stay below the hard limit
_CHUNK_OVERLAP       = 1        # one shared boundary point per chunk boundary
assert _CHUNK_SIZE <= _VALHALLA_MAX_POINTS, \
    f"_CHUNK_SIZE ({_CHUNK_SIZE}) must not exceed Valhalla's {_VALHALLA_MAX_POINTS}-point limit"


def _match_shape(
    actor,
    shape: List[Dict],
    costing: str = "auto",
    gps_accuracy: int = 10,
    search_radius: int = 50,
    shape_match: str = "walk_or_snap",
    turn_penalty_factor: int = 100,
    breakage_distance: int = 2000,
    interpolation_distance: int = 10,
) -> Dict:
    """
    Run trace_attributes on a shape of any length, automatically splitting into
    _CHUNK_SIZE chunks when the shape exceeds _VALHALLA_MAX_POINTS.

    Returns a dict: edges, matched_coords, confidence_score, n_chunks, n_input_points.
    Chunks overlap by _CHUNK_OVERLAP points so the matched geometry is continuous.
    Returns min(confidence_score) across chunks; None if Valhalla did not populate it
    (common for trace_attributes on short traces).
    """
    from valhalla.utils import decode_polyline

    if len(shape) < 2:
        raise ValueError(f"Trace requires ≥2 points, got {len(shape)}")

    filters = {
        "attributes": [
            "edge.names", "edge.road_class", "edge.speed",
            "edge.length", "edge.toll", "edge.way_id",
            "edge.surface", "edge.use", "edge.tunnel", "edge.bridge",
        ],
        "action": "include",
    }

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
    all_coords           = []
    min_confidence: Optional[float] = None

    for i, chunk in enumerate(chunks):
        result = actor.trace_attributes({
            "shape":                chunk,
            "costing":              costing,
            "shape_match":          shape_match,
            "gps_accuracy":         gps_accuracy,
            "search_radius":        search_radius,
            "turn_penalty_factor":  turn_penalty_factor,
            "breakage_distance":    breakage_distance,
            "interpolation_distance": interpolation_distance,
            "filters":              filters,
        })

        all_edges.extend(result.get("edges", []))

        coords = decode_polyline(result.get("shape", "")) or []
        skip = _CHUNK_OVERLAP if i > 0 else 0
        all_coords.extend(coords[skip:])

        conf = result.get("confidence_score")
        if conf is not None:
            min_confidence = conf if min_confidence is None else min(min_confidence, conf)

    return {
        "edges":            all_edges,
        "matched_coords":   all_coords,
        "confidence_score": min_confidence,
        "n_chunks":         len(chunks),
        "n_input_points":   len(shape),
    }


# ---------------------------------------------------------------------------
# Spark schemas
# ---------------------------------------------------------------------------

from pyspark.sql.types import (
    ArrayType, StructType, StructField,
    LongType, StringType, DoubleType, BooleanType
)

edge_schema = ArrayType(StructType([
    StructField("way_id",     LongType()),
    StructField("road_class", StringType()),
    StructField("length_km",  DoubleType()),
    StructField("speed_kmh",  DoubleType()),
    StructField("is_toll",    BooleanType()),
]))

MATCH_TRACES_SCHEMA = """
    trip_id string, city string,
    n_edges int, total_length_km double, avg_speed_kmh double,
    has_toll_road boolean, road_classes string, confidence double,
    geometry_wkt string, result_json string, edges_json string, error string
"""


# ---------------------------------------------------------------------------
# mapInPandas worker factory
# ---------------------------------------------------------------------------

def make_match_traces(config_path, match_cfg: dict):
    """
    Return a mapInPandas-compatible function that runs trace_attributes on
    each row's shape_json, using the given Valhalla config and match parameters.

    The config_path is broadcast once per call to make_match_traces, not per
    Spark task. Pass a snapshot of MATCH_CONFIG (dict(MATCH_CONFIG)) so the
    config is frozen at submission time.

    Usage:
        _cfg = dict(MATCH_CONFIG)
        fn   = make_match_traces(config_path, _cfg)
        df   = trips_df.mapInPandas(fn, schema=MATCH_TRACES_SCHEMA)
    """
    import valhalla
    config_path_bc = spark.sparkContext.broadcast(config_path)

    def match_traces(batch_iter: Iterator, _cfg=match_cfg) -> Iterator:
        import pandas as pd
        actor = valhalla.Actor(config_path_bc.value)
        try:
            for pdf in batch_iter:
                rows = []
                for _, row in pdf.iterrows():
                    try:
                        shape   = json.loads(row["shape_json"])
                        matched = _match_shape(actor, shape, **_cfg)
                        edges   = matched["edges"]
                        coords  = matched["matched_coords"]

                        if not edges:
                            raise ValueError("Valhalla returned 0 matched edges — trace may be outside tile coverage")

                        total_len = sum(e.get("length", 0) for e in edges)
                        avg_speed = (
                            sum(e.get("speed", 0) * e.get("length", 0) for e in edges) / total_len
                            if total_len > 0 else None
                        )

                        geometry_wkt = (
                            "LINESTRING ({})".format(", ".join(f"{lon} {lat}" for lat, lon in coords))
                            if coords else None
                        )

                        result_json = json.dumps({
                            "confidence_score": matched["confidence_score"],
                            "n_chunks":         matched["n_chunks"],
                            "n_input_points":   matched["n_input_points"],
                            "match_config":     _cfg,
                        })

                        edges_json = json.dumps([
                            {
                                "way_id":     e.get("way_id"),
                                "road_class": e.get("road_class"),
                                "length_km":  e.get("length"),
                                "speed_kmh":  e.get("speed"),
                                "is_toll":    e.get("toll", False),
                            }
                            for e in edges
                        ])

                        rows.append({
                            "trip_id":         row["trip_id"],
                            "city":            row["city"],
                            "n_edges":         len(edges),
                            "total_length_km": round(total_len, 3),
                            "avg_speed_kmh":   round(avg_speed, 1) if avg_speed else None,
                            "has_toll_road":   any(e.get("toll", False) for e in edges),
                            "road_classes":    ", ".join(sorted({e.get("road_class") for e in edges if e.get("road_class")})),
                            "confidence":      matched["confidence_score"],
                            "geometry_wkt":    geometry_wkt,
                            "result_json":     result_json,
                            "edges_json":      edges_json,
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
                            "result_json":     None,
                            "edges_json":      None,
                            "error":           f"{type(e).__name__}: {e}",
                        })
                yield pd.DataFrame(rows)
        finally:
            del actor

    return match_traces


print("✅ valhalla_utils loaded")
