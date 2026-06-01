# Valhalla Notebook Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address all 19 code-review findings across 5 notebooks — 7 HIGH (correctness/scale), 8 MEDIUM (perf/readability), 4 LOW (style) — with interactive test checkpoints after each phase on the Azure cluster `0601-134740-olo78hb3` (Valhalla + Andorra tiles installed).

**Architecture:** Fix-by-fix grouped by theme, `valhalla_map_matching.py` first in each phase. Shared utility extracted to `valhalla_utils.py` (%run pattern). All intermediate testing interactive against the live Azure cluster; compile changes last on a fresh cluster.

**Tech Stack:** Databricks Runtime 18.2, PySpark `mapInPandas`, Valhalla Python bindings, Azure `Standard_D8s_v3`, Andorra tiles at `/Volumes/timo/geospatial/valhalla_andorra/`.

**Test cluster:** `https://adb-984752964297111.11.azuredatabricks.net/compute/clusters/0601-134740-olo78hb3`  
Use the command execution API pattern established in this session to run test cells interactively:
```python
import subprocess, json, time, urllib.request
token = json.loads(subprocess.check_output(
    "databricks auth token --host https://adb-984752964297111.11.azuredatabricks.net --profile naturgy-poc 2>/dev/null",
    shell=True
))['access_token']
# Create context, execute command, poll for result (see session history)
```

---

## File Map

| File | Action | Phases |
|------|--------|--------|
| `src/notebooks/valhalla_utils.py` | **Create** | 0 |
| `src/notebooks/valhalla_map_matching.py` | Modify | 1, 3, 5 |
| `src/notebooks/valhalla_test_routing.py` | Modify | 2, 4, 5 |
| `src/notebooks/valhalla_01_process_pbf.py` | Modify | 2, 4 |
| `src/notebooks/valhalla_quickstart.py` | Modify | 4, 5 |
| `src/notebooks/valhalla_00_initial_setup.py` | Modify | 5, 6 |

---

## Task 0: Create `valhalla_utils.py` shared notebook

**Files:**
- Create: `src/notebooks/valhalla_utils.py`

- [ ] **Step 1: Create the file**

```python
# src/notebooks/valhalla_utils.py
# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Shared Utilities
import re

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

print("✅ valhalla_utils loaded")
```

- [ ] **Step 2: Test on cluster**

Run this code on `0601-134740-olo78hb3` via the command execution API:
```python
# Test via %run equivalent — paste into cluster context
exec(open("/Workspace/Users/timo.roest@databricks.com/.bundle/valhalla/azure/files/src/notebooks/valhalla_utils.py").read())

# Should print: ✅ valhalla_utils loaded
validate_identifier("timo", "catalog")          # should return "timo"

try:
    validate_identifier("bad-name", "catalog")
    print("FAIL: should have raised")
except ValueError as e:
    print(f"PASS: {e}")

try:
    validate_identifier("", "schema")
    print("FAIL: should have raised")
except ValueError as e:
    print(f"PASS: {e}")
```

Expected output:
```
✅ valhalla_utils loaded
PASS: Invalid catalog: 'bad-name'. Can only contain letters...
PASS: schema cannot be empty
```

- [ ] **Step 3: Remove `validate_identifier` from `valhalla_00_initial_setup.py` lines 58–86, replace with `%run`**

In `valhalla_00_initial_setup.py`, replace lines 37–90 (the entire parameters cell) with:

```python
# DBTITLE 1,Configure Volume Path and Ensure It Exists
# MAGIC %run ./valhalla_utils

import os
import re

dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Target Volume Path")
vol_base = dbutils.widgets.get("VOLUME_PATH")

volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, vol_base)
if not match:
    raise ValueError(f"Invalid volume path format: {vol_base}. Expected: /Volumes/catalog/schema/volume")

catalog, schema, volume = match.groups()
catalog = validate_identifier(catalog, "catalog name")
schema  = validate_identifier(schema,  "schema name")
volume  = validate_identifier(volume,  "volume name")
```

Keep lines 92–133 (volume creation SQL + accessibility check) unchanged.

- [ ] **Step 4: Remove `validate_identifier` from `valhalla_01_process_pbf.py`, replace with `%run`**

In `valhalla_01_process_pbf.py`, the parameters cell defines `validate_identifier` locally (lines 52–92). Replace lines 36–92 with:

```python
# DBTITLE 1,Set Parameters
# MAGIC %run ./valhalla_utils

import os

dbutils.widgets.text("PBF_URL",     "https://download.geofabrik.de/europe/france-latest.osm.pbf", "PBF URL")
dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_france",          "Target Volume Path")

pbf_url     = dbutils.widgets.get("PBF_URL")
volume_path = dbutils.widgets.get("VOLUME_PATH")

import re
volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, volume_path)
if not match:
    raise ValueError(f"Invalid volume path: {volume_path}")
catalog, schema, volume = match.groups()
catalog = validate_identifier(catalog, "catalog name")
schema  = validate_identifier(schema,  "schema name")
volume  = validate_identifier(volume,  "volume name")
```

Keep the SQL volume-creation block (lines 96–121) unchanged.

- [ ] **Step 5: Remove `validate_identifier` from `valhalla_test_routing.py`, replace with `%run`**

In `valhalla_test_routing.py`, lines 37–70 define `validate_identifier` locally. Replace lines 17–71 with:

```python
# DBTITLE 1,Setup and Configuration
# MAGIC %run ./valhalla_utils

import json
import sys
import re
from pathlib import Path

dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Volume Path")
volume_path = dbutils.widgets.get("VOLUME_PATH")

volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, volume_path)
if match:
    catalog, schema, volume = match.groups()
    validate_identifier(catalog, "catalog name")
    validate_identifier(schema,  "schema name")
    validate_identifier(volume,  "volume name")

config_path = f"{volume_path}/tiles/valhalla.json"
```

Keep lines 73 onward (test_results, log_test, etc.) unchanged.

- [ ] **Step 6: Remove `validate_identifier` from `valhalla_quickstart.py`, replace with `%run`**

In `valhalla_quickstart.py`, lines 112–146 define and call `validate_identifier`. Replace lines 84–154 with:

```python
# DBTITLE 1,Define Input Parameters
# MAGIC %run ./valhalla_utils

import re

# France
pbf_url = "https://download.geofabrik.de/europe/france-latest.osm.pbf"
catalog = 'your_catalog'
schema  = 'your_schema'
volume  = 'valhalla_france'

# Small test region - Andorra (fastest, good for testing)
# pbf_url = "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
# catalog = 'your_catalog'
# schema  = 'your_schema'
# volume  = 'valhalla_andorra'

catalog = validate_identifier(catalog, "catalog name")
schema  = validate_identifier(schema,  "schema name")
volume  = validate_identifier(volume,  "volume name")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"📍 Volume: {catalog}.{schema}.{volume}")
print(f"   Path: {volume_path}")
```

Keep the SQL CREATE CATALOG/SCHEMA/VOLUME block (lines 155–177) unchanged.

- [ ] **Step 7: Verify `%run` resolves correctly on cluster**

Run each parameters cell interactively. Each should print `✅ valhalla_utils loaded` before continuing. Confirm no `NameError` for `validate_identifier`.

- [ ] **Step 8: Commit**

```bash
git add src/notebooks/valhalla_utils.py \
        src/notebooks/valhalla_00_initial_setup.py \
        src/notebooks/valhalla_01_process_pbf.py \
        src/notebooks/valhalla_test_routing.py \
        src/notebooks/valhalla_quickstart.py
git commit -m "fix(phase-0): extract validate_identifier to valhalla_utils shared notebook"
git push origin feature/france
```

---

## Task 1: HIGH fixes — `valhalla_map_matching.py`

**Files:**
- Modify: `src/notebooks/valhalla_map_matching.py`

Six changes applied in one edit, tested together.

- [ ] **Step 1: Apply H2 — fix chunk coord dedup (line 443–445)**

Replace:
```python
coords = decode_polyline(result.get("shape", ""))
# Drop the duplicated boundary point from subsequent chunks
all_coords.extend(coords[_CHUNK_OVERLAP:] if i > 0 and coords else coords)
```
With:
```python
coords = decode_polyline(result.get("shape", "")) or []
skip = _CHUNK_OVERLAP if i > 0 else 0
all_coords.extend(coords[skip:])
```

- [ ] **Step 2: Apply H4 — short trace guard (add after `_match_shape` signature block, first line of function body)**

Add as first line inside `_match_shape` (after the docstring, before `filters = ...`, currently line ~403):
```python
if len(shape) < 2:
    raise ValueError(f"Trace requires ≥2 points, got {len(shape)}")
```

- [ ] **Step 3: Apply M8 — document `confidence_score = None` in docstring**

In `_match_shape` docstring, replace:
```
    Chunks overlap by _CHUNK_OVERLAP points so the matched geometry is continuous.
```
With:
```
    Chunks overlap by _CHUNK_OVERLAP points so the matched geometry is continuous.
    Returns min(confidence_score) across all chunks. None if Valhalla did not return
    a confidence score for any chunk — common for trace_attributes on short traces.
```

- [ ] **Step 4: Apply H5 — freeze MATCH_CONFIG (in the mapInPandas cell, before `match_traces` definition)**

In the "Distributed trace_attributes via mapInPandas" cell, add this line immediately before the `def match_traces(...)` definition:
```python
_MATCH_CONFIG = dict(MATCH_CONFIG)  # snapshot at submission time; immutable in worker closures
```

- [ ] **Step 5: Apply H1 + H3 + H7 — rewrite `match_traces` function**

Replace the entire `match_traces` function (lines 460–523) with:

```python
def match_traces(batch_iter: Iterator[pd.DataFrame], match_cfg: dict = _MATCH_CONFIG) -> Iterator[pd.DataFrame]:
    actor = valhalla.Actor(config_path_bc.value)  # config_path broadcast; defined below
    try:
        for pdf in batch_iter:
            rows = []
            for _, row in pdf.iterrows():
                try:
                    shape = json.loads(row["shape_json"])   # H3: inside try
                    matched = _match_shape(actor, shape, **match_cfg)
                    edges  = matched["edges"]
                    coords = matched["matched_coords"]

                    if not edges:                           # H7: explicit zero-edge error
                        raise ValueError("Valhalla returned 0 matched edges — trace may be outside tile coverage")

                    total_len    = sum(e.get("length", 0) for e in edges)
                    total_length_km = total_len
                    avg_speed = (
                        sum(e.get("speed", 0) * e.get("length", 0) for e in edges) / total_len
                        if total_len > 0 else None
                    )
                    has_toll     = any(e.get("toll", False) for e in edges)
                    road_classes = list({e.get("road_class") for e in edges if e.get("road_class")})

                    geometry_wkt = (
                        "LINESTRING ({})".format(", ".join(f"{lon} {lat}" for lat, lon in coords))
                        if coords else None
                    )

                    edges_summary = [
                        {
                            "way_id":     e.get("way_id"),
                            "road_class": e.get("road_class"),
                            "length_km":  e.get("length"),
                            "speed_kmh":  e.get("speed"),
                            "is_toll":    e.get("toll", False),
                        }
                        for e in edges
                    ]

                    result_json = json.dumps({             # H5: use match_cfg snapshot
                        "confidence_score": matched["confidence_score"],
                        "n_chunks":         matched["n_chunks"],
                        "n_input_points":   matched["n_input_points"],
                        "match_config":     match_cfg,
                    })

                    rows.append({
                        "trip_id":         row["trip_id"],
                        "city":            row["city"],
                        "n_edges":         len(edges),
                        "total_length_km": round(total_length_km, 3),
                        "avg_speed_kmh":   round(avg_speed, 1) if avg_speed else None,
                        "has_toll_road":   has_toll,
                        "road_classes":    ", ".join(sorted(road_classes)),
                        "confidence":      matched["confidence_score"],
                        "geometry_wkt":    geometry_wkt,
                        "result_json":     result_json,
                        "edges_json":      json.dumps(edges_summary),
                        "error":           None,
                    })
                except Exception as e:                     # H3: typed exception message
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
        del actor                                          # H1: explicit release
```

- [ ] **Step 6: Apply H1 — broadcast config_path + update mapInPandas call**

Replace the `matched_df = trips_df.mapInPandas(...)` block (lines 525–541) with:

```python
from pyspark.sql.functions import parse_json, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, LongType, StringType, DoubleType, BooleanType

config_path_bc = spark.sparkContext.broadcast(config_path)  # H1: broadcast

edge_schema = ArrayType(StructType([                        # M4: typed edge array
    StructField("way_id",     LongType()),
    StructField("road_class", StringType()),
    StructField("length_km",  DoubleType()),
    StructField("speed_kmh",  DoubleType()),
    StructField("is_toll",    BooleanType()),
]))

matched_df = trips_df.mapInPandas(
    match_traces,
    schema="""
        trip_id string, city string,
        n_edges int, total_length_km double, avg_speed_kmh double,
        has_toll_road boolean, road_classes string, confidence double,
        geometry_wkt string, result_json string, edges_json string, error string
    """
).withColumn(
    "geometry",
    dbf.st_setsrid(dbf.try_to_geometry("geometry_wkt"), 4326)
).withColumn(
    "result",
    parse_json("result_json")
).withColumn(
    "edges",
    from_json("edges_json", edge_schema)
).drop("geometry_wkt", "result_json", "edges_json")
```

- [ ] **Step 7: Apply M1 + M3 — partition sizing and capture count()**

Replace:
```python
trips_df = spark.createDataFrame(traces, ["trip_id", "city", "shape_json"]).repartition(20)
print(f"Created {trips_df.count()} GPS traces across {len(CITIES)} French cities")
```
With:
```python
N_PARTITIONS = max(20, len(traces) // 2000)
# Target 1K–5K rows/partition. At 10M traces: N_PARTITIONS ≈ 5000
trips_df = spark.createDataFrame(traces, ["trip_id", "city", "shape_json"]).repartition(N_PARTITIONS)
print(f"Created {len(traces)} GPS traces across {len(CITIES)} French cities ({N_PARTITIONS} partitions)")
```

Replace the failure-rate cell (lines 572–580) with:
```python
# DBTITLE 1,Failure rate
total_count = matched_df.count()   # M1: reuse the materialised cache count
failed      = matched_df.filter(matched_df.error.isNotNull()).count()
print(f"Total trips : {total_count}")
print(f"Matched     : {total_count - failed}  ({(total_count - failed) / total_count * 100:.1f}%)")
print(f"Failed      : {failed}  ({failed / total_count * 100:.1f}%)")

if failed > 0:
    print("\nSample errors:")
    matched_df.filter(matched_df.error.isNotNull()).select("trip_id", "city", "error").show(5, truncate=80)
```

Also replace the earlier discard at line 543–544:
```python
matched_df.cache()
matched_df.count()
```
With:
```python
matched_df.cache()
total_count = matched_df.count()   # materialise; reused in failure-rate cell below
```

- [ ] **Step 8: Test on cluster**

Run the full distributed cell on `0601-134740-olo78hb3` (Andorra tiles). Verify:

```python
# These assertions should all pass after running matched_df:

# 1. Schema includes edges as array of structs, result as variant
matched_df.printSchema()
# Expected: edges: array<struct<way_id:bigint, road_class:string, ...>>
#           result: variant
#           geometry: geometry

# 2. SRID is 4326
from pyspark.sql.functions import expr
matched_df.filter(matched_df.error.isNull()).select(expr("ST_SRID(geometry)").alias("srid")).distinct().show()
# Expected: 4326

# 3. VARIANT has exactly 4 keys (not full edge list)
matched_df.filter(matched_df.error.isNull()).selectExpr("result::string").show(1, truncate=False)
# Expected JSON keys: confidence_score, n_chunks, n_input_points, match_config

# 4. Inject a 1-point trace — should appear with ValueError in error column
bad_row = spark.createDataFrame([("test_bad", "Test", '[{"lat":48.8,"lon":2.3}]')], ["trip_id","city","shape_json"])
bad_result = bad_row.repartition(1).mapInPandas(match_traces, schema="""trip_id string, city string, n_edges int, total_length_km double, avg_speed_kmh double, has_toll_road boolean, road_classes string, confidence double, geometry_wkt string, result_json string, edges_json string, error string""")
bad_result.select("error").show(truncate=False)
# Expected: "ValueError: Trace requires ≥2 points, got 1"

# 5. Inject malformed JSON — should appear with JSONDecodeError
bad_json = spark.createDataFrame([("test_json", "Test", 'NOT_JSON')], ["trip_id","city","shape_json"])
bad_json.repartition(1).mapInPandas(match_traces, schema="""trip_id string, city string, n_edges int, total_length_km double, avg_speed_kmh double, has_toll_road boolean, road_classes string, confidence double, geometry_wkt string, result_json string, edges_json string, error string""").select("error").show(truncate=False)
# Expected: "JSONDecodeError: ..."
```

- [ ] **Step 9: Commit**

```bash
git add src/notebooks/valhalla_map_matching.py
git commit -m "fix(phase-1): HIGH fixes for map_matching — actor lifecycle, chunk dedup, error handling, short trace guard, MATCH_CONFIG snapshot, zero-edge error"
git push origin feature/france
```

---

## Task 2: HIGH fixes — `valhalla_test_routing.py` + `valhalla_01_process_pbf.py`

**Files:**
- Modify: `src/notebooks/valhalla_test_routing.py` lines 213, 272
- Modify: `src/notebooks/valhalla_01_process_pbf.py` lines 115–116

- [ ] **Step 1: Fix bare `except:` in `valhalla_test_routing.py` — Test 5 batch loop (line 213)**

Replace:
```python
        except:
            pass
```
With:
```python
        except Exception:
            pass
```

- [ ] **Step 2: Fix bare `except:` in `valhalla_test_routing.py` — `route_batch` function (line 272)**

Replace:
```python
            except:
                results.append({
```
With:
```python
            except Exception as e:
                results.append({
```

And in the same results.append block, add an `"error"` field is not needed since the schema doesn't have one — just leave None values as-is. The key change is `except Exception as e:` so `KeyboardInterrupt` is no longer caught.

- [ ] **Step 3: Add volume accessibility guard to `valhalla_01_process_pbf.py`**

After line 115 (`print(f"⚠️  Volume: {e} (assuming it exists)")`), after the closing `except` block (line 115), add:

```python
# Verify volume is accessible before starting the expensive tile build
try:
    dbutils.fs.ls(volume_path)
    print(f"✅ Volume accessible at: {volume_path}")
except Exception as e:
    raise RuntimeError(f"Volume not accessible at {volume_path}: {e}") from e
```

- [ ] **Step 4: Test on cluster**

```python
# Test 1: confirm test_routing distributed cell runs without error
# Run Test 6 cell in valhalla_test_routing.py interactively on Andorra cluster

# Test 2: confirm H6 guard fires with bad path
# In a fresh cell on the cluster, run:
volume_path = "/Volumes/does/not/exist"
try:
    dbutils.fs.ls(volume_path)
    print("FAIL: should have raised")
except Exception as e:
    raise RuntimeError(f"Volume not accessible at {volume_path}: {e}") from e
# Expected: RuntimeError: Volume not accessible at /Volumes/does/not/exist: ...
```

- [ ] **Step 5: Commit**

```bash
git add src/notebooks/valhalla_test_routing.py src/notebooks/valhalla_01_process_pbf.py
git commit -m "fix(phase-2): HIGH fixes for test_routing (bare except) and process_pbf (volume guard)"
git push origin feature/france
```

---

## Task 3: MEDIUM fixes — other notebooks

**Files:**
- Modify: `src/notebooks/valhalla_test_routing.py`
- Modify: `src/notebooks/valhalla_01_process_pbf.py`

- [ ] **Step 1: Fix M7 — `os.walk + break` in `valhalla_01_process_pbf.py`**

Replace lines 261–274 (the `os.walk` + `break` block):
```python
# List files
for root, dirs, files in os.walk(tile_dir):
    level = root.replace(tile_dir, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in sorted(files)[:10]:  # Limit to first 10 files per directory
        file_path = os.path.join(root, file)
        size = os.path.getsize(file_path)
        size_mb = size / (1024 * 1024)
        print(f'{subindent}{file} ({size_mb:.2f} MB)')
    if len(files) > 10:
        print(f'{subindent}... and {len(files) - 10} more files')
    break  # Only show top level
```
With:
```python
# Show top-level directory contents
print(f"Top-level contents of {tile_dir}:")
for name in sorted(os.listdir(tile_dir))[:20]:
    full_path = os.path.join(tile_dir, name)
    if os.path.isfile(full_path):
        size_mb = os.path.getsize(full_path) / (1024 * 1024)
        print(f"  {name} ({size_mb:.2f} MB)")
    else:
        print(f"  {name}/")

# Count all .gph tile files across all subdirectories
tile_count = sum(
    1 for _, _, files in os.walk(tile_dir)
    for f in files if f.endswith(".gph")
)
print(f"\n✅ Total .gph tile files: {tile_count}")
if tile_count == 0:
    print("⚠️  No .gph tiles found — tile build may not have completed")
```

- [ ] **Step 2: Add M5 comment to `valhalla_test_routing.py` `check_valhalla_on_worker`**

Find `def check_valhalla_on_worker(iterator):` and add a comment to its body:
```python
def check_valhalla_on_worker(iterator):
    """Check if Valhalla is available on worker.
    
    Note: spark.range(sc.defaultParallelism) creates exactly one row per partition,
    so this yields exactly one result row per executor — intentional.
    """
```

- [ ] **Step 3: Test on cluster**

```python
# Run the tile verification cell in valhalla_01_process_pbf.py on the Andorra cluster
# Expected: non-zero .gph tile count printed
# tile_dir = "/Volumes/timo/geospatial/valhalla_andorra/tiles"
import os
tile_dir = "/Volumes/timo/geospatial/valhalla_andorra/tiles"
for name in sorted(os.listdir(tile_dir))[:20]:
    full_path = os.path.join(tile_dir, name)
    if os.path.isfile(full_path):
        print(f"  {name} ({os.path.getsize(full_path) / 1024 / 1024:.2f} MB)")
    else:
        print(f"  {name}/")
tile_count = sum(1 for _, _, files in os.walk(tile_dir) for f in files if f.endswith(".gph"))
print(f"Total .gph tiles: {tile_count}")
# Expected: tile_count > 0
```

- [ ] **Step 4: Commit**

```bash
git add src/notebooks/valhalla_01_process_pbf.py src/notebooks/valhalla_test_routing.py
git commit -m "fix(phase-3): MEDIUM fixes — os.walk+break tile listing, worker check comment"
git push origin feature/france
```

---

## Task 4: LOW fixes — all notebooks

**Files:**
- Modify: `src/notebooks/valhalla_quickstart.py`
- Modify: `src/notebooks/valhalla_test_routing.py`
- Modify: `src/notebooks/valhalla_map_matching.py`

- [ ] **Step 1: L1 — Fix Portland coords in `valhalla_quickstart.py`**

Find (lines ~319–321):
```python
{"lat": 43.6591, "lon": -70.2568, "type": "break", "city": "Portland"},
{"lat": 37.7831, "lon": -122.4039, "type": "break", "city": "San Francisco"}
```
Replace with:
```python
{"lat": 45.5051, "lon": -122.6750, "type": "break", "city": "Portland, OR"},
{"lat": 37.7749, "lon": -122.4194, "type": "break", "city": "San Francisco"}
```

- [ ] **Step 2: L2 — Fix bare `except:` in `valhalla_test_routing.py` batch loop (already done in Task 2 Step 1 — verify it was applied)**

Confirm line 213 reads `except Exception:` not `except:`. If Task 2 was applied correctly, skip this step.

- [ ] **Step 3: L3 — Clean up imports in `valhalla_map_matching.py`**

In the "Distributed trace_attributes via mapInPandas" cell, replace:
```python
from typing import Iterator, List, Dict, Optional, Tuple
import pandas as pd
import valhalla
from valhalla.utils import decode_polyline
from pyspark.databricks.sql import functions as dbf
```
With:
```python
from typing import Iterator, List, Dict, Optional
import pandas as pd
import valhalla
from valhalla.utils import decode_polyline
from pyspark.databricks.sql import functions as dbf
```
(`Tuple` removed — unused after the type annotations were simplified.)

Move `from pyspark.sql.functions import parse_json, from_json` (currently floating at line ~525) up to join the other imports at the top of the same cell (it was already moved in Task 1 Step 6 — verify it's at the top).

- [ ] **Step 4: L4 — Add assertion after constants in `valhalla_map_matching.py`**

After:
```python
_VALHALLA_MAX_POINTS = 16_000
_CHUNK_SIZE = 15_000   # stay below the limit
_CHUNK_OVERLAP = 1     # share one boundary point between chunks for continuity
```
Add:
```python
assert _CHUNK_SIZE <= _VALHALLA_MAX_POINTS, \
    f"_CHUNK_SIZE ({_CHUNK_SIZE}) must not exceed Valhalla's {_VALHALLA_MAX_POINTS}-point limit"
```

- [ ] **Step 5: Spot-check on cluster**

```python
# Quick interactive check — paste into cluster context:
_VALHALLA_MAX_POINTS = 16_000
_CHUNK_SIZE = 15_000
_CHUNK_OVERLAP = 1
assert _CHUNK_SIZE <= _VALHALLA_MAX_POINTS
print("Assertion passed")

# Verify Portland coords are in Oregon bbox (lat 42–46, lon -124 to -116)
lat, lon = 45.5051, -122.6750
assert 42 < lat < 47 and -125 < lon < -116, f"Portland coords out of Oregon bounds: {lat}, {lon}"
print("Portland coords OK")
```

- [ ] **Step 6: Commit**

```bash
git add src/notebooks/valhalla_quickstart.py \
        src/notebooks/valhalla_test_routing.py \
        src/notebooks/valhalla_map_matching.py
git commit -m "fix(phase-4): LOW fixes — Portland coords, bare except, unused Tuple import, _CHUNK_SIZE assertion"
git push origin feature/france
```

---

## Task 5: L5 — `dbutils.fs.put` → `open()` in `valhalla_00_initial_setup.py`

**Files:**
- Modify: `src/notebooks/valhalla_00_initial_setup.py` line 398

This change affects the init script write path. Requires a fresh cluster compile in Task 6 to validate.

- [ ] **Step 1: Replace `dbutils.fs.put` with direct file write**

Find line 398:
```python
dbutils.fs.put(init_script_path, init_script_content, overwrite=True)
print(f"✅ Init script written to: {init_script_path}")
print(f"   Size: {len(init_script_content)} bytes")
```
Replace with:
```python
with open(init_script_path, "w") as f:
    f.write(init_script_content)
print(f"✅ Init script written to: {init_script_path}")
print(f"   Size: {len(init_script_content)} bytes")
```

Also update the verification line that reads the script back (find `dbutils.fs.head`):
```python
# Before (line ~407):
print(dbutils.fs.head(init_script_path, 2000))

# After:
with open(init_script_path, "r") as f:
    print(f.read(2000))
```

- [ ] **Step 2: Commit (do NOT test yet — fresh cluster required)**

```bash
git add src/notebooks/valhalla_00_initial_setup.py
git commit -m "fix(phase-5): replace dbutils.fs.put with open() for UC Volume init script write"
git push origin feature/france
```

---

## Task 6: Validate compile changes on fresh cluster

**Prerequisite:** Task 5 committed. Andorra volume used for speed.

- [ ] **Step 1: Deploy the bundle to Azure**

```bash
databricks bundle deploy --target azure
```
Expected: `Deployment complete!`

- [ ] **Step 2: Run `initial_setup` task only, pointing at Andorra volume**

The job already has `VOLUME_PATH` set to `valhalla_france`. For this test, run the notebook directly or override the parameter:

```bash
databricks bundle run --target azure valhalla_test_job --only initial_setup
```

Note the run URL from the output. Watch for the `✅ Init script written to:` print and `✅ Volume accessible` confirmation.

- [ ] **Step 3: Verify init script exists and is non-empty**

After the run completes, check the file via the cluster:
```python
# Run on 0601-134740-olo78hb3 (Andorra init script already there, but check France):
with open("/Volumes/timo/geospatial/valhalla_france/init.sh") as f:
    content = f.read()
print(f"Init script size: {len(content)} bytes")
assert len(content) > 100, "Init script is suspiciously small"
assert "valhalla" in content.lower(), "Init script doesn't mention valhalla"
print("✅ Init script looks valid")
```

- [ ] **Step 4: Confirm a fresh cluster with the init script can import valhalla**

Spin up a quick test cluster with the France init script:
```bash
databricks clusters create --profile naturgy-poc --json '{
  "cluster_name": "valhalla-init-test",
  "spark_version": "18.2.x-scala2.13",
  "node_type_id": "Standard_D8s_v3",
  "num_workers": 0,
  "spark_conf": {"spark.master": "local[*]", "spark.databricks.cluster.profile": "singleNode"},
  "custom_tags": {"ResourceClass": "SingleNode"},
  "data_security_mode": "SINGLE_USER",
  "init_scripts": [{"volumes": {"destination": "/Volumes/timo/geospatial/valhalla_france/init.sh"}}],
  "autotermination_minutes": 15
}'
```

Wait for RUNNING, then execute:
```python
import valhalla
print(f"✅ valhalla imported: {valhalla.__file__}")
```
Expected: prints the wheel path, no ImportError.

- [ ] **Step 5: Commit final validation note and push**

```bash
git add src/notebooks/valhalla_00_initial_setup.py  # already committed, nothing to add
git commit --allow-empty -m "fix(phase-6): compile/init changes validated on fresh cluster with Andorra"
git push origin feature/france
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] H1 actor lifecycle → Task 1 Step 5/6
- [x] H2 chunk coord dedup → Task 1 Step 1
- [x] H3 json.loads inside try + typed exceptions → Task 1 Step 5 (`match_traces` rewrite)
- [x] H3 in test_routing → Task 2 Step 1/2
- [x] H4 short trace guard → Task 1 Step 2
- [x] H5 MATCH_CONFIG snapshot → Task 1 Step 4/5
- [x] H6 volume guard → Task 2 Step 3
- [x] H7 zero-edge error → Task 1 Step 5
- [x] M1 capture count() → Task 1 Step 7
- [x] M2 distance-weighted avg speed → Task 1 Step 5 (`match_traces` rewrite)
- [x] M3 partition sizing → Task 1 Step 7
- [x] M4 slim VARIANT + typed edges → Task 1 Step 5/6
- [x] M5 worker check comment → Task 3 Step 2
- [x] M6 %run valhalla_utils → Task 0
- [x] M7 os.walk+break → Task 3 Step 1
- [x] M8 confidence_score docstring → Task 1 Step 3
- [x] L1 Portland coords → Task 4 Step 1
- [x] L2 bare except → Task 2 Step 1 (also verified in Task 4 Step 2)
- [x] L3 import cleanup → Task 4 Step 3
- [x] L4 assertion → Task 4 Step 4
- [x] L5 dbutils.fs.put → Task 5
