# Databricks notebook source
# MAGIC %md
# MAGIC # Valhalla Routing Tests
# MAGIC
# MAGIC Automated testing notebook that verifies:
# MAGIC 1. Valhalla Python bindings are available
# MAGIC 2. Configuration and tiles are accessible
# MAGIC 3. Single-node routing works
# MAGIC 4. Distributed routing with Spark works
# MAGIC 5. Performance meets expectations
# MAGIC
# MAGIC **This notebook is designed to run as part of the valhalla_test_job DAB workflow.**

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
import json
import sys
import re
from pathlib import Path

dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Volume Path")
volume_path = dbutils.widgets.get("VOLUME_PATH")

# Validate volume path format and identifiers
volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, volume_path)

if match:
    catalog, schema, volume = match.groups()
    
    # Unity Catalog identifier rules (non-delimited/unquoted identifiers):
    # - Must start with letter (A-Z, a-z) or underscore (_)
    # - Can contain letters, digits (0-9), and underscores
    # - Maximum 255 characters
    # Reference: https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html
    def validate_identifier(name, identifier_type="identifier"):
        """
        Validate Unity Catalog identifier for security.
        
        Only allows non-delimited identifiers (no backticks) to prevent SQL injection.
        Follows Databricks naming rules for catalogs, schemas, and volumes.
        """
        if not name:
            raise ValueError(f"{identifier_type} cannot be empty")
        
        if len(name) > 255:
            raise ValueError(f"{identifier_type} too long: {len(name)} chars (max 255)")
        
        # Must start with letter or underscore
        if not re.match(r'^[a-zA-Z_]', name):
            raise ValueError(
                f"Invalid {identifier_type}: '{name}'. "
                f"Must start with a letter (A-Z, a-z) or underscore (_)."
            )
        
        # Can only contain letters, digits, and underscores
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(
                f"Invalid {identifier_type}: '{name}'. "
                f"Can only contain letters, digits, and underscores. "
                f"Hyphens and special characters require backtick escaping (not supported for security)."
            )
        
        return name
    
    # Validate identifiers
    validate_identifier(catalog, "catalog name")
    validate_identifier(schema, "schema name")
    validate_identifier(volume, "volume name")

config_path = f"{volume_path}/tiles/valhalla.json"

print(f"🧪 Valhalla Test Suite")
print(f"=" * 80)
print(f"Volume: {volume_path}")
print(f"Config: {config_path}")
print(f"Cluster: {sc.defaultParallelism} cores")
print("")

test_results = []

def log_test(name, passed, message=""):
    """Log test result"""
    status = "✅ PASS" if passed else "❌ FAIL"
    test_results.append({"name": name, "passed": passed, "message": message})
    print(f"{status}: {name}")
    if message:
        print(f"   {message}")
    print("")
    if not passed:
        # Don't raise immediately - collect all results first
        pass

# COMMAND ----------

# DBTITLE 1,Test 1: Verify Valhalla Import
print("Test 1: Verify Valhalla Import")
print("-" * 80)

try:
    import valhalla
    from valhalla import Actor
    log_test("Import Valhalla", True, f"Module: {valhalla.__file__}")
except ImportError as e:
    log_test("Import Valhalla", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 2: Verify Config and Tiles Exist
print("Test 2: Verify Config and Tiles Exist")
print("-" * 80)

import os

config_exists = os.path.exists(config_path)
if not config_exists:
    log_test("Config file exists", False, f"Not found: {config_path}")
else:
    log_test("Config file exists", True, config_path)

# Check for tiles
tile_dir = f"{volume_path}/tiles"

if not os.path.exists(tile_dir):
    log_test("Routing tiles exist", False, f"Tile directory not found: {tile_dir}")
else:
    # Count actual tile files (.gph)
    tile_count = 0
    tile_levels_found = set()
    
    for root, dirs, files in os.walk(tile_dir):
        for f in files:
            if f.endswith('.gph'):
                tile_count += 1
                # Extract level from path (e.g., /tiles/0/xxx -> level 0)
                rel_path = root.replace(tile_dir, "").lstrip("/")
                if rel_path and rel_path[0].isdigit():
                    tile_levels_found.add(rel_path.split("/")[0])
    
    if tile_count > 0:
        levels_str = ", ".join(sorted(tile_levels_found))
        log_test("Routing tiles exist", True, f"Found {tile_count} tile files in levels: {levels_str}")
    else:
        log_test("Routing tiles exist", False, "No .gph tile files found")

# COMMAND ----------

# DBTITLE 1,Test 3: Initialize Actor
print("Test 3: Initialize Actor")
print("-" * 80)

try:
    actor = Actor(config_path)
    status = actor.status()
    log_test("Initialize Actor", True, f"Status: {status}")
except Exception as e:
    log_test("Initialize Actor", False, str(e))
    actor = None

# COMMAND ----------

# DBTITLE 1,Test 4: Single Route Query
print("Test 4: Single Route Query")
print("-" * 80)

# Test coordinates for Andorra
query = {
    "locations": [
        {"lat": 42.5078, "lon": 1.5211, "type": "break", "city": "Andorra la Vella"},
        {"lat": 42.5562, "lon": 1.5336, "type": "break", "city": "Ordino"}
    ],
    "costing": "auto",
    "directions_options": {"units": "kilometers"}
}

if actor is None:
    log_test("Single route query", False, "Actor not initialized (skipped)")
else:
    try:
        result = actor.route(query)
        summary = result.get("trip", {}).get("summary", {})
        distance = summary.get("length", 0)
        time = summary.get("time", 0)
        
        if distance > 0 and time > 0:
            log_test("Single route query", True, f"Distance: {distance:.1f}km, Time: {time/60:.1f}min")
        else:
            log_test("Single route query", False, f"Invalid route result: distance={distance}, time={time}")
    except Exception as e:
        log_test("Single route query", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 5: Multiple Routes (Batch Performance)
print("Test 5: Multiple Routes (Batch Performance)")
print("-" * 80)

if actor is None:
    log_test("Batch routing", False, "Actor not initialized (skipped)")
else:
    import time
    
    num_tests = 20
    start_time = time.time()
    success_count = 0
    
    for i in range(num_tests):
        try:
            result = actor.route(query)
            if "trip" in result:
                success_count += 1
        except:
            pass
    
    elapsed = time.time() - start_time
    throughput = num_tests / elapsed if elapsed > 0 else 0
    
    if success_count == num_tests:
        log_test("Batch routing", True, f"{success_count}/{num_tests} routes, {throughput:.1f} routes/sec")
    else:
        log_test("Batch routing", False, f"Only {success_count}/{num_tests} succeeded")

# COMMAND ----------

# DBTITLE 1,Test 6: Distributed Routing with Spark
print("Test 6: Distributed Routing with Spark")
print("-" * 80)

from typing import Iterator
import pandas as pd
import valhalla

# Create small test dataset (20 random OD pairs in Andorra bounds)
test_data = []
import random
for i in range(20):
    test_data.append((
        42.428 + random.random() * (42.655 - 42.428),
        1.413 + random.random() * (1.786 - 1.413),
        42.428 + random.random() * (42.655 - 42.428),
        1.413 + random.random() * (1.786 - 1.413)
    ))

test_df = spark.createDataFrame(test_data, ["orig_lat", "orig_lon", "dest_lat", "dest_lon"]).repartition(4)

def route_batch(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Route a batch of OD pairs"""
    actor = valhalla.Actor(config_path)
    
    for pdf in batch_iter:
        results = []
        for _, row in pdf.iterrows():
            try:
                query = {
                    "locations": [
                        {"lat": row.orig_lat, "lon": row.orig_lon},
                        {"lat": row.dest_lat, "lon": row.dest_lon}
                    ],
                    "costing": "auto"
                }
                route = actor.route(query)
                summary = route["trip"]["legs"][0]["summary"]
                results.append({
                    "orig_lat": row.orig_lat,
                    "orig_lon": row.orig_lon,
                    "dest_lat": row.dest_lat,
                    "dest_lon": row.dest_lon,
                    "distance_km": summary["length"],
                    "time_min": summary["time"] / 60
                })
            except:
                results.append({
                    "orig_lat": row.orig_lat,
                    "orig_lon": row.orig_lon,
                    "dest_lat": row.dest_lat,
                    "dest_lon": row.dest_lon,
                    "distance_km": None,
                    "time_min": None
                })
        yield pd.DataFrame(results)

try:
    result_df = test_df.mapInPandas(route_batch, schema="""
        orig_lat double, orig_lon double,
        dest_lat double, dest_lon double,
        distance_km double, time_min double
    """)
    
    count = result_df.count()
    successful = result_df.filter(result_df.distance_km.isNotNull()).count()
    
    # Success if at least 50% of routes work (some random points may be unreachable)
    if successful >= count * 0.5:
        log_test("Distributed routing", True, f"{successful}/{count} routes succeeded ({successful/count*100:.0f}%)")
    else:
        log_test("Distributed routing", False, f"Only {successful}/{count} routes succeeded ({successful/count*100:.0f}%)")
except Exception as e:
    log_test("Distributed routing", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 7: Verify Init Script on Workers
print("Test 7: Verify Init Script on Workers")
print("-" * 80)

def check_valhalla_on_worker(iterator):
    """Check if Valhalla is available on worker"""
    import os
    import pandas as pd
    
    try:
        import valhalla
        result = {
            "valhalla_available": True,
            "valhalla_path": valhalla.__file__,
            "binaries_exist": os.path.exists("/usr/local/bin/valhalla_service")
        }
    except ImportError:
        result = {
            "valhalla_available": False,
            "valhalla_path": None,
            "binaries_exist": False
        }
    
    for _ in iterator:
        yield pd.DataFrame([result])

# Create a single-row DataFrame to distribute work
check_df = spark.range(sc.defaultParallelism).mapInPandas(
    check_valhalla_on_worker,
    schema="valhalla_available boolean, valhalla_path string, binaries_exist boolean"
)

results = check_df.collect()
all_workers_ok = all(r.valhalla_available for r in results)

if all_workers_ok:
    log_test("Init script on workers", True, f"Valhalla available on all {len(results)} workers")
else:
    failed_count = sum(1 for r in results if not r.valhalla_available)
    log_test("Init script on workers", False, f"{failed_count}/{len(results)} workers missing Valhalla")

# COMMAND ----------

# DBTITLE 1,Test Summary
print("=" * 80)
print("🧪 TEST SUMMARY")
print("=" * 80)

passed = sum(1 for t in test_results if t["passed"])
failed = len(test_results) - passed

print(f"\nTotal tests: {len(test_results)}")
print(f"✅ Passed: {passed}")
print(f"❌ Failed: {failed}")

if failed > 0:
    print("\n❌ Failed tests:")
    for t in test_results:
        if not t["passed"]:
            print(f"  • {t['name']}: {t['message']}")
    
    print("\n" + "=" * 80)
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "passed": passed,
        "failed": failed,
        "results": test_results
    }))
else:
    print("\n🎉 All tests passed!")
    print("=" * 80)
    dbutils.notebook.exit(json.dumps({
        "status": "SUCCESS",
        "passed": passed,
        "failed": failed,
        "results": test_results
    }))

# COMMAND ----------
