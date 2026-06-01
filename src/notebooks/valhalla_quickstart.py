# Databricks notebook source
# MAGIC %md
# MAGIC # Valhalla Quickstart: Routing on Databricks
# MAGIC
# MAGIC This notebook serves as a **single-node Valhalla quickstart** for routing based on OpenStreetMap `.pbf` data.
# MAGIC
# MAGIC **Tested with:**
# MAGIC - **DBR 18.0.x-scala2.13** (standard & Photon) ✅
# MAGIC - **DBR 17.3.x-scala2.13** (LTS, standard & Photon) ✅
# MAGIC - **Single-node cluster** (n2-highmem-16 or larger recommended)
# MAGIC - **Dedicated access mode** (no isolation shared)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC 1. **Installs and compiles Valhalla** (via `valhalla_00_initial_setup.py`), persisting the binaries and Python wheel.
# MAGIC 2. **Creates an init script** in a shared volume. This script can later be used across all cluster nodes.
# MAGIC 3. **Downloads and processes a `.pbf` file** (via `valhalla_01_process_pbf.py`), building tiles for routing.
# MAGIC 4. **Initializes a Valhalla actor** and executes sample route queries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Notes
# MAGIC
# MAGIC There are **two ways** to use Valhalla on Databricks:
# MAGIC
# MAGIC ### 1. **Driver-only Execution (This Notebook)**
# MAGIC - Everything runs on the **driver node only**
# MAGIC - Suitable for quickstart, local prototyping, and interactive testing
# MAGIC - You manually run the installation, then test routing
# MAGIC
# MAGIC ### 2. **Cluster-wide via Init Script (Next Notebook)**
# MAGIC - All cluster **workers** have Valhalla and routing tiles available
# MAGIC - Enables **parallel routing at scale using Spark**
# MAGIC - Assumes the init script created here has been applied to the cluster
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Compatibility Notes
# MAGIC
# MAGIC ❌ **Not compatible with Databricks Connect or Shared Compute Mode**  
# MAGIC This approach requires a standard, dedicated compute cluster. It will **not work in**:
# MAGIC - **Shared mode clusters**
# MAGIC - **Serverless environments**
# MAGIC - **Databricks Connect-based local development**
# MAGIC
# MAGIC ✅ For routing in those environments, use **PySpark UDFs with Python routing libraries** such as `osmnx`, `openrouteservice`, or similar.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Apply the Init Script in a Cluster
# MAGIC
# MAGIC Once `valhalla_01_process_pbf.py` has run, the init script will be saved to the volume path, e.g.:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/my_catalog/my_schema/my_volume/init.sh
# MAGIC ```
# MAGIC
# MAGIC To use it across all nodes:
# MAGIC
# MAGIC 1. Open your **Compute** settings in Databricks  
# MAGIC 2. Go to **Advanced > Init Scripts**  
# MAGIC 3. Click **Add** and select **Volume** path:
# MAGIC    ```
# MAGIC    /Volumes/my_catalog/my_schema/my_volume/init.sh
# MAGIC    ```
# MAGIC 4. Restart the cluster
# MAGIC
# MAGIC 📘 [Databricks Docs – Configure cluster init scripts](https://docs.databricks.com/clusters/init-scripts.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC - This quickstart runs **locally on the driver**
# MAGIC - The **next notebook** (distributed routing) assumes the init script has been applied and shows **distributed routing**
# MAGIC - The processing step ensures tiles and binaries are persisted and reusable
# MAGIC
# MAGIC ✅ Good for getting started  
# MAGIC 🚀 For scale, use the cluster-wide init script in production

# COMMAND ----------

# DBTITLE 1,Define Input Parameters
# Example configurations for different regions
# Uncomment the region you want to use:

# Small test region - Andorra (fastest, good for testing)
pbf_url = "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
catalog = 'your_catalog'
schema = 'your_schema'
volume = 'valhalla_andorra'

# Medium region - Spain
# pbf_url = "https://download.geofabrik.de/europe/spain-latest.osm.pbf"
# catalog = 'your_catalog'
# schema = 'your_schema'
# volume = 'valhalla_spain'

# Large region - USA (takes 1-3 hours)
# pbf_url = "https://download.geofabrik.de/north-america/us-latest.osm.pbf"
# catalog = 'your_catalog'
# schema = 'your_schema'
# volume = 'valhalla_us'

# Validate identifiers to prevent SQL injection
# Unity Catalog identifier rules (non-delimited/unquoted identifiers):
# - Must start with letter (A-Z, a-z) or underscore (_)
# - Can contain letters, digits (0-9), and underscores
# - Maximum 255 characters
# Reference: https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html
import re

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

catalog = validate_identifier(catalog, "catalog name")
schema = validate_identifier(schema, "schema name")
volume = validate_identifier(volume, "volume name")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

print(f"📍 Configuration:")
print(f"   PBF URL: {pbf_url}")
print(f"   Volume: {catalog}.{schema}.{volume}")
print(f"   Path: {volume_path}")

# Ensure catalog, schema, and volume exist
print(f"\n🔧 Ensuring environment exists...")

# Create catalog (may fail if no permissions)
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    print(f"✅ Catalog {catalog} ready")
except Exception as e:
    print(f"⚠️  Catalog: {e} (assuming it exists)")

# Create schema
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"✅ Schema {catalog}.{schema} ready")
except Exception as e:
    print(f"⚠️  Schema: {e} (assuming it exists)")

# Create volume
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    print(f"✅ Volume {catalog}.{schema}.{volume} ready")
except Exception as e:
    raise Exception(f"Failed to create volume: {e}")

# COMMAND ----------

# DBTITLE 1,Install and Cache Valhalla Binaries
print("🔨 Running Valhalla installation and compilation...")
print("   This will take 15-30 minutes on n2-highmem-16")
print("")

dbutils.notebook.run(
    path="./valhalla_00_initial_setup", 
    timeout_seconds=3600,  # 1 hour timeout
    arguments={"VOLUME_PATH": volume_path}
)

print("✅ Valhalla installation complete!")

# COMMAND ----------

# DBTITLE 1,Process PBF and Build Tiles
print("🗺️  Processing OSM PBF file and building routing tiles...")
print(f"   Source: {pbf_url}")
print(f"   This may take 10-60 minutes depending on region size")
print("")

dbutils.notebook.run(
    path="./valhalla_01_process_pbf",
    timeout_seconds=7200,  # 2 hour timeout
    arguments={
        "VOLUME_PATH": volume_path,
        "PBF_URL": pbf_url
    }
)

print("✅ PBF processing complete!")

# COMMAND ----------

# DBTITLE 1,Install Valhalla Python Package
from pathlib import Path

# Find the wheel file
whl_dir = Path(f"{volume_path}/whl")
whl_files = list(whl_dir.glob("*.whl"))

if not whl_files:
    raise FileNotFoundError(f"No wheel files found in {whl_dir}")

# Get the most recent wheel
whl_path = str(max(whl_files, key=lambda f: f.stat().st_mtime))

print(f"📦 Installing Valhalla wheel: {whl_path}")
print("")

# COMMAND ----------

%pip install {whl_path}

# COMMAND ----------

# DBTITLE 1,Import Valhalla
import valhalla

print(f"✅ Valhalla imported successfully")
print(f"   Module: {valhalla.__file__}")

# COMMAND ----------

# DBTITLE 1,Initialize Actor with Config
import json

config_path = f"{volume_path}/tiles/valhalla.json"

print(f"🔧 Initializing Valhalla actor...")
print(f"   Config: {config_path}")

actor = valhalla.Actor(config_path)

print(f"✅ Actor initialized")
print("")
print("📊 Actor status:")
print(actor.status())

# COMMAND ----------

# DBTITLE 1,Run Sample Route Queries
# Adjust coordinates based on your region

# Example 1: Andorra
if "andorra" in volume.lower():
    queries = [
        {
            "name": "Andorra la Vella to Ordino",
            "locations": [
                {"lat": 42.5078, "lon": 1.5211, "type": "break", "city": "Andorra la Vella"},
                {"lat": 42.5562, "lon": 1.5336, "type": "break", "city": "Ordino"}
            ]
        },
        {
            "name": "Pas de la Casa to Sant Julià de Lòria",
            "locations": [
                {"lat": 42.5425, "lon": 1.7336, "type": "break", "city": "Pas de la Casa"},
                {"lat": 42.4638, "lon": 1.4911, "type": "break", "city": "Sant Julià"}
            ]
        }
    ]

# Example 2: Spain
elif "spain" in volume.lower():
    queries = [
        {
            "name": "Barcelona to Madrid",
            "locations": [
                {"lat": 41.3851, "lon": 2.1734, "type": "break", "city": "Barcelona"},
                {"lat": 40.4168, "lon": -3.7038, "type": "break", "city": "Madrid"}
            ]
        },
        {
            "name": "Valencia to Seville",
            "locations": [
                {"lat": 39.4699, "lon": -0.3763, "type": "break", "city": "Valencia"},
                {"lat": 37.3891, "lon": -5.9845, "type": "break", "city": "Seville"}
            ]
        }
    ]

# Example 3: USA
elif "us" in volume.lower():
    queries = [
        {
            "name": "Portland to San Francisco",
            "locations": [
                {"lat": 43.6591, "lon": -70.2568, "type": "break", "city": "Portland"},
                {"lat": 37.7749, "lon": -122.4194, "type": "break", "city": "San Francisco"}
            ]
        },
        {
            "name": "New York to Boston",
            "locations": [
                {"lat": 40.7128, "lon": -74.0060, "type": "break", "city": "New York"},
                {"lat": 42.3601, "lon": -71.0589, "type": "break", "city": "Boston"}
            ]
        }
    ]
else:
    # Generic fallback
    queries = [
        {
            "name": "Generic test route",
            "locations": [
                {"lat": 40.0, "lon": -3.0, "type": "break"},
                {"lat": 41.0, "lon": -2.0, "type": "break"}
            ]
        }
    ]

# Run all queries
print("🧪 Testing sample routes:")
print("=" * 80)

for i, q in enumerate(queries, 1):
    print(f"\n{i}. {q['name']}")
    
    query = {
        "locations": q["locations"],
        "costing": "auto",
        "directions_options": {"units": "kilometers"}
    }
    
    try:
        result = actor.route(query)
        summary = result.get("trip", {}).get("summary", {})
        distance = summary.get("length", 0)
        time_sec = summary.get("time", 0)
        time_min = time_sec / 60
        
        print(f"   ✅ Success")
        print(f"   📏 Distance: {distance:.1f} km")
        print(f"   ⏱️  Time: {time_min:.1f} minutes")
        
    except Exception as e:
        print(f"   ❌ Failed: {str(e)}")

print("")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Detailed Route Example with Maneuvers
# Get a detailed route with turn-by-turn directions

query = queries[0]  # Use first query from previous cell

route_query = {
    "locations": query["locations"],
    "costing": "auto",
    "directions_options": {
        "units": "kilometers",
        "language": "en-US"
    },
    "shape_format": "polyline6"
}

print(f"🗺️  Detailed route: {query['name']}")
print("=" * 80)

result = actor.route(route_query)

# Pretty print the full result
print(json.dumps(result, indent=2))

# COMMAND ----------

# DBTITLE 1,Extract and Display Maneuvers
# Show turn-by-turn directions

if "trip" in result and "legs" in result["trip"]:
    leg = result["trip"]["legs"][0]
    
    print(f"\n📍 Route Details:")
    print("=" * 80)
    print(f"Distance: {leg['summary']['length']:.2f} km")
    print(f"Time: {leg['summary']['time'] / 60:.1f} minutes")
    print("")
    print("Turn-by-turn directions:")
    print("-" * 80)
    
    for i, maneuver in enumerate(leg.get("maneuvers", []), 1):
        instruction = maneuver.get("instruction", "Continue")
        distance = maneuver.get("length", 0)
        time = maneuver.get("time", 0)
        
        print(f"{i:2d}. {instruction}")
        if distance > 0:
            print(f"    ({distance:.2f} km, {time:.0f} seconds)")
        print("")
    
    print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Quickstart Summary
print("=" * 80)
print("🎉 QUICKSTART COMPLETE")
print("=" * 80)
print(f"\n✅ Valhalla is now fully operational!")
print(f"\n📦 Resources:")
print(f"   • Volume: {volume_path}")
print(f"   • Binaries: {volume_path}/bin")
print(f"   • Wheel: {volume_path}/whl")
print(f"   • Tiles: {volume_path}/tiles")
print(f"   • Init script: {volume_path}/init.sh")
print(f"\n📋 Next Steps:")
print(f"   1. Apply init script to enable distributed routing")
print(f"      Path: {volume_path}/init.sh")
print(f"   2. Run valhalla_distributed_routing.py for parallel routing at scale")
print(f"   3. Integrate routing into your data pipelines")
print("\n🔗 Helpful Commands:")
print(f"   # Test routing via CLI")
print(f"   valhalla_service {config_path} route '{{...query...}}'")
print(f"\n   # Import in Python")
print(f"   from valhalla import Actor")
print(f"   actor = Actor('{config_path}')")
print(f"   result = actor.route({{...query...}})")
print("=" * 80)

# COMMAND ----------
