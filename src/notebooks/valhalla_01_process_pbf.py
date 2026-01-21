# Databricks notebook source
# MAGIC %md
# MAGIC # Process OSM PBF with Valhalla (Multi-Runtime Compatible)
# MAGIC
# MAGIC This notebook downloads and processes an OpenStreetMap `.pbf` extract using Valhalla build tools. 
# MAGIC It creates routing tiles, timezones, and administrative boundaries, and stores them in the specified volume path.
# MAGIC
# MAGIC **Tested with:**
# MAGIC - **DBR 18.0.x-scala2.13** (standard & Photon) ✅
# MAGIC - **DBR 17.3.x-scala2.13** (LTS, standard & Photon) ✅
# MAGIC - **Single-node cluster** (n2-highmem-16 or larger recommended)
# MAGIC - **Dedicated access mode** (no isolation shared)
# MAGIC
# MAGIC ## Usage
# MAGIC This notebook is intended to be **run as a scheduled Databricks job** or manually with two parameters:
# MAGIC - **`PBF_URL`**: URL of the OSM `.pbf` file (e.g. from [Geofabrik](https://download.geofabrik.de/))
# MAGIC - **`VOLUME_PATH`**: Path to the destination volume for tiles (e.g. `/Volumes/your_catalog/your_schema/valhalla_region`)
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Valhalla must be installed via `valhalla_00_initial_setup.py`
# MAGIC - Init script must be applied to the cluster
# MAGIC
# MAGIC **Estimated Runtime:**
# MAGIC - Small region (Andorra): 5-10 minutes
# MAGIC - Medium region (Spain): 20-40 minutes
# MAGIC - Large region (USA): 1-3 hours
# MAGIC
# MAGIC Time varies based on:
# MAGIC - PBF file size
# MAGIC - Cluster size (more vCPUs = faster processing)
# MAGIC - Network speed for download

# COMMAND ----------

# DBTITLE 1,Set Parameters and Ensure Volume Exists
import os
import re

dbutils.widgets.text("PBF_URL", "https://download.geofabrik.de/europe/andorra-latest.osm.pbf", "PBF URL")
dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Target Volume Path")

pbf_url = dbutils.widgets.get("PBF_URL")
volume_path = dbutils.widgets.get("VOLUME_PATH")

# Validate identifiers to prevent SQL injection
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

# Parse volume path to extract catalog, schema, volume
volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, volume_path)

if match:
    catalog, schema, volume = match.groups()
    
    # Validate extracted identifiers
    catalog = validate_identifier(catalog, "catalog name")
    schema = validate_identifier(schema, "schema name")
    volume = validate_identifier(volume, "volume name")
    
    print(f"🔧 Ensuring volume exists...")
    
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
        print(f"⚠️  Volume: {e} (assuming it exists)")

os.environ["PBF_URL"] = pbf_url
os.environ["VALHALLA_VOLUME_PATH"] = volume_path

print(f"\n📥 PBF URL: {pbf_url}")
print(f"📦 Volume path: {volume_path}")

# COMMAND ----------

# DBTITLE 1,Verify Valhalla Installation
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "🔍 Verifying Valhalla installation..."
# MAGIC
# MAGIC if ! command -v valhalla_build_config &> /dev/null; then
# MAGIC     echo "❌ ERROR: Valhalla tools not found in PATH"
# MAGIC     echo "Please ensure the init script is applied to this cluster."
# MAGIC     exit 1
# MAGIC fi
# MAGIC
# MAGIC echo "✅ Valhalla tools found:"
# MAGIC ls -lh /usr/local/bin/valhalla* | head -5
# MAGIC
# MAGIC echo ""
# MAGIC echo "✅ Valhalla libraries found:"
# MAGIC ls -lh /usr/local/lib/libvalhalla* | head -3

# COMMAND ----------

# DBTITLE 1,Install Required Dependencies (jq)
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "📦 Installing jq for JSON processing..."
# MAGIC sudo apt-get update -qq
# MAGIC sudo apt-get install -y -qq jq
# MAGIC
# MAGIC # Verify jq installation
# MAGIC if ! command -v jq &> /dev/null; then
# MAGIC     echo "❌ ERROR: jq installation failed"
# MAGIC     echo "   jq is required for JSON processing in tile configuration"
# MAGIC     echo "   Possible causes:"
# MAGIC     echo "   - Network connectivity issues"
# MAGIC     echo "   - Package repository unavailable"
# MAGIC     echo "   - Insufficient disk space"
# MAGIC     exit 1
# MAGIC fi
# MAGIC
# MAGIC echo "✅ jq installed successfully:"
# MAGIC jq --version

# COMMAND ----------

# DBTITLE 1,Download and Build Tiles
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC # Inputs
# MAGIC PBF_FILE=$(basename "${PBF_URL}")
# MAGIC PBF_PATH="/local_disk0/${PBF_FILE}"
# MAGIC TILE_DIR="${VALHALLA_VOLUME_PATH}/tiles"
# MAGIC TEMP_DIR=$(mktemp -d -p /local_disk0 valhalla_tmp_XXXX)
# MAGIC
# MAGIC echo "📥 Downloading ${PBF_URL}"
# MAGIC echo "   Target: ${PBF_PATH}"
# MAGIC wget -N -O "${PBF_PATH}" "${PBF_URL}"
# MAGIC
# MAGIC echo ""
# MAGIC echo "📊 Downloaded file info:"
# MAGIC ls -lh "${PBF_PATH}"
# MAGIC
# MAGIC # Create temp build directory
# MAGIC echo ""
# MAGIC echo "📁 Creating temporary build directory: ${TEMP_DIR}"
# MAGIC mkdir -p "${TEMP_DIR}"
# MAGIC cd "${TEMP_DIR}"
# MAGIC
# MAGIC # Generate initial config file
# MAGIC echo ""
# MAGIC echo "⚙️  Generating Valhalla configuration..."
# MAGIC valhalla_build_config \
# MAGIC   --mjolnir-tile-dir "${TEMP_DIR}" \
# MAGIC   --mjolnir-tile-extract "${TEMP_DIR}/tiles.tar" \
# MAGIC   --mjolnir-timezone "${TEMP_DIR}/timezones.sqlite" \
# MAGIC   --mjolnir-admin "${TEMP_DIR}/admins.sqlite" > "${TEMP_DIR}/valhalla.json"
# MAGIC
# MAGIC echo "✅ Configuration generated"
# MAGIC
# MAGIC # Build supporting files
# MAGIC echo ""
# MAGIC echo "🌍 Building timezones database..."
# MAGIC valhalla_build_timezones > "${TEMP_DIR}/timezones.sqlite"
# MAGIC
# MAGIC echo ""
# MAGIC echo "🏛️  Building administrative boundaries..."
# MAGIC valhalla_build_admins -c "${TEMP_DIR}/valhalla.json" "${PBF_PATH}"
# MAGIC
# MAGIC echo ""
# MAGIC echo "🗺️  Building routing tiles (this may take a while)..."
# MAGIC valhalla_build_tiles -c "${TEMP_DIR}/valhalla.json" "${PBF_PATH}"
# MAGIC
# MAGIC echo ""
# MAGIC echo "📦 Building tile extract..."
# MAGIC valhalla_build_extract -c "${TEMP_DIR}/valhalla.json" -v
# MAGIC
# MAGIC # Rewrite paths in config to final TILE_DIR
# MAGIC echo ""
# MAGIC echo "🔧 Rewriting config paths for volume storage..."
# MAGIC jq --arg from "${TEMP_DIR}" --arg to "${TILE_DIR}" '
# MAGIC   walk(
# MAGIC     if type == "string" and startswith($from) then
# MAGIC       $to + (.[($from | length):])
# MAGIC     else
# MAGIC       .
# MAGIC     end
# MAGIC   )
# MAGIC ' "${TEMP_DIR}/valhalla.json" > "${TEMP_DIR}/valhalla_fixed.json"
# MAGIC
# MAGIC echo ""
# MAGIC echo "📦 Copying files to volume: ${TILE_DIR}"
# MAGIC mkdir -p "${TILE_DIR}"
# MAGIC rsync -a --info=progress2 "${TEMP_DIR}/" "${TILE_DIR}/"
# MAGIC mv "${TILE_DIR}/valhalla_fixed.json" "${TILE_DIR}/valhalla.json"
# MAGIC
# MAGIC echo ""
# MAGIC echo "🧹 Cleaning up temporary directory..."
# MAGIC rm -rf "${TEMP_DIR}"
# MAGIC
# MAGIC echo ""
# MAGIC echo "✅ Tile processing complete!"
# MAGIC echo "   Output directory: ${TILE_DIR}"

# COMMAND ----------

# DBTITLE 1,Verify Tile Structure
import os
import json

tile_dir = f"{volume_path}/tiles"
config_path = f"{tile_dir}/valhalla.json"

print(f"📁 Tile directory contents:")
print("=" * 80)

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

print("=" * 80)

# Verify config
if os.path.exists(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)
    print(f"\n✅ Configuration file found: {config_path}")
    print(f"   Tile directory: {config.get('mjolnir', {}).get('tile_dir', 'N/A')}")
    print(f"   Admin DB: {config.get('mjolnir', {}).get('admin', 'N/A')}")
    print(f"   Timezone DB: {config.get('mjolnir', {}).get('timezone', 'N/A')}")
else:
    print(f"❌ Configuration file not found: {config_path}")

# COMMAND ----------

# DBTITLE 1,Test Configuration - Simple Route
import json
import warnings
import subprocess
from subprocess import PIPE, DEVNULL, CalledProcessError

config_path = f"{volume_path}/tiles/valhalla.json"
force_cli = False  # Set to True to force using CLI fallback

# Test coordinates - adjust these to match your tile coverage
# These are sample coordinates for Andorra
query = {
    "locations": [
        {"lat": 42.5078, "lon": 1.5211, "type": "break", "city": "Andorra la Vella"},
        {"lat": 42.5562, "lon": 1.5336, "type": "break", "city": "Ordino"}
    ],
    "costing": "auto",
    "directions_options": {"units": "kilometers"}
}

print("🧪 Testing routing configuration...")
print(f"   Config: {config_path}")
print(f"   Route: {query['locations'][0]['city']} → {query['locations'][1]['city']}")
print("")

def run_cli_fallback():
    """Use valhalla_service CLI tool for routing"""
    try:
        cli_command = [
            "/usr/local/bin/valhalla_service",
            config_path,
            "route",
            json.dumps(query)
        ]

        result = subprocess.run(
            cli_command,
            stdout=PIPE,
            stderr=PIPE,
            text=True,
            timeout=30
        )

        # Filter stderr for relevant errors only
        stderr_filtered = "\n".join([
            line for line in result.stderr.strip().splitlines()
            if "No suitable edges" in line or "error" in line.lower()
        ])

        try:
            parsed = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise RuntimeError(f"Invalid JSON output from valhalla_service: {result.stdout[:200]}")

        if "trip" not in parsed:
            print("❌ Routing failed — no trip returned.")
            if stderr_filtered:
                print(f"   Errors: {stderr_filtered}")
            return {
                "status": "error",
                "stderr": stderr_filtered,
                "stdout": result.stdout.strip()[:500]
            }

        summary = parsed["trip"].get("summary", {})
        distance = summary.get("length", "N/A")
        time = summary.get("time", "N/A")

        print("✅ Routing via valhalla_service succeeded.")
        print(f"   ➤ Distance: {distance} km")
        if isinstance(time, (int, float)):
            print(f"   ➤ Estimated Time: {round(time / 60, 1)} min")
        else:
            print(f"   ➤ Time: {time}")

        return {
            "status": "success",
            "distance": distance,
            "time_min": round(time / 60, 1) if isinstance(time, (int, float)) else time
        }

    except subprocess.TimeoutExpired:
        print("❌ CLI fallback timed out after 30 seconds")
        return {"status": "timeout", "message": "Request timed out"}
    except Exception as e:
        print(f"❌ CLI fallback failed with exception: {str(e)}")
        return {"status": "exception", "message": str(e)}


# --- Main Routing ---
if force_cli:
    result = run_cli_fallback()
else:
    try:
        import valhalla
        from valhalla import Actor

        actor = Actor(config_path)
        result = actor.route(query)

        summary = result.get("trip", {}).get("summary", {})
        distance = summary.get("length", "N/A")
        time = summary.get("time", "N/A")

        print("✅ Routing via Python bindings succeeded.")
        print(f"   ➤ Distance: {distance} km")
        if isinstance(time, (int, float)):
            print(f"   ➤ Estimated Time: {round(time / 60, 1)} min")
        else:
            print(f"   ➤ Time: {time}")

    except ImportError:
        warnings.warn(
            "⚠️ Valhalla Python bindings unavailable. Falling back to valhalla_service.",
            RuntimeWarning
        )
        result = run_cli_fallback()
    except Exception as e:
        print(f"❌ Python bindings failed: {str(e)}")
        print("   Trying CLI fallback...")
        result = run_cli_fallback()

# COMMAND ----------

# DBTITLE 1,Processing Summary
print("=" * 80)
print("🎉 PBF PROCESSING COMPLETE")
print("=" * 80)
print(f"\n📦 Outputs saved to: {volume_path}/tiles")
print(f"   • Routing tiles: {volume_path}/tiles/[0-3]/")
print(f"   • Configuration: {volume_path}/tiles/valhalla.json")
print(f"   • Timezone DB: {volume_path}/tiles/timezones.sqlite")
print(f"   • Admin DB: {volume_path}/tiles/admins.sqlite")
print(f"\n📋 Next Steps:")
print(f"   1. Verify routing test passed above")
print(f"   2. Use valhalla_quickstart.py for interactive testing")
print(f"   3. Use valhalla_distributed_routing.py for parallel routing at scale")
print("=" * 80)

# COMMAND ----------
