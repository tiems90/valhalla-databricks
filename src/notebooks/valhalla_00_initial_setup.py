# Databricks notebook source
# MAGIC %md
# MAGIC # Valhalla Build & Cache Script (DBR 18.0 Compatible)
# MAGIC
# MAGIC This notebook builds the Valhalla routing engine with Python bindings in a Databricks environment. 
# MAGIC It installs all dependencies, compiles the code, and caches the resulting binaries and wheel file to persistent storage. 
# MAGIC It also generates an init script for restoring these binaries on future cluster startups.
# MAGIC
# MAGIC **Tested with:**
# MAGIC - **Databricks Runtime 18.0.x-scala2.13** (GCP)
# MAGIC - **Single-node cluster** (n2-highmem-16 or larger recommended)
# MAGIC - **Dedicated access mode** (no isolation shared)
# MAGIC
# MAGIC **Key Changes for DBR 18.0:**
# MAGIC - Added compiler flags to handle GCC 11+ format-truncation warnings
# MAGIC - Explicit build directory cleanup for reproducible builds
# MAGIC - Build environment verification steps
# MAGIC
# MAGIC **Important Notes:**
# MAGIC - You generally should **not run this notebook directly**.
# MAGIC - It is invoked internally by the process handling `.pbf` file inputs.
# MAGIC - The generated init script can be reused across clusters to avoid recompilation.
# MAGIC
# MAGIC **Estimated Runtime:**
# MAGIC - 15-30 minutes on n2-highmem-16 (16 vCPUs, 128 GB RAM)
# MAGIC - 10-20 minutes on n2-highmem-32 (32 vCPUs, 256 GB RAM)

# COMMAND ----------

# DBTITLE 1,Configure Volume Path and Ensure It Exists
import os
import re

dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Target Volume Path")
vol_base = dbutils.widgets.get("VOLUME_PATH")

# Parse volume path to extract catalog, schema, volume
volume_pattern = r"/Volumes/([^/]+)/([^/]+)/([^/]+)"
match = re.match(volume_pattern, vol_base)

if not match:
    raise ValueError(f"Invalid volume path format: {vol_base}. Expected: /Volumes/catalog/schema/volume")

catalog, schema, volume = match.groups()

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

catalog = validate_identifier(catalog, "catalog name")
schema = validate_identifier(schema, "schema name")
volume = validate_identifier(volume, "volume name")

print(f"🔧 Ensuring volume exists...")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Volume: {volume}")

# Create catalog (may fail if no permissions, that's ok)
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
    print(f"❌ Failed to create schema: {e}")
    raise

# Create volume
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    print(f"✅ Volume {catalog}.{schema}.{volume} ready")
except Exception as e:
    print(f"❌ Failed to create volume: {e}")
    raise

# Verify volume is accessible
try:
    dbutils.fs.ls(vol_base)
    print(f"✅ Volume accessible at: {vol_base}")
except Exception as e:
    print(f"❌ Cannot access volume: {e}")
    raise

cache_bin_dir = f"{vol_base}/bin"
cache_whl_dir = f"{vol_base}/whl"
init_script_path = f"{vol_base}/init.sh"

os.environ["CACHE_BIN_DIR"] = cache_bin_dir
os.environ["CACHE_WHL_DIR"] = cache_whl_dir

print(f"\n📦 Volume configuration:")
print(f"   Base path: {vol_base}")
print(f"   Binary cache: {cache_bin_dir}")
print(f"   Wheel cache: {cache_whl_dir}")
print(f"   Init script: {init_script_path}")

# COMMAND ----------

# DBTITLE 1,Clean Previous Build Attempts
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "🧹 Cleaning previous build directory..."
# MAGIC rm -rf /local_disk0/valhalla_build
# MAGIC echo "✅ Cleaned previous build directory"

# COMMAND ----------

# DBTITLE 1,Clone and Install Dependencies
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "📥 Cloning Valhalla repository..."
# MAGIC # Define base directory for build
# MAGIC cd /local_disk0/
# MAGIC mkdir -p valhalla_build && cd valhalla_build
# MAGIC
# MAGIC # Clone Valhalla with all submodules
# MAGIC git clone --recurse-submodules https://github.com/valhalla/valhalla.git
# MAGIC cd valhalla
# MAGIC
# MAGIC echo "📦 Installing system dependencies..."
# MAGIC # Install required system packages and Valhalla-specific dependencies
# MAGIC sudo apt-get update -y
# MAGIC ./scripts/install-linux-deps.sh
# MAGIC
# MAGIC echo "🔧 Configuring dynamic linker..."
# MAGIC # Set up dynamic linker to find Valhalla libraries
# MAGIC export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH:-}
# MAGIC echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/valhalla.conf
# MAGIC sudo ldconfig
# MAGIC
# MAGIC echo "✅ Dependencies installed"

# COMMAND ----------

# DBTITLE 1,Verify Build Environment
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "🔍 Build Environment Information:"
# MAGIC echo "================================="
# MAGIC echo ""
# MAGIC echo "GCC Version:"
# MAGIC gcc --version | head -n1
# MAGIC echo ""
# MAGIC echo "G++ Version:"
# MAGIC g++ --version | head -n1
# MAGIC echo ""
# MAGIC echo "CMake Version:"
# MAGIC cmake --version | head -n1
# MAGIC echo ""
# MAGIC echo "Python Version:"
# MAGIC python3 --version
# MAGIC echo ""
# MAGIC echo "Available CPU cores:"
# MAGIC nproc
# MAGIC echo ""
# MAGIC echo "Available memory:"
# MAGIC free -h | grep Mem
# MAGIC echo ""
# MAGIC echo "✅ Environment verification complete"

# COMMAND ----------

# DBTITLE 1,Build Valhalla and Python Wheel
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC echo "🔨 Building Valhalla..."
# MAGIC cd /local_disk0/valhalla_build/valhalla/
# MAGIC
# MAGIC # Configure Valhalla with Python bindings and compiler flags for GCC 11+
# MAGIC # Key fix: -Wno-error=format-truncation prevents format-truncation warnings from failing the build
# MAGIC echo "Running CMake configuration..."
# MAGIC cmake -B build \
# MAGIC   -DCMAKE_BUILD_TYPE=Release \
# MAGIC   -DENABLE_PYTHON_BINDINGS=ON \
# MAGIC   -DCMAKE_CXX_FLAGS="-Wno-error=format-truncation -Wno-format-truncation" \
# MAGIC   -DCMAKE_C_FLAGS="-Wno-error=format-truncation -Wno-format-truncation"
# MAGIC
# MAGIC echo "Compiling with $(nproc) parallel jobs..."
# MAGIC make -C build -j$(nproc)
# MAGIC
# MAGIC echo "Installing compiled binaries..."
# MAGIC sudo make -C build install
# MAGIC
# MAGIC echo "Refreshing dynamic linker..."
# MAGIC sudo ldconfig
# MAGIC
# MAGIC echo "Building Python wheel directly to volume..."
# MAGIC # Install build dependencies
# MAGIC python3 -m pip install --upgrade pip setuptools wheel
# MAGIC python3 -m pip install -r src/bindings/python/requirements-build.txt
# MAGIC
# MAGIC # Build wheel directly to the volume using pip wheel
# MAGIC echo "Building wheel to: $CACHE_WHL_DIR"
# MAGIC mkdir -p "$CACHE_WHL_DIR"
# MAGIC python3 -m pip wheel . --no-deps -w "$CACHE_WHL_DIR"
# MAGIC
# MAGIC # Verify wheel was created
# MAGIC WHEEL_FILE=$(find "$CACHE_WHL_DIR" -name "pyvalhalla-*.whl" -type f | head -n1)
# MAGIC if [ -z "$WHEEL_FILE" ]; then
# MAGIC   echo "❌ ERROR: Python wheel was not created in $CACHE_WHL_DIR"
# MAGIC   echo "Contents of wheel directory:"
# MAGIC   ls -la "$CACHE_WHL_DIR"
# MAGIC   exit 1
# MAGIC fi
# MAGIC
# MAGIC echo "✅ Python wheel created: $WHEEL_FILE"
# MAGIC ls -lh "$WHEEL_FILE"
# MAGIC
# MAGIC echo "✅ Build complete"

# COMMAND ----------

# DBTITLE 1,Install and Verify Python Bindings
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC CACHE_WHL_DIR="${CACHE_WHL_DIR:?}"
# MAGIC
# MAGIC echo "Installing Valhalla Python bindings from volume..."
# MAGIC WHEEL_FILE=$(find "$CACHE_WHL_DIR" -name "pyvalhalla-*.whl" -type f | head -n1)
# MAGIC
# MAGIC if [ -z "$WHEEL_FILE" ]; then
# MAGIC   echo "❌ ERROR: No wheel file found in $CACHE_WHL_DIR"
# MAGIC   exit 1
# MAGIC fi
# MAGIC
# MAGIC echo "Installing from: $WHEEL_FILE"
# MAGIC python3 -m pip install --force-reinstall "$WHEEL_FILE"
# MAGIC
# MAGIC echo "✅ Valhalla Python bindings installed"

# COMMAND ----------

# DBTITLE 1,Test Python Import
import valhalla
print(f"✅ Valhalla Python module imported successfully")
print(f"   Module location: {valhalla.__file__}")
print(f"   Valhalla version: {valhalla.__version__ if hasattr(valhalla, '__version__') else 'N/A'}")

# COMMAND ----------

# DBTITLE 1,Cache Valhalla Binaries to Volume
# MAGIC %sh
# MAGIC set -euxo pipefail
# MAGIC
# MAGIC CACHE_BIN_DIR="${CACHE_BIN_DIR:?}"
# MAGIC CACHE_WHL_DIR="${CACHE_WHL_DIR:?}"
# MAGIC LOCAL_BIN="/usr/local/bin"
# MAGIC LOCAL_LIB="/usr/local/lib"
# MAGIC
# MAGIC echo "📦 Caching Valhalla binaries and libraries to $CACHE_BIN_DIR"
# MAGIC mkdir -p "$CACHE_BIN_DIR"
# MAGIC
# MAGIC # Copy compiled binaries and libraries
# MAGIC echo "   Copying Valhalla binaries..."
# MAGIC cp -f "$LOCAL_BIN"/valhalla* "$CACHE_BIN_DIR/" 2>/dev/null || echo "   (No binaries found)"
# MAGIC
# MAGIC echo "   Copying Valhalla libraries..."
# MAGIC cp -f "$LOCAL_LIB"/libvalhalla* "$CACHE_BIN_DIR/" 2>/dev/null || echo "   (No Valhalla libraries found)"
# MAGIC
# MAGIC echo "   Copying Prime Server libraries..."
# MAGIC cp -f "$LOCAL_LIB"/libprime_server* "$CACHE_BIN_DIR/" 2>/dev/null || echo "   (No Prime Server libraries found)"
# MAGIC
# MAGIC echo "✅ Binaries and libraries cached to volume"
# MAGIC echo ""
# MAGIC echo "Cached binaries:"
# MAGIC ls -lh "$CACHE_BIN_DIR" 2>/dev/null | tail -n +2 || echo "   (Empty)"
# MAGIC echo ""
# MAGIC echo "Python wheel (already on volume):"
# MAGIC ls -lh "$CACHE_WHL_DIR"/*.whl 2>/dev/null || echo "   (Wheel not found)"

# COMMAND ----------

# DBTITLE 1,Generate Init Script
init_script_content = f"""#!/bin/bash
set -euxo pipefail

# Valhalla Init Script for DBR 18.0
# Generated by: valhalla_00_initial_setup.py
# Purpose: Restore precompiled Valhalla binaries and Python wheel on cluster startup

# Define important paths
CACHE_BIN_DIR="{cache_bin_dir}"      # Where precompiled Valhalla binaries are stored
CACHE_WHL_DIR="{cache_whl_dir}"      # Where the Valhalla Python wheel is stored
LOCAL_BIN="/usr/local/bin"           # System path for executables
LOCAL_LIB="/usr/local/lib"           # System path for libraries

echo "🚀 Valhalla Init Script Starting..."

# Install system-level dependencies required to run Valhalla (no build tools needed)
echo "📦 Installing runtime dependencies..."
sudo apt-get update -y
env DEBIAN_FRONTEND=noninteractive sudo apt install --yes --quiet \\
  libboost-all-dev libcurl4-openssl-dev libczmq-dev \\
  libgdal-dev libgeos++-dev libgeos-dev libluajit-5.1-dev liblz4-dev \\
  libprotobuf-dev libspatialite-dev libsqlite3-dev libsqlite3-mod-spatialite \\
  libzmq3-dev luajit spatialite-bin zlib1g-dev \\
  python3-all-dev python3-pip

# Ensure system linker can locate Valhalla and Prime Server libraries
echo "🔧 Configuring dynamic linker..."
export LD_LIBRARY_PATH=/usr/local/lib:${{LD_LIBRARY_PATH:-}}
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/valhalla.conf
sudo ldconfig

# Try to detect if prebuilt binaries and Python wheel are already cached
BIN_OK=false
WHL_OK=false

echo "🔍 Checking for cached binaries..."
ROUTE_FILE=$(find "$CACHE_BIN_DIR" -maxdepth 1 -name "valhalla_*route" 2>/dev/null | head -n1)
[[ -f "$ROUTE_FILE" ]] && BIN_OK=true

echo "🔍 Checking for cached wheel..."
WHEEL_FILE=$(find "$CACHE_WHL_DIR" -maxdepth 1 -name "*.whl" 2>/dev/null | head -n1)
[[ -n "$WHEEL_FILE" ]] && WHL_OK=true

# If both are found, restore them
if [[ "$BIN_OK" == "true" && "$WHL_OK" == "true" ]]; then
  echo "✅ Found cached binaries and wheel, restoring..."
  
  echo "   Copying binaries to $LOCAL_BIN..."
  sudo cp -f "$CACHE_BIN_DIR"/valhalla* "$LOCAL_BIN/" || true
  
  echo "   Copying libraries to $LOCAL_LIB..."
  sudo cp -f "$CACHE_BIN_DIR"/libvalhalla* "$LOCAL_LIB/" || true
  sudo cp -f "$CACHE_BIN_DIR"/libprime_server* "$LOCAL_LIB/" || true
  
  echo "   Running ldconfig..."
  sudo ldconfig
  
  echo "   Installing Python wheel..."
  python3 -m pip install --force-reinstall "$WHEEL_FILE"
  
  echo "✅ Valhalla binaries and wheel restored from cache."
  echo "   Binary cache: $CACHE_BIN_DIR"
  echo "   Wheel: $WHEEL_FILE"
  exit 0
else
  echo "⚠️  Cached binaries or wheel not found!"
  echo "   BIN_OK=$BIN_OK (looking for valhalla_*route in $CACHE_BIN_DIR)"
  echo "   WHL_OK=$WHL_OK (looking for *.whl in $CACHE_WHL_DIR)"
  echo ""
  echo "Please run the valhalla_00_initial_setup notebook first to build and cache Valhalla."
  exit 1
fi
"""

# Write script to Volumes
dbutils.fs.put(init_script_path, init_script_content, overwrite=True)
print(f"✅ Init script written to: {init_script_path}")
print(f"   Script size: {len(init_script_content)} bytes")

# COMMAND ----------

# DBTITLE 1,Preview Init Script
print("📄 Init Script Preview:")
print("=" * 80)
print(dbutils.fs.head(init_script_path, 2000))
print("=" * 80)
print(f"\n✅ Full script available at: {init_script_path}")
print(f"\nTo use this init script:")
print(f"1. Go to Compute → Your Cluster → Configuration")
print(f"2. Navigate to Advanced Options → Init Scripts")
print(f"3. Add Volume-based init script: {init_script_path}")
print(f"4. Restart the cluster")

# COMMAND ----------

# DBTITLE 1,Build Summary
print("=" * 80)
print("🎉 VALHALLA BUILD COMPLETE")
print("=" * 80)
print(f"\n📦 Artifacts saved to volume: {vol_base}")
print(f"   • Binaries: {cache_bin_dir}")
print(f"   • Python wheel: {cache_whl_dir}")
print(f"   • Init script: {init_script_path}")
print(f"\n📋 Next Steps:")
print(f"   1. Run valhalla_01_process_pbf.py to generate routing tiles")
print(f"   2. Apply init script to clusters for distributed routing")
print(f"   3. Use valhalla_quickstart.py for testing")
print("=" * 80)
