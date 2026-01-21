# Notebooks Documentation

Comprehensive guide to all Valhalla notebooks included in this repository.

## Table of Contents

1. [valhalla_00_initial_setup.py](#valhalla_00_initial_setuppy)
2. [valhalla_01_process_pbf.py](#valhalla_01_process_pbfpy)
3. [valhalla_quickstart.py](#valhalla_quickstartpy)
4. [valhalla_test_routing.py](#valhalla_test_routingpy)

---

## valhalla_00_initial_setup.py

**Purpose**: Compile Valhalla routing engine and create initialization script for cluster deployment.

### Overview

This notebook performs a one-time compilation of Valhalla from source, builds the Python wheel, and generates an init script that can be reused across multiple clusters.

### When to Use

- **First time setup**: Run once per Databricks environment
- **Version updates**: Re-run when upgrading Valhalla version
- **New regions**: Only needed once; same binaries work for all regions

### Parameters

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `VOLUME_PATH` | Unity Catalog volume for artifacts | `/Volumes/main/default/valhalla_region` | Yes |

### What It Does

1. **Install Dependencies**
   - Build tools (cmake, gcc, g++)
   - Valhalla dependencies (protobuf, boost, lua, etc.)
   
2. **Clone Valhalla Repository**
   - Fetches latest stable version from GitHub
   - Applies DBR 18.0 compatibility fixes

3. **Compile Valhalla**
   - Configures CMake with GCC 11+ compatibility flags
   - Compiles C++ routing engine
   - Links libraries

4. **Build Python Wheel**
   - Creates Python bindings wheel file
   - Stores in Unity Catalog volume

5. **Generate Init Script**
   - Creates `/Volumes/.../init.sh`
   - Configures environment variables
   - Installs Python wheel on cluster startup

### Cluster Requirements

- **Node Type**: Minimum 16 vCPUs, 128 GB RAM (e.g., n2-highmem-16)
- **Workers**: 0 (single-node cluster)
- **Runtime**: DBR 18.0.x-scala2.13 or higher
- **Access Mode**: Single User

### Expected Duration

| Machine Type | vCPUs | Duration |
|-------------|-------|----------|
| n2-highmem-16 | 16 | 20-25 min |
| n2-highmem-32 | 32 | 10-15 min |
| n2-highmem-64 | 64 | 8-12 min |

**Recommendation**: Use n2-highmem-32 for optimal cost/speed balance.

### Output Artifacts

```
/Volumes/your_catalog/your_schema/valhalla_region/
├── bin/              # Valhalla binaries (valhalla_service, valhalla_build_tiles, etc.)
├── lib/              # Shared libraries
├── whl/              # Python wheel file (pyvalhalla-*.whl)
└── init.sh           # Cluster initialization script
```

### Verification

Check that artifacts were created:

```python
volume_path = "/Volumes/your_catalog/your_schema/valhalla_region"

# List binaries
display(dbutils.fs.ls(f"{volume_path}/bin"))

# List wheel
display(dbutils.fs.ls(f"{volume_path}/whl"))

# Check init script exists
display(dbutils.fs.ls(f"{volume_path}/init.sh"))
```

### DBR 18.0 Compatibility

**Problem**: GCC 11+ treats `format-truncation` warnings as errors.

**Solution**: Notebook automatically applies compiler flags:
```cmake
-DCMAKE_CXX_FLAGS="-Wno-error=format-truncation -Wno-format-truncation"
```

### Troubleshooting

**Issue**: Compilation fails with GCC errors

**Solution**:
- Verify DBR 18.0.x or higher
- Check machine has sufficient memory (128 GB minimum)
- Review full error in notebook output

**Issue**: Out of memory during compilation

**Solution**:
- Use larger machine type (32+ vCPUs recommended)
- Ensure single-node cluster (`num_workers: 0`)

**Issue**: Permission denied writing to volume

**Solution**:
- Check Unity Catalog permissions (`USE CATALOG`, `USE SCHEMA`, `WRITE FILES`)
- Ensure cluster `data_security_mode: SINGLE_USER`

---

## valhalla_01_process_pbf.py

**Purpose**: Download OSM data and build routing tiles for a specific geographic region.

### Overview

This notebook downloads OpenStreetMap data in PBF format, extracts it, builds routing tiles, and generates Valhalla configuration. It must be run after `valhalla_00_initial_setup.py`.

### When to Use

- **New geographic region**: Run once per region (e.g., Spain, USA, Europe)
- **Data updates**: Re-run to refresh routing data with latest OSM
- **Multiple regions**: Run separately for each region

### Parameters

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `VOLUME_PATH` | Unity Catalog volume (must match setup) | `/Volumes/main/default/valhalla_spain` | Yes |
| `PBF_URL` | OSM PBF file URL | `https://download.geofabrik.de/europe/spain-latest.osm.pbf` | Yes |

### What It Does

1. **Validate Init Script**
   - Verifies init script exists from setup step
   - Checks that Valhalla is installed

2. **Download OSM Data**
   - Fetches PBF file from specified URL
   - Validates file integrity

3. **Build Routing Tiles**
   - Runs `valhalla_build_tiles` on PBF data
   - Creates multi-level tile hierarchy
   - Optimizes for routing queries

4. **Generate Configuration**
   - Creates `valhalla.json` with tile paths
   - Configures routing algorithms
   - Sets performance parameters

5. **Move to Volume**
   - Transfers tiles to Unity Catalog volume
   - Updates configuration paths
   - Cleans up temporary files

### Cluster Requirements

- **Node Type**: Minimum 16 vCPUs, 128 GB RAM (e.g., n2-highmem-16)
- **Workers**: 0 (single-node cluster)
- **Runtime**: DBR 18.0.x-scala2.13 or higher
- **Access Mode**: Single User
- **Init Script**: Must use init script from `valhalla_00_initial_setup.py`

### Expected Duration

Duration depends on region size:

| Region | PBF Size | Machine Type | Duration |
|--------|----------|-------------|----------|
| Andorra | 3 MB | n2-highmem-16 | ~10 min |
| Spain | 1.5 GB | n2-highmem-32 | ~30 min |
| USA | 11 GB | n2-highmem-64 | ~90 min |
| Europe | 28 GB | n2-highmem-128 | ~4 hours |
| Planet | 70 GB | n2-highmem-128 | ~8 hours |

**Recommendation**: Start with small region (Andorra) for testing.

### Output Artifacts

```
/Volumes/your_catalog/your_schema/valhalla_region/
├── tiles/                 # Routing tiles
│   ├── 0/                # Level 0 tiles (lowest detail)
│   │   └── *.gph
│   ├── 1/                # Level 1 tiles
│   │   └── *.gph
│   └── 2/                # Level 2 tiles (highest detail)
│       └── *.gph
└── valhalla.json         # Valhalla configuration
```

### Verification

Check that tiles were created:

```python
volume_path = "/Volumes/your_catalog/your_schema/valhalla_region"

# Count tiles
tile_count = len([f for f in dbutils.fs.ls(f"{volume_path}/tiles/") if f.name.endswith('.gph')])
print(f"Created {tile_count} tiles")

# Check config
display(spark.read.text(f"{volume_path}/valhalla.json"))
```

### PBF Data Sources

**Geofabrik** (recommended):
- https://download.geofabrik.de/
- Regional extracts updated daily
- Fast mirrors worldwide

**Examples**:
```python
# Small regions (testing)
pbf_url = "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
pbf_url = "https://download.geofabrik.de/europe/monaco-latest.osm.pbf"

# Medium regions
pbf_url = "https://download.geofabrik.de/europe/spain-latest.osm.pbf"
pbf_url = "https://download.geofabrik.de/europe/germany-latest.osm.pbf"

# Large regions
pbf_url = "https://download.geofabrik.de/north-america/us-latest.osm.pbf"
pbf_url = "https://download.geofabrik.de/europe-latest.osm.pbf"

# Planet (full OSM dataset)
pbf_url = "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
```

### Troubleshooting

**Issue**: Init script not found

**Solution**:
- Run `valhalla_00_initial_setup.py` first
- Verify `VOLUME_PATH` matches between notebooks

**Issue**: PBF download fails

**Solution**:
- Check URL is accessible
- Try alternative mirror
- Verify cluster network egress rules

**Issue**: Tile building fails with exit code 134

**Solution**:
- Increase machine memory (use highmem types)
- Reduce region size (try smaller region)
- Check driver disk space

**Issue**: No tiles generated

**Solution**:
- Check PBF file is valid OSM data
- Review `valhalla_build_tiles` logs in notebook output
- Verify sufficient disk space

---

## valhalla_quickstart.py

**Purpose**: Complete end-to-end walkthrough for setting up and testing Valhalla routing.

### Overview

This interactive notebook guides you through the entire Valhalla workflow: creating Unity Catalog resources, running setup and processing, and testing routing functionality.

### When to Use

- **First time users**: Step-by-step introduction to Valhalla on Databricks
- **Learning**: Understand how all components work together
- **Testing**: Validate setup before production deployment

### Parameters

Configured via Python variables (edit notebook):

```python
# Choose region
pbf_url = "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"

# Unity Catalog settings
catalog = 'your_catalog'
schema = 'your_schema'
volume = 'valhalla_andorra'
```

### What It Does

1. **Create Unity Catalog Resources**
   - Creates catalog (if needed)
   - Creates schema
   - Creates volume

2. **Validate Configuration**
   - Checks identifier naming rules
   - Verifies permissions

3. **Run Initial Setup**
   - Calls `valhalla_00_initial_setup.py`
   - Monitors compilation progress

4. **Process PBF**
   - Calls `valhalla_01_process_pbf.py`
   - Monitors tile building

5. **Test Routing**
   - Imports Valhalla module
   - Runs sample routing queries
   - Displays results on map

### Cluster Requirements

**Development Cluster** (for running quickstart):
- **Node Type**: Any (e.g., n2-standard-4)
- **Workers**: 1-2
- **Runtime**: DBR 18.0.x-scala2.13 or higher

**Note**: Setup and processing run on larger clusters automatically via `dbutils.notebook.run()`.

### Expected Duration

- **Setup phase**: 10-15 minutes (compilation)
- **Processing phase**: 5-10 minutes (Andorra PBF)
- **Testing phase**: 2-3 minutes
- **Total**: 20-30 minutes

### Sample Routing Queries

The notebook includes examples for:

**Basic route**:
```python
request = {
    "locations": [
        {"lat": 42.5, "lon": 1.5},  # Andorra la Vella
        {"lat": 42.55, "lon": 1.45}
    ],
    "costing": "auto"
}
route = actor.route(request)
```

**Multi-modal routing**:
```python
# Car
route_auto = actor.route({..., "costing": "auto"})

# Bicycle
route_bicycle = actor.route({..., "costing": "bicycle"})

# Walking
route_pedestrian = actor.route({..., "costing": "pedestrian"})
```

### Troubleshooting

**Issue**: Notebook run fails with timeout

**Solution**:
- Increase timeout in `dbutils.notebook.run()` calls
- Use larger machine types for setup/processing

**Issue**: Catalog/schema creation fails

**Solution**:
- Check Unity Catalog is enabled
- Verify you have `CREATE CATALOG` permission
- Use existing catalog/schema if you lack permissions

---

## valhalla_test_routing.py

**Purpose**: Automated test suite to validate Valhalla installation and routing functionality.

### Overview

This notebook runs comprehensive tests to ensure Valhalla is correctly installed, tiles are valid, and routing works across multiple modes.

### When to Use

- **Post-installation**: Verify setup completed successfully
- **CI/CD**: Automated testing in deployment pipelines
- **Troubleshooting**: Diagnose routing issues

### Parameters

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `VOLUME_PATH` | Unity Catalog volume with tiles | `/Volumes/main/default/valhalla_region` | Yes |

### Test Suite

#### Test 1: Import Valhalla Module
Validates Python bindings are installed correctly.

```python
import valhalla
print(valhalla.__version__)
```

**Pass Criteria**: No ImportError, version displayed

#### Test 2: Load Actor
Creates routing engine instance.

```python
actor = valhalla.Actor()
```

**Pass Criteria**: Actor object created successfully

#### Test 3: Tile Validation
Checks routing tiles exist and are readable.

```python
tile_dir = f"{volume_path}/tiles"
tile_count = count_tiles(tile_dir)
```

**Pass Criteria**: At least 1 tile found, levels 0-2 present

#### Test 4: Basic Routing
Tests simple point-to-point route.

```python
route = actor.route({
    "locations": [{"lat": lat1, "lon": lon1}, {"lat": lat2, "lon": lon2}],
    "costing": "auto"
})
```

**Pass Criteria**: Route returned, has legs and distance

#### Test 5: Multi-Modal Routing
Tests different transportation modes.

```python
for mode in ["auto", "bicycle", "pedestrian"]:
    route = actor.route({..., "costing": mode})
```

**Pass Criteria**: All modes return valid routes

#### Test 6: Error Handling
Tests invalid request handling.

```python
route = actor.route({"locations": []})  # Invalid: no locations
```

**Pass Criteria**: Graceful error, no crash

#### Test 7: Performance
Measures routing query latency.

```python
start = time.time()
route = actor.route(request)
duration = time.time() - start
```

**Pass Criteria**: Query completes in < 1 second

### Cluster Requirements

- **Node Type**: Any (e.g., n2-standard-8)
- **Workers**: 0-2 (tests work on single or multi-node)
- **Runtime**: DBR 18.0.x-scala2.13 or higher
- **Init Script**: Must use init script from `valhalla_00_initial_setup.py`

### Expected Duration

**Total**: 5-10 minutes

### Output

Test results are displayed in structured format:

```
✅ TEST PASSED: Import Valhalla module
   Module version: 3.6.2

✅ TEST PASSED: Load Valhalla Actor  
   Actor created successfully

✅ TEST PASSED: Routing tiles exist
   Found 1,234 tile files in levels: 0, 1, 2

✅ TEST PASSED: Basic routing (auto)
   Route distance: 12.5 km, duration: 15 minutes

✅ TEST PASSED: Multi-modal routing
   Auto: 12.5 km, Bicycle: 14.2 km, Pedestrian: 13.8 km

✅ TEST PASSED: Error handling
   Invalid requests handled gracefully

✅ TEST PASSED: Routing performance
   Average query time: 0.15 seconds

=================================
SUMMARY: 7/7 tests passed
=================================
```

### Troubleshooting

**Issue**: Test 1 fails (import error)

**Solution**:
- Verify init script was applied (check cluster init logs)
- Ensure wheel file exists in volume
- Restart cluster to apply init script

**Issue**: Test 3 fails (no tiles)

**Solution**:
- Run `valhalla_01_process_pbf.py` first
- Verify `VOLUME_PATH` matches processing notebook
- Check tiles directory exists

**Issue**: Test 4 fails (routing error)

**Solution**:
- Verify coordinates are within PBF region bounds
- Check `valhalla.json` configuration
- Review actor error messages

---

## Best Practices

### Parameter Management

**Use widgets for flexibility**:
```python
dbutils.widgets.text("VOLUME_PATH", "/Volumes/your_catalog/your_schema/valhalla_region", "Volume Path")
volume_path = dbutils.widgets.get("VOLUME_PATH")
```

**Validate inputs**:
```python
def validate_identifier(name):
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid identifier: {name}")
```

### Error Handling

**Use try-except blocks**:
```python
try:
    route = actor.route(request)
except Exception as e:
    print(f"Routing failed: {e}")
    # Handle gracefully
```

**Provide informative errors**:
```python
if not os.path.exists(tile_dir):
    raise FileNotFoundError(f"Tiles not found at {tile_dir}. Run valhalla_01_process_pbf.py first.")
```

### Performance

**Cache expensive operations**:
```python
# Create actor once, reuse for multiple queries
actor = valhalla.Actor()

for request in requests:
    route = actor.route(request)
```

**Use Spark for parallelization**:
```python
@pandas_udf("string")
def route_udf(origins, destinations):
    actor = valhalla.Actor()  # One per worker
    return origins.apply(lambda o: actor.route(o))

df.withColumn("route", route_udf("origin", "destination"))
```

### Documentation

**Add markdown cells**:
```python
# MAGIC %md
# MAGIC # Step 1: Setup
# MAGIC This cell creates the Unity Catalog volume for storing Valhalla artifacts.
```

**Include examples**:
```python
# MAGIC Example:
# MAGIC ```python
# MAGIC volume_path = "/Volumes/main/default/valhalla_andorra"
# MAGIC ```
```

## Common Workflows

### Workflow 1: First-Time Setup

```python
# 1. Run setup (once)
dbutils.notebook.run("valhalla_00_initial_setup", timeout_seconds=3600,
                     arguments={"VOLUME_PATH": volume_path})

# 2. Process region (once per region)
dbutils.notebook.run("valhalla_01_process_pbf", timeout_seconds=3600,
                     arguments={"VOLUME_PATH": volume_path, "PBF_URL": pbf_url})

# 3. Test
dbutils.notebook.run("valhalla_test_routing", timeout_seconds=1800,
                     arguments={"VOLUME_PATH": volume_path})
```

### Workflow 2: Update OSM Data

```python
# Re-run processing only (binaries don't need recompilation)
dbutils.notebook.run("valhalla_01_process_pbf", timeout_seconds=3600,
                     arguments={"VOLUME_PATH": volume_path, "PBF_URL": pbf_url})
```

### Workflow 3: Add New Region

```python
# Use different volume for each region
regions = [
    ("andorra", "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"),
    ("spain", "https://download.geofabrik.de/europe/spain-latest.osm.pbf")
]

for region_name, pbf_url in regions:
    volume_path = f"/Volumes/{catalog}/{schema}/valhalla_{region_name}"
    
    # Process each region
    dbutils.notebook.run("valhalla_01_process_pbf", timeout_seconds=7200,
                         arguments={"VOLUME_PATH": volume_path, "PBF_URL": pbf_url})
```

## Additional Resources

- [Valhalla API Documentation](https://valhalla.github.io/valhalla/api/)
- [OSM Data Sources](https://download.geofabrik.de/)
- [Databricks Notebooks Best Practices](https://docs.databricks.com/notebooks/best-practices.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
