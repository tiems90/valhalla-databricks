# Valhalla Routing on Databricks - Notebook Collection

This directory contains Databricks notebooks for installing, configuring, and using the Valhalla routing engine on Databricks Runtime 18.0 (GCP).

> **📖 For comprehensive documentation**, see the [Design Document](../../context/valhalla_databricks_design_doc.md)

## 🎯 Quick Start

**Recommended cluster configuration:**
- **Runtime**: `18.0.x-scala2.13` (GCP)
- **Machine Type**: `n2-highmem-16` or larger (16+ vCPUs, 128+ GB RAM)
- **Workers**: 0 (single node for compilation, scale out for routing)
- **Access Mode**: No isolation shared (dedicated)

**Run notebooks in order:**

1. `valhalla_quickstart.py` - Complete end-to-end setup and testing
2. `valhalla_distributed_routing.py` - Scale out with parallel routing

## 📚 Notebook Descriptions

### 1. `valhalla_00_initial_setup.py`
**Purpose**: Build and compile Valhalla from source

**What it does:**
- Clones Valhalla repository with submodules
- Installs system dependencies
- Compiles Valhalla with Python bindings (handles DBR 18.0 GCC 11+ compatibility)
- Builds Python wheel
- Caches binaries to Databricks Volume
- Generates init script for cluster-wide deployment

**Key features for DBR 18.0:**
- Adds compiler flags `-Wno-error=format-truncation` to handle GCC 11+ warnings
- Clean build process for reproducible compilation
- Build environment verification

**Runtime**: 15-30 minutes on n2-highmem-16

**Parameters:**
- `VOLUME_PATH`: Target volume for caching (e.g., `/Volumes/your_catalog/your_schema/valhalla_region`)

**Outputs:**
- Compiled binaries → `{VOLUME_PATH}/bin/`
- Python wheel → `{VOLUME_PATH}/whl/`
- Init script → `{VOLUME_PATH}/init.sh`

---

### 2. `valhalla_01_process_pbf.py`
**Purpose**: Process OpenStreetMap PBF files into routing tiles

**What it does:**
- Downloads OSM PBF extract
- Builds routing tiles, timezones, and admin boundaries
- Generates Valhalla configuration
- Stores tiles to Databricks Volume
- Tests routing configuration

**Runtime**: 
- Small region (Andorra): 5-10 minutes
- Medium region (Spain): 20-40 minutes  
- Large region (USA): 1-3 hours

**Parameters:**
- `PBF_URL`: URL to OSM PBF file (e.g., from [Geofabrik](https://download.geofabrik.de/))
- `VOLUME_PATH`: Target volume for tiles

**Outputs:**
- Routing tiles → `{VOLUME_PATH}/tiles/[0-3]/`
- Configuration → `{VOLUME_PATH}/tiles/valhalla.json`
- Databases → `{VOLUME_PATH}/tiles/{timezones,admins}.sqlite`

---

### 3. `valhalla_quickstart.py`
**Purpose**: Complete end-to-end setup and testing

**What it does:**
- Runs `valhalla_00_initial_setup.py` (compilation)
- Runs `valhalla_01_process_pbf.py` (tile processing)
- Installs Python wheel
- Executes sample routing queries
- Shows turn-by-turn directions

**Runtime**: 30-90 minutes total (depends on region size)

**Usage:**
```python
# Edit the configuration section:
pbf_url = "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
catalog = 'your_catalog'
schema = 'your_schema'
volume = 'valhalla_andorra'
```

**Best for:**
- Initial setup and testing
- Driver-only routing (single node)
- Interactive development

---

### 4. `valhalla_distributed_routing.py`
**Purpose**: Parallel routing at scale using Spark

**What it does:**
- Distributes routing across Spark workers
- Uses `mapInPandas` for parallel execution
- Processes thousands of OD pairs efficiently
- Returns routes with geometries
- Saves results to Delta tables

**Prerequisites:**
- Init script must be applied to cluster
- Cluster must be restarted after applying init script

**Runtime**: Scales with cluster size
- 1000 routes on 4-core cluster: ~30 seconds
- 10000 routes on 32-core cluster: ~60 seconds

**Best for:**
- Production workloads
- Batch routing of large datasets
- Multi-node clusters

---

## 🔧 DBR 18.0 Compatibility

### Key Changes from DBR 17.1

**Problem**: GCC 11+ in DBR 18.0 treats format-truncation warnings as errors, causing Valhalla compilation to fail.

**Solution**: Added compiler flags to cmake configuration:
```cmake
-DCMAKE_CXX_FLAGS="-Wno-error=format-truncation -Wno-format-truncation"
-DCMAKE_C_FLAGS="-Wno-error=format-truncation -Wno-format-truncation"
```

**Additional improvements:**
- Explicit build directory cleanup
- Build environment verification (GCC, CMake versions)
- Enhanced error messages
- Progress indicators

### Verified Runtimes

These notebooks are tested and verified on:
- `18.0.x-scala2.13` ✅
- `18.0.x-photon-scala2.13` ✅
- `18.0.x-cpu-ml-scala2.13` ✅

---

## 📋 Setup Workflow

### Step 1: Create Cluster

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import DataSecurityMode

w = WorkspaceClient(profile="gcp")  # Or "azure", "aws"

cluster = w.clusters.create(
    cluster_name="valhalla-cluster",
    spark_version="18.0.x-scala2.13",
    node_type_id="n2-highmem-16",
    num_workers=0,  # Single node for setup
    autotermination_minutes=120,
    data_security_mode=DataSecurityMode.NONE,
    spark_conf={
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    }
).result()
```

### Step 2: Run Quickstart

Upload and run `valhalla_quickstart.py` on the cluster.

### Step 3: Apply Init Script

1. Go to **Compute** → Your Cluster → **Configuration**
2. Navigate to **Advanced Options** → **Init Scripts**
3. Add Volume-based init script:
   ```
   /Volumes/your_catalog/your_schema/valhalla_andorra/init.sh
   ```
4. **Restart the cluster**

### Step 4: Scale Out (Optional)

For distributed routing:
1. Add workers to your cluster (e.g., 2-8 nodes)
2. Run `valhalla_distributed_routing.py`

---

## 🗺️ Sample PBF URLs

### Small Regions (Testing)
- **Andorra**: `https://download.geofabrik.de/europe/andorra-latest.osm.pbf` (~3 MB)
- **Liechtenstein**: `https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf` (~1 MB)
- **Monaco**: `https://download.geofabrik.de/europe/monaco-latest.osm.pbf` (~1 MB)

### Medium Regions
- **Spain**: `https://download.geofabrik.de/europe/spain-latest.osm.pbf` (~1.5 GB)
- **France**: `https://download.geofabrik.de/europe/france-latest.osm.pbf` (~4 GB)
- **UK**: `https://download.geofabrik.de/europe/great-britain-latest.osm.pbf` (~1.5 GB)

### Large Regions
- **USA**: `https://download.geofabrik.de/north-america/us-latest.osm.pbf` (~10 GB)
- **Europe**: `https://download.geofabrik.de/europe-latest.osm.pbf` (~30 GB)

Find more at [Geofabrik Downloads](https://download.geofabrik.de/)

---

## 🐛 Troubleshooting

### Issue: Compilation fails with format-truncation error

**Symptom:**
```
error: '%02d' directive output may be truncated writing between 2 and 11 bytes
```

**Solution:**  
✅ Use `valhalla_00_initial_setup.py` from this collection - it includes the fix.

---

### Issue: ImportError on workers

**Symptom:**
```python
ImportError: libvalhalla.so.3: cannot open shared object file
```

**Solution:**  
1. Verify init script is applied: Compute → Your Cluster → Advanced → Init Scripts
2. Restart cluster after applying init script
3. Check init script logs: `/databricks/init_scripts/[script_name].log`

---

### Issue: Routing fails with "No suitable edges"

**Symptom:**
```
RuntimeError: No suitable edges near location
```

**Solution:**  
- Coordinates are outside tile coverage area
- Verify PBF covers your query region
- Check coordinates are (lat, lon) not (lon, lat)

---

### Issue: Out of memory during compilation

**Symptom:**
```
c++: fatal error: Killed signal terminated program
```

**Solution:**  
- Use larger cluster: `n2-highmem-16` or `n2-highmem-32`
- Reduce parallel jobs: `make -C build -j8` instead of `-j$(nproc)`

---

## 📊 Performance Tuning

### Compilation Speed
- **Single-core**: 60-90 minutes
- **n2-highmem-16 (16 cores)**: 15-30 minutes
- **n2-highmem-32 (32 cores)**: 10-20 minutes

### PBF Processing Speed
Depends on:
- PBF file size
- Number of OSM elements
- Disk I/O speed

### Routing Throughput
- **Single thread**: ~50-100 routes/second
- **4-core cluster**: ~200-400 routes/second
- **32-core cluster**: ~1500-3000 routes/second

**Optimization tips:**
1. Increase partitions: `repartition(cores * 2)`
2. Batch routes in each partition
3. Use larger machines for workers
4. Cache tile data on local SSDs

---

## 🔗 Additional Resources

- [Valhalla Documentation](https://valhalla.github.io/valhalla/)
- [Valhalla GitHub](https://github.com/valhalla/valhalla)
- [Databricks Init Scripts](https://docs.databricks.com/clusters/init-scripts.html)
- [Geofabrik Downloads](https://download.geofabrik.de/)
- [OpenStreetMap](https://www.openstreetmap.org/)

---

## 📝 Notes

**Not compatible with:**
- Shared access mode clusters
- Serverless compute
- Databricks Connect local development

**For those environments:**  
Use Python routing libraries like `osmnx`, `openrouteservice`, or `routingpy` with PySpark UDFs.

---

## 📄 License

These notebooks are provided as examples. Valhalla is licensed under MIT License.
