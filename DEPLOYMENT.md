# Deployment Guide

Complete guide for deploying Valhalla on Databricks across AWS, Azure, and GCP.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Cloud-Specific Configuration](#cloud-specific-configuration)
4. [Unity Catalog Setup](#unity-catalog-setup)
5. [Bundle Deployment](#bundle-deployment)
6. [Running the Test Job](#running-the-test-job)
7. [Production Deployment](#production-deployment)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Tools

- **Databricks CLI** (v0.213.0 or higher)
  ```bash
  # Install via pip
  pip install databricks-cli
  
  # Verify installation
  databricks --version
  ```

- **Git** (for cloning repository)
  ```bash
  git --version
  ```

### Required Permissions

Your Databricks user needs:
- Unity Catalog: `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME`
- Workspace: `CAN_MANAGE` permissions for jobs
- Compute: `CAN_ATTACH_TO` for clusters

### Workspace Requirements

- **Unity Catalog enabled** (check in workspace settings)
- **Databricks Runtime 18.0.x or higher**
- **Cloud provider**: AWS, Azure, or GCP

## Initial Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd valhalla_20260121
```

### 2. Configure Unity Catalog Identifiers

Edit `databricks.yml` and update these values:

```yaml
variables:
  valhalla_catalog:
    default: "main"  # Change to your catalog
  
  valhalla_schema:
    default: "default"  # Change to your schema
  
  valhalla_volume:
    default: "valhalla_region"  # Volume name for this region
  
  valhalla_pbf_url:
    default: "https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
```

**Naming Rules**:
- Must start with letter or underscore
- Can contain letters, digits, underscores only
- No hyphens, spaces, or special characters
- Maximum 255 characters

## Cloud-Specific Configuration

### GCP

#### Authentication

```bash
databricks auth login --profile gcp --host https://<workspace-id>.<region>.gcp.databricks.com
```

Example:
```bash
databricks auth login --profile gcp --host https://1234567890.8.gcp.databricks.com
```

#### Machine Types

Update `resources/valhalla_test_job.yml`:

```yaml
# Task 1 & 2: Compilation and PBF processing
node_type_id: "n2-highmem-32"  # 32 vCPUs, 256 GB RAM

# Task 3: Testing
node_type_id: "n2-standard-8"  # 8 vCPUs, 32 GB RAM
```

#### Spark Version

```yaml
spark_version: "18.0.x-scala2.13"
```

### Azure

#### Authentication

```bash
databricks auth login --profile azure --host https://adb-<workspace-id>.<random-number>.azuredatabricks.net
```

Example:
```bash
databricks auth login --profile azure --host https://adb-1234567890.12.azuredatabricks.net
```

#### Machine Types

Update `resources/valhalla_test_job.yml`:

```yaml
# Task 1 & 2: Compilation and PBF processing
node_type_id: "Standard_E32s_v3"  # 32 vCPUs, 256 GB RAM

# Task 3: Testing
node_type_id: "Standard_D8s_v3"  # 8 vCPUs, 32 GB RAM
```

#### Spark Version

```yaml
spark_version: "18.0.x-scala2.13"
```

### AWS

#### Authentication

```bash
databricks auth login --profile aws --host https://<workspace-name>.cloud.databricks.com
```

Example:
```bash
databricks auth login --profile aws --host https://dbc-12345678-abcd.cloud.databricks.com
```

#### Machine Types

Update `resources/valhalla_test_job.yml`:

```yaml
# Task 1 & 2: Compilation and PBF processing
node_type_id: "r5.8xlarge"  # 32 vCPUs, 256 GB RAM

# Task 3: Testing
node_type_id: "m5.2xlarge"  # 8 vCPUs, 32 GB RAM
```

#### Spark Version

```yaml
spark_version: "18.0.x-scala2.13"
```

## Unity Catalog Setup

### Option 1: Automatic (Recommended)

The notebooks will automatically create the catalog, schema, and volume if they don't exist.

### Option 2: Manual Setup

Create Unity Catalog resources manually:

```sql
-- 1. Create catalog (if needed)
CREATE CATALOG IF NOT EXISTS your_catalog;

-- 2. Create schema
CREATE SCHEMA IF NOT EXISTS your_catalog.your_schema;

-- 3. Create volume
CREATE VOLUME IF NOT EXISTS your_catalog.your_schema.valhalla_region;
```

Run via Databricks SQL or notebooks.

### Verify Setup

```sql
-- Check catalog exists
SHOW CATALOGS LIKE 'your_catalog';

-- Check schema exists
SHOW SCHEMAS IN your_catalog LIKE 'your_schema';

-- Check volume exists
SHOW VOLUMES IN your_catalog.your_schema LIKE 'valhalla_region';
```

## Bundle Deployment

### 1. Validate Configuration

```bash
# For GCP (default)
databricks bundle validate -t gcp

# For Azure
databricks bundle validate -t azure

# For AWS
databricks bundle validate -t aws
```

**Expected Output**:
```
Validation successful!
```

### 2. Deploy to Workspace

```bash
# For GCP
databricks bundle deploy -t gcp

# For Azure
databricks bundle deploy -t azure

# For AWS
databricks bundle deploy -t aws
```

**Expected Output**:
```
Successfully deployed!
Bundle deployed to: /Users/<username>/.bundle/valhalla_20260121/gcp
```

### 3. Verify Deployment

Check that resources were created:

1. Open Databricks workspace
2. Go to **Workflows** → **Jobs**
3. Find `valhalla_test_gcp` (or `valhalla_test_azure`, `valhalla_test_aws`)
4. Verify 3 tasks are configured:
   - `initial_setup`
   - `process_pbf`
   - `test_routing`

## Running the Test Job

### Via CLI (Recommended)

```bash
# Run the job
databricks bundle run valhalla_test_job -t gcp

# Follow logs in terminal
databricks bundle run valhalla_test_job -t gcp --follow
```

### Via UI

1. Open Databricks workspace
2. Go to **Workflows** → **Jobs**
3. Find `valhalla_test_gcp`
4. Click **Run now**
5. Monitor progress in run details page

### Expected Duration

| Task | Duration | What It Does |
|------|----------|-------------|
| **initial_setup** | 10-15 min | Compiles Valhalla, builds wheel, creates init script |
| **process_pbf** | 5-10 min | Downloads Andorra PBF, builds routing tiles |
| **test_routing** | 5 min | Runs 7 automated tests |
| **Total** | 20-30 min | Complete workflow |

### Monitoring Progress

**Task 1: initial_setup**
- ✓ Install dependencies
- ✓ Clone Valhalla repository
- ✓ Compile with CMake
- ✓ Build Python wheel
- ✓ Create init script

**Task 2: process_pbf**
- ✓ Download OSM PBF file
- ✓ Build routing tiles
- ✓ Generate valhalla.json

**Task 3: test_routing**
- ✓ Import Valhalla module
- ✓ Load routing tiles
- ✓ Test basic routing
- ✓ Test multiple modes (auto, bicycle, pedestrian)
- ✓ Validate tile structure

## Production Deployment

### Recommended Changes

#### 1. Use Larger Regions

Update `valhalla_pbf_url` for production regions:

```yaml
# Spain
valhalla_pbf_url: "https://download.geofabrik.de/europe/spain-latest.osm.pbf"

# USA
valhalla_pbf_url: "https://download.geofabrik.de/north-america/us-latest.osm.pbf"

# Europe
valhalla_pbf_url: "https://download.geofabrik.de/europe-latest.osm.pbf"
```

#### 2. Adjust Timeouts

Edit `resources/valhalla_test_job.yml`:

```yaml
tasks:
  - task_key: initial_setup
    timeout_seconds: 3600  # Keep 1 hour

  - task_key: process_pbf
    timeout_seconds: 7200  # 2 hours for large regions
    
  - task_key: test_routing
    timeout_seconds: 1800  # Keep 30 minutes
```

#### 3. Enable Scheduling

Uncomment in `resources/valhalla_test_job.yml`:

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # 2 AM daily
  timezone_id: "UTC"
  pause_status: UNPAUSED  # Change from PAUSED
```

#### 4. Configure Notifications

Update email notifications:

```yaml
email_notifications:
  on_success:
    - team@company.com
  on_failure:
    - ops@company.com
    - oncall@company.com
```

#### 5. Use Job Clusters vs Interactive Clusters

**Current (Development)**:
- Uses job clusters (created per run)
- Good for testing, isolated environment

**Production Option**:
- Use long-running interactive cluster
- Set `existing_cluster_id` in job config
- Faster startup, lower costs if running frequently

Example:
```yaml
tasks:
  - task_key: test_routing
    existing_cluster_id: "0123-456789-abcd1234"  # Your cluster ID
```

### Multiple Regions

To process multiple regions, create separate jobs or volumes:

```yaml
# databricks.yml
variables:
  valhalla_catalog:
    default: "main"
  valhalla_schema:
    default: "routing"
  
  # Define multiple volumes
  valhalla_volume_andorra:
    default: "valhalla_andorra"
  valhalla_volume_spain:
    default: "valhalla_spain"
  valhalla_volume_usa:
    default: "valhalla_usa"
```

## Troubleshooting

### Authentication Issues

**Symptom**: `Error: authentication failed`

**Solution**:
```bash
# Clear cached credentials
rm -rf ~/.databrickscfg

# Re-authenticate
databricks auth login --profile gcp --host <workspace-url>
```

### Bundle Validation Errors

**Symptom**: `Error: invalid configuration`

**Solution**:
- Check YAML syntax (indentation, quotes)
- Verify all variable references (e.g., `${var.valhalla_catalog}`)
- Ensure notebook paths are correct (`../src/notebooks/...`)

### Compilation Failures (Task 1)

**Symptom**: GCC errors about `format-truncation`

**Solution**:
- Verify DBR version is 18.0.x or higher
- Check compiler flags in `valhalla_00_initial_setup.py`:
  ```bash
  -DCMAKE_CXX_FLAGS="-Wno-error=format-truncation -Wno-format-truncation"
  ```

**Symptom**: Out of memory during compilation

**Solution**:
- Use larger machine type (n2-highmem-32 recommended)
- Ensure single-node cluster (`num_workers: 0`)

### Permission Errors (Task 1 or 2)

**Symptom**: `No Unity API token found in Unity Scope`

**Solution**:
Update `data_security_mode` in `resources/valhalla_test_job.yml`:
```yaml
new_cluster:
  data_security_mode: "SINGLE_USER"  # Not "NONE"
```

**Symptom**: `User does not have CREATE permission on catalog`

**Solution**:
- Grant `CREATE SCHEMA` on catalog
- Or use existing catalog/schema you have permissions for

### PBF Download Failures (Task 2)

**Symptom**: `Failed to download PBF file`

**Solution**:
- Verify PBF URL is accessible
- Check cluster network egress rules
- Try alternative mirror (e.g., different Geofabrik region)

### Tile Building Failures (Task 2)

**Symptom**: `valhalla_build_tiles failed with exit code 134`

**Solution**:
- Increase cluster memory (use highmem machine types)
- Reduce PBF region size (try smaller region first)
- Check disk space on driver node

### Init Script Not Applied (Task 2 or 3)

**Symptom**: `ModuleNotFoundError: No module named 'valhalla'`

**Solution**:
- Verify init script exists: `/Volumes/.../init.sh`
- Check init script logs: `/databricks/logs/init_script.log`
- Ensure volume path is correct in job config

### Routing Failures (Task 3)

**Symptom**: `No tiles found`

**Solution**:
- Verify tiles exist: `/Volumes/.../tiles/`
- Check file count: `ls -R /Volumes/.../tiles/ | grep .gph | wc -l`
- Ensure `valhalla.json` points to correct tile directory

## Performance Optimization

### Compilation Speed

| Machine Type | vCPUs | Compilation Time |
|-------------|-------|-----------------|
| n2-highmem-16 | 16 | ~20-25 min |
| n2-highmem-32 | 32 | ~10-15 min |
| n2-highmem-64 | 64 | ~8-12 min |

**Recommendation**: Use n2-highmem-32 for balance of cost and speed.

### PBF Processing Speed

Depends on region size:

| Region | Size | Machine Type | Duration |
|--------|------|-------------|----------|
| Andorra | 3 MB | n2-highmem-32 | ~5 min |
| Spain | 1.5 GB | n2-highmem-32 | ~30 min |
| USA | 11 GB | n2-highmem-64 | ~90 min |
| Planet | 70 GB | n2-highmem-128 | ~8 hours |

### Cost Optimization

**Development**:
- Use smallest region (Andorra) for testing
- Use job clusters (auto-terminate after run)
- Run during off-peak hours

**Production**:
- Use interactive cluster for frequent runs
- Schedule during low-cost periods
- Consider preemptible/spot instances

## Next Steps

After successful deployment:

1. **Explore Notebooks**: Review `src/notebooks/` for usage patterns
2. **Run Quickstart**: Execute `valhalla_quickstart.py` for guided walkthrough
3. **Customize Regions**: Update PBF URLs for your target regions
4. **Scale Testing**: Try distributed routing with `mapInPandas`
5. **Production Deploy**: Follow production recommendations above

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
- [Cluster Init Scripts](https://docs.databricks.com/clusters/init-scripts.html)
- [Valhalla Documentation](https://valhalla.github.io/valhalla/)
- [Geofabrik Downloads](https://download.geofabrik.de/)

## Support

For deployment issues:
- Check logs in Databricks job run output
- Review init script logs: `/databricks/logs/init_script.log`
- Verify Unity Catalog permissions
- Consult troubleshooting section above

For Valhalla-specific questions:
- [Valhalla GitHub Issues](https://github.com/valhalla/valhalla/issues)
- [Valhalla Discussions](https://github.com/valhalla/valhalla/discussions)
