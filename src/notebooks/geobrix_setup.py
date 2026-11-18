# Databricks notebook source
# MAGIC %md
# MAGIC # GeoBrix 0.6.0 — spatial analysis and visualization
# MAGIC
# MAGIC After installing the wheel and restarting Python in the calling notebook,
# MAGIC run `%run ./geobrix_setup` to register GeoBrix and check its Spark functions.
# MAGIC The env5 wheel supports DBR 17.3 and 18 (including this project's DBR 18 LTS).
# MAGIC It includes GDAL, PROJ and GEOS; `[vizx]` adds the map dependencies.
# MAGIC Cluster nodes need HTTPS access to GitHub release assets and the Python package index.
# MAGIC See the [release](https://github.com/databrickslabs/geobrix/releases/tag/v0.6.0).

# COMMAND ----------

# DBTITLE 1,Register GeoBrix functions and readers
from importlib.metadata import version

from databricks.labs import gbx
from databricks.labs.gbx.vectorx import functions as vx
from databricks.labs.gbx.vizx import as_gdf, plot_interactive, vector_layer

assert version("geobrix") == "0.6.0+env5", "Expected the GeoBrix 0.6.0 env5 wheel"
gbx.register(spark)

# COMMAND ----------

# DBTITLE 1,Verify spatial analysis on Spark
import json

# Executes the registered function on an executor before running the examples.
_gbx_validity = spark.sql(
    "SELECT gbx_st_explainvalidity('LINESTRING (2.295 48.8738, 2.3305 48.8655)') AS validity"
).first().validity
assert json.loads(_gbx_validity)["valid"], _gbx_validity
print(f"GeoBrix {version('geobrix')} ready")
