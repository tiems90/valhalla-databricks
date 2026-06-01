# Valhalla Notebook Improvements — Design Spec

**Date:** 2026-06-01  
**Branch:** feature/france  
**Scope:** All five notebooks in `src/notebooks/`

---

## Background

A full code review identified 19 findings across the five notebooks: 7 HIGH (correctness/reliability), 8 MEDIUM (performance/readability), 4 LOW (style/minor). This spec describes the plan to address all of them.

---

## Approach

Fix-by-fix across notebooks, grouped by theme. `valhalla_map_matching.py` leads every phase since it has the most findings and is the primary production-facing notebook. Each phase ends with an interactive test checkpoint on the Azure cluster using Andorra tiles (fast, already built). Compile/init changes are saved for last since they require a fresh cluster.

---

## Testing Strategy

- **Interactive cluster:** Azure `Standard_D8s_v3` single-node with `/Volumes/timo/geospatial/valhalla_andorra/init.sh`
- **Andorra tiles** for all routing/matching tests — small region, fast actor load
- **No full job runs** until Phase 6 — all intermediate testing is interactive notebook execution
- **Phase 6 only:** fresh cluster compile run to validate init script changes

---

## Phase 0 — Shared Utilities Notebook

**Goal:** Eliminate the `validate_identifier` copy-paste (M6).

**Deliverable:** `src/notebooks/valhalla_utils.py`

Contents:
- `validate_identifier(name, identifier_type)` — unchanged logic from `00_initial_setup`
- `print("✅ valhalla_utils loaded")` sentinel at the end

All four notebooks (`00_initial_setup`, `01_process_pbf`, `quickstart`, `test_routing`) replace their local `validate_identifier` definition with `%run ./valhalla_utils` at the top of their parameters cell.

**Test checkpoint:**
```python
%run ./valhalla_utils
validate_identifier("timo", "catalog")          # must pass silently
validate_identifier("bad-name", "catalog")      # must raise ValueError
validate_identifier("", "schema")               # must raise ValueError
```

---

## Phase 1 — HIGH Fixes: `valhalla_map_matching.py`

Six changes to `valhalla_map_matching.py`. Applied together, tested together.

### H1 — Actor lifecycle + config_path closure

**Problem:** `config_path` captured as driver-side closure; no explicit Actor destruction on task retry.

**Fix:**
```python
# Before mapInPandas call:
config_path_bc = spark.sparkContext.broadcast(config_path)

def match_traces(batch_iter, match_cfg=_MATCH_CONFIG):
    actor = valhalla.Actor(config_path_bc.value)
    try:
        for pdf in batch_iter:
            yield _process_batch(actor, pdf, match_cfg)
    finally:
        del actor
```

### H2 — Chunk coord dedup

**Problem:** `decode_polyline("")` may return `None`; guard logic is fragile.

**Fix:**
```python
coords = decode_polyline(result.get("shape", "")) or []
skip = _CHUNK_OVERLAP if i > 0 else 0
all_coords.extend(coords[skip:])
```

### H3 — `json.loads` outside try + untyped exceptions

**Problem:** Malformed `shape_json` raises `json.JSONDecodeError` outside the try block, killing the entire batch.

**Fix:** Move `shape = json.loads(row["shape_json"])` inside the `try`. Change error formatting to `f"{type(e).__name__}: {e}"`.

### H4 — Short trace guard

**Problem:** Traces with < 2 points raise a cryptic Valhalla error.

**Fix:** First line of `_match_shape`:
```python
if len(shape) < 2:
    raise ValueError(f"Trace requires ≥2 points, got {len(shape)}")
```

### H5 — Freeze `MATCH_CONFIG`

**Problem:** `MATCH_CONFIG` is a mutable driver-side dict; if re-run mid-session, running Spark tasks see stale config and `result_json` records inaccurate provenance.

**Fix:**
```python
_MATCH_CONFIG = dict(MATCH_CONFIG)  # snapshot before submission
```
Pass as a default argument rather than closure capture:
```python
def match_traces(batch_iter, match_cfg=_MATCH_CONFIG):
    ...
    matched = _match_shape(actor, shape, **match_cfg)
    result_json = json.dumps({..., "match_config": match_cfg})
```

### H7 — Zero-edge success rows

**Problem:** A trace that returns 0 edges gets `error = NULL`, `geometry = NULL` — invisible to the `error.isNull()` filter.

**Fix:** After `_match_shape` call:
```python
if not edges:
    raise ValueError("Valhalla returned 0 matched edges — trace may be outside tile coverage")
```

**Phase 1 test checkpoint:**
1. Run the distributed cell end-to-end on Andorra cluster (100 synthetic traces)
2. Verify VARIANT `result` column is populated
3. Verify `geometry` column has SRID 4326 (`ST_SRID(geometry) = 4326`)
4. Inject a malformed `shape_json` row — confirm it appears in `error` with typed exception
5. Inject a 1-point trace — confirm `ValueError` with "≥2 points" message
6. Rerun with `MATCH_CONFIG` widget changed mid-session — confirm VARIANT records the snapshot value, not the new value

---

## Phase 2 — HIGH Fixes: Other Notebooks

### H3 in `valhalla_test_routing.py`

Bare `except:` in `route_batch` → `except Exception as e:` with `f"{type(e).__name__}: {e}"`.

### H6 in `valhalla_01_process_pbf.py`

After the SQL volume creation block, add accessibility guard:
```python
try:
    dbutils.fs.ls(volume_path)
except Exception as e:
    raise RuntimeError(f"Volume not accessible at {volume_path}: {e}") from e
```

**Phase 2 test checkpoint:**
1. Run `test_routing` distributed cell interactively on Andorra cluster
2. Pass a non-existent `VOLUME_PATH` to `01_process_pbf` parameters cell — confirm `RuntimeError` fires before the `%sh` tile build cell

---

## Phase 3 — MEDIUM Fixes: `valhalla_map_matching.py`

### M1 — Capture `count()` return value

```python
total_count = matched_df.count()  # materialises cache; reused in failure-rate cell
```

### M2 — Distance-weighted avg speed

```python
total_len = sum(e.get("length", 0) for e in edges)
avg_speed = (
    sum(e.get("speed", 0) * e.get("length", 0) for e in edges) / total_len
    if total_len > 0 else None
)
```

### M3 — Partition sizing guidance

```python
N_PARTITIONS = max(20, len(traces) // 2000)
trips_df = trips_df.repartition(N_PARTITIONS)
# Target 1K–5K rows/partition. At 10M traces: N_PARTITIONS ≈ 5000
```

### M4 — Slim VARIANT + typed edges array

`result` VARIANT stores only: `confidence_score`, `n_chunks`, `n_input_points`, `match_config`.

New `edges` column: typed array of structs with 5 fields per edge (`way_id`, `road_class`, `length_km`, `speed_kmh`, `is_toll`). Written as a JSON string `edges_json` in `mapInPandas` (add `edges_json string` to the schema string), converted post-hoc:

```python
edge_schema = ArrayType(StructType([
    StructField("way_id",      LongType()),
    StructField("road_class",  StringType()),
    StructField("length_km",   DoubleType()),
    StructField("speed_kmh",   DoubleType()),
    StructField("is_toll",     BooleanType()),
]))
...
.withColumn("edges", from_json("edges_json", edge_schema))
.drop("edges_json")
```

### M8 — Document `confidence_score = None`

Add to `_match_shape` docstring:
> Returns `min(confidence_score)` across all chunks. `None` if Valhalla did not return a confidence score for any chunk — this is common for `trace_attributes` on short traces.

**Phase 3 test checkpoint:**
1. Verify `result` VARIANT has exactly 4 keys
2. Verify `edges` column is `ArrayType(StructType(...))` via `printSchema()`
3. Compare `avg_speed_kmh` before and after on a trace with long motorway + short roundabout — values should differ
4. Confirm `total_count` is reused correctly in the failure-rate cell

---

## Phase 4 — MEDIUM Fixes: Other Notebooks

### M5 — `test_routing.py` worker check (documentation only)

Add comment to `check_valhalla_on_worker`:
```python
# spark.range(sc.defaultParallelism) produces exactly one row per partition,
# so this yields exactly one result row per executor — intentional.
```
No code change.

### M6 — `%run ./valhalla_utils` in all notebooks

Remove `validate_identifier` definition from `01_process_pbf`, `quickstart`, `test_routing`. Add `%run ./valhalla_utils` at top of each parameters cell.

### M7 — `os.walk + break` in `01_process_pbf`

Replace the top-level listing (which uses `break`) with `os.listdir(tile_dir)`. Keep the full `os.walk` for the `.gph` file count — remove the `break` there.

**Phase 4 test checkpoint:**
1. `%run ./valhalla_utils` in isolation on cluster — confirm sentinel prints
2. Run parameters cell of each notebook — confirm no `NameError` for `validate_identifier`
3. Run tile verification cell in `01_process_pbf` on Andorra — confirm non-zero `.gph` count

---

## Phase 5 — LOW Fixes: All Notebooks

Applied in a single pass, no dedicated test checkpoint:

| # | File | Change |
|---|------|--------|
| L1 | `quickstart.py` | Portland coords → Oregon: `(45.5051, -122.6750)` |
| L2 | `test_routing.py` | `except:` → `except Exception:` in batch loop |
| L3 | `map_matching.py` | Move `dbf` import to top of its cell; remove unused `Tuple` from typing imports |
| L4 | `map_matching.py` | Add `assert _CHUNK_SIZE <= _VALHALLA_MAX_POINTS` after constants |
| L5 | `00_initial_setup.py` | `dbutils.fs.put(init_script_path, content)` → `open(init_script_path, "w").write(content)` |

**Spot-check:** Run affected cells interactively — no import errors, no assertion failures.

---

## Phase 6 — Compile/Init Changes (Fresh Cluster Required)

**Affected file:** `valhalla_00_initial_setup.py`

Changes from Phase 5 (L5) must be validated with a real compile run since the init script write path changes from DBFS API to direct file write.

**Test checkpoint:**
1. Run `initial_setup` job task on a fresh cluster pointing at Andorra volume
2. Confirm init script written at expected path, non-empty
3. Start a second cluster with that init script attached
4. `import valhalla` on the second cluster — must succeed

---

## Git Strategy

One commit per phase. Phase 0–5 on `feature/france`. Push after each phase. Phase 6 gets its own commit after compile validation.

Commit message format:
```
fix(phase-N): <theme> — <summary of changes>
```

---

## Files Changed

| File | Phases |
|------|--------|
| `src/notebooks/valhalla_utils.py` (new) | 0 |
| `src/notebooks/valhalla_map_matching.py` | 1, 3, 5 |
| `src/notebooks/valhalla_test_routing.py` | 2, 4, 5 |
| `src/notebooks/valhalla_01_process_pbf.py` | 2, 4 |
| `src/notebooks/valhalla_quickstart.py` | 4, 5 |
| `src/notebooks/valhalla_00_initial_setup.py` | 5, 6 |
