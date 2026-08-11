# Architecture & Data Flow

> How data moves through GenTS, what each module owns, and the shapes of the core
> data structures. Reference symbols by name — every public function/class has a
> docstring in source; this file covers what the docstrings don't: the connections
> between them.

## Pipeline

```
hf_dir (filesystem)
   │  find_files() — os.walk + fnmatch, default pattern "*.nc*"
   ▼
HFCollection {path: None}                          gents/hfcollection.py
   │  .include()/.exclude() — path-glob filters, no I/O
   │  .pull_metadata() — pool (or serial) → get_meta_from_path() per file
   ▼
HFCollection {path: netCDFMeta}                    gents/meta.py
   │  .get_groups() — sort_hf_groups() by dir + filename prefix,
   │                  then merge_fragmented_groups() for tiled files
   │  .slice_groups() — year windows; keys gain "[sorting_pivot]YYYY-YYYY";
   │                    per-file cut points recorded for straddling files
   ▼
TSCollection — list of order dicts                 gents/timeseries.py
   │  modifiers: include/exclude, add_args, apply_compression,
   │             apply_chunk_target_bytes, apply_path_swap, apply_overwrite,
   │             append_timestep_dirs, add_attrs
   │  .execute() — batches orders sharing source files (optimize=True),
   │               pool (or in-process) → generate_time_series() per batch
   ▼
generate_time_series(hf_paths, ...)                gents/timeseries.py
   │  opens MHFDataset over the group                gents/mhfdataset.py
   │  one pass over the files fills the cache (secondaries + preloadable primaries)
   │  write_timeseries_file() per primary variable
   ▼
one .nc file per variable per group per time slice
   name: {ts_path_template}.{variable}.{ts_string}.nc
```

The CLI (`gents/cli.py`) is a thin driver over this exact pipeline, parameterized by a
bundled YAML config (`gents/configs/*.yaml`) merged with command-line flags.

## Module responsibilities

| Module | Owns | Depends on |
|---|---|---|
| `datastore.py` | `GenTSDataStore` — the *only* place `netCDF4.Dataset` is constructed in the pipeline; context manager + delegation | netCDF4 |
| `meta.py` | `netCDFMeta` (cached header metadata, with opt-out load flags), `is_var_secondary` (primary/secondary rules), `get_meta_from_path` (picklable factory), `get_attributes`, `get_time_variables_names` | datastore |
| `hfcollection.py` | `HFCollection` + module-level helpers for discovery, grouping, year math, fragmentation merging | meta, utils |
| `mhfdataset.py` | `MHFDataset` — presents a file group as one virtual dataset; time→file mapping; memory-bounded variable cache; tile reassembly | datastore, meta |
| `timeseries.py` | `TSCollection`, order construction/execution, `write_timeseries_file`, `compute_chunksizes`, `CHUNK_TARGET_BYTES`, `check_timeseries_integrity`, `check_timeseries_conform`, `get_timestamp_format`, `get_timestep_label` | mhfdataset, datastore, meta, utils |
| `cli.py` | argparse, YAML config loading/merging, `check_config`, `main()` | hfcollection, timeseries, utils |
| `utils.py` | `enable_logging`, `ProgressBar`, `get_version`, `get_time_stamp`, `log_hfcollection_info`, `log_tscollection_info`, `LOG_LEVEL_IO_WARNING = 5` | — |

Dependency direction is one-way: `cli → (hfcollection, timeseries) → (meta, mhfdataset) → datastore`.
`utils` is a leaf used by the upper layers. Keep it that way.

## Conformity subsystem (`gents/conformity/`)

Sits *outside* the pipeline above and outside the pytest suite — it verifies GenTS's
output on disk rather than its internals. `gents/conformity/README.md` is the
authoritative guide; read it before editing a specification. Its modules may construct
`netCDF4.Dataset` directly (see [conventions.md](conventions.md)).

```
real case dir
   │  gents_conform_build → case_builder.clone_netcdf_with_missing()
   │  reuses find_files + sort_hf_groups (discovery) + is_var_secondary (classification)
   ▼
missing-value clone (small enough to store/share)
   │  run_gents --no-data  (the normal pipeline, primaries never read/written)
   ▼
time series output tree
   │  gents_conform → check.main()
   │     models/__init__.SPECIFICATIONS[model] → spec.run(ts_dir, hf_dir, report)
   ▼
Report → console text + optional JSON; exit 0 if conformant, 1 if any check failed
```

| Module | Owns |
|---|---|
| `case_builder.py` | `gents_conform_build`; missing-value cloning, size guard, netCDF3→4 upgrade, `log_case_summary` |
| `check.py` | `gents_conform` argparse + driver; model lookup, exit code |
| `report.py` | `Report`, `CheckResult`; `check`/`check_each`/`skip`, `pass_rate`, `to_dict`, `format_text` |
| `models/__init__.py` | `SPECIFICATIONS` registry (mirrors `model_config_files` in `cli.main`) |
| `models/cesm3.py` | The CESM3 specification — four sections: directory structure, file names, file contents, comparison against the original case |

Adding a model = write `models/<model>.py` exposing `MODEL`, `SPEC_VERSION`,
`run(ts_dir, hf_dir, report)`, then register it in `SPECIFICATIONS`. Do **not** factor
shared checks out of an existing specification; see the "model specification" concept in
[concepts.md](concepts.md) for why duplication is intended here.

## Core data structures

### Group dictionary (`HFCollection.get_groups()`)

```python
{ "<parent_dir>/<filename_prefix>*": [Path, Path, ...],                # unsliced
  "<parent_dir>/<prefix>*[sorting_pivot]1850-1859": [Path, ...] }     # sliced
```

### Order dictionary (`TSCollection` internal unit of work)

Built by `update_ts_orders`, consumed by `execute` → `generate_time_series` →
`write_timeseries_file`:

```python
{
  "hf_paths": [Path, ...],           # source history files, time-sorted
  "ts_path_template": str,           # output path prefix, no variable/timestamp yet
  "primary_var": str,                # variable name, or "auxiliary"
  "secondary_vars": [str, ...],
  "ts_string": str,                  # "18500101-18591231"-style timestamp suffix
  "ts_start_index": int | None,      # multistep slice bounds into the aggregated
  "ts_end_index": int | None,        #   time axis (None = whole range)
  # optional, added by modifiers:
  "complevel": int, "compression": str, "overwrite": bool, "append_attrs": dict,
  "chunk_target_bytes": int,  # via apply_chunk_target_bytes; see conventions.md's 4 MiB rule
}
```

`ts_path_template` is derived inline in `update_ts_orders`, not by a helper: the group
key has the input head dir split off its front, any `"[sorting_pivot]<years>"` suffix
removed, the output dir prepended, and the group key's trailing `*` dropped with
`[:-1]`. The whole filename prefix is kept (`model.cam.h0`), and `hist`→`tseries`
renaming is *not* done here — that is `apply_path_swap`'s job, applied afterwards as a
modifier.

`execute(optimize=True)` groups orders by `(first hf_path, start, end)` key and merges
up to `optimize_batch_n` (default 200) of them into one worker call so each HF group is
opened once, not once per variable.

### `netCDFMeta` (per-file cache)

Caches at construction (one header read, then the file is closed): global attrs, raw
float time values, optional time bounds, all variable names split into
primary/secondary, per-variable shape/dims/dtype/attrs, per-dimension coordinate
min/max (`get_dim_bounds`, used for fragmentation matching). Picklable — it crosses
process boundaries as a pool result.

Four constructor flags skip work a caller does not need. `MHFDataset` uses all four:

| Flag | Default | Effect when off |
|---|---|---|
| `decode_dates` | `True` | `num2date` is deferred to the first `get_cftimes()`/`get_cftime_bounds()` call (needs only cached values, not an open file). `get_meta_from_path` — i.e. the whole pipeline — opts out; pipeline code decodes endpoints only, via `decode_time_values()`/`decode_time_bounds_values()` |
| `load_time_bounds` | `True` | the bounds array is never read; the bounds getters raise if the file actually has one |
| `load_variable_attrs` | `True` | per-variable attrs are not read; `get_variable_attrs` raises |
| `compute_dim_bounds` | `True` | coordinate bounds are not computed; `get_dim_bounds` raises |

Opting out is deliberately strict — the getter raises rather than returning stale or
empty data.

### `MHFDataset`

Presents one group as a single virtual dataset. Files are opened **one at a time**, not
all at once: `open()` walks the group once building the time mapping and filling the
cache, and any variable that did not fit is reread from its own file on demand. The
number of simultaneously open handles is therefore 1, independent of group size.

- **Time mapping:** `{float_time: [(file_index, sub_time_index), ...]}`. `sub_time_index`
  is the step's position inside its own file, precomputed so reads never rescan a time
  array. More than one entry per time ⇒ fragmented group.
- **Cache:** `open()` caches every secondary variable plus as many primaries from
  `preload_var_list` as fit under `memory_limit_bytes` (`__plan_cacheable_vars`, sized
  from one file's shape × file count). All-or-nothing per variable — a partial entry
  would serve the wrong file's data. Anything left over is cached on first use if it
  fits, otherwise only the requested time slice is read from disk per access.
  `preload_primaries=False` (used by `no_data` runs) skips all primary preloading —
  primaries stay readable on demand.
- **Eviction:** reads move through one variable at a time, so switching variables frees
  the previous one's cache. A single file's entry is *not* freed after one read, since
  write chunks don't align with source file boundaries.
- **Reads:** `get_var_vals(var, start, end)` reads maximal runs of consecutive steps that
  land in the same file in one slice read (non-fragmented), or places each tile into a
  pre-allocated array by coordinate matching (fragmented). The sorted time axis and the
  fragmentation flag are computed once at `open()` and cached.

## Timestamp / output naming

`get_timestamp_format` picks a strftime format from the group's time-step delta:
sub-minute `%Y%m%d%H%M%S`, hourly `%Y%m%d%H`, daily `%Y%m%d`, monthly `%Y%m`,
yearly `%Y`. `ts_string` is `f"{start.strftime(fmt)}-{end.strftime(fmt)}"` using the
chosen time-alignment method; `update_ts_orders` finds the range on raw float times
(alignment applied vectorized) and decodes only each file's two endpoint candidates. `get_timestep_label` maps the same delta to a frequency
label (`hour_6`, `day_1`, `month_1`, `year_1`, or `unsorted`), used by
`append_timestep_dirs` and by `case_builder.log_case_summary`.

## Parallelism model

Two independent stages, each with its own worker count:

1. **Metadata pull** (`HFCollection.pull_metadata`, CLI `--hfcores`, default 64):
   scales strongly — header reads are tiny and independent.
2. **TS generation** (`TSCollection.execute`, CLI `--tscores`, default 8): scales
   weakly — bound by disk write throughput.

Both take a plain `concurrent.futures.ProcessPoolExecutor` when `num_processes > 1` and
a **serial in-process loop** when it is `1` — no pool is constructed at all. That is what
makes `py-spy` see real frames (see the profiling section of
[workflows.md](workflows.md)).

Everything submitted to a pool must be picklable: module-level functions
(`get_meta_from_path`, `generate_time_series`) with plain-data arguments. Failures in
workers are logged (`raise_errors=False` default) rather than raised, so a bad file
doesn't kill a long batch job. Dask was removed; `dask_client` kwargs remain only as
deprecation shims.

Memory is bounded per worker, not globally: `execute(memory_limit_bytes=...)` (CLI
`--memory-limit`, in GB) is forwarded to every `MHFDataset` a worker opens, so the
process-wide ceiling is roughly `tscores × memory_limit`. The default is
`DEFAULT_MEMORY_LIMIT_BYTES` (4 GiB per worker); constructing an `MHFDataset` directly
is still unbounded by default.

## Error-handling philosophy

Skip-and-warn, not fail-fast: invalid files are dropped by `check_validity`, minority
variable sets are dropped by `check_groups_by_variables`, worker exceptions are logged.
Logging uses the `"gents"` logger hierarchy; `enable_logging(verbose=True)` turns on
per-file I/O traces at custom level `LOG_LEVEL_IO_WARNING = 5`.

The one hard-fail is `get_group_timestep_delta` raising `ValueError` on a group with
fewer than two total time steps — see the gotcha in [conventions.md](conventions.md).
