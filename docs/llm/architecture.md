# Architecture & Data Flow

> How data moves through GenTS, what each module owns, and the shapes of the core
> data structures. Reference symbols by name — every public function/class has a
> thorough docstring in source; this file covers what the docstrings don't: the
> connections between them.

## Pipeline

```
hf_dir (filesystem)
   │  find_files() — os.walk + fnmatch, default pattern "*.nc*"
   ▼
HFCollection {path: None}                          gents/hfcollection.py
   │  .include()/.exclude() — path-glob filters, no I/O
   │  .pull_metadata() — ProcessPoolExecutor → get_meta_from_path() per file
   ▼
HFCollection {path: netCDFMeta}                    gents/meta.py
   │  .get_groups() — sort_hf_groups() by dir + filename prefix,
   │                  then merge_fragmented_groups() for tiled files
   │  .slice_groups() — year windows; keys gain "[sorting_pivot]YYYY-YYYY"
   ▼
TSCollection — list of order dicts                 gents/timeseries.py
   │  modifiers: include/exclude, add_args, apply_compression,
   │             apply_path_swap, apply_overwrite, append_timestep_dirs, add_attrs
   │  .execute() — batches orders sharing source files (optimize=True),
   │               ProcessPoolExecutor → generate_time_series() per batch
   ▼
generate_time_series(hf_paths, ...)                gents/timeseries.py
   │  opens MHFDataset over the group                gents/mhfdataset.py
   │  pre-loads secondary variable data once
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
| `datastore.py` | `GenTSDataStore` — the *only* place `netCDF4.Dataset` is constructed; context manager + delegation | netCDF4 |
| `meta.py` | `netCDFMeta` (cached header metadata), `is_var_secondary` (primary/secondary rules), `get_meta_from_path` (picklable factory), `get_attributes`, `get_time_variables_names` | datastore |
| `hfcollection.py` | `HFCollection` + module-level helpers for discovery, grouping, year math, fragmentation merging | meta, utils |
| `mhfdataset.py` | `MHFDataset` — presents a file group as one virtual dataset; time→file mapping; tile reassembly | datastore, meta |
| `timeseries.py` | `TSCollection`, order construction/execution, `write_timeseries_file` (chunking + integrity stamp + create-time fill value & skip-empty writes via `_is_missing`), `check_timeseries_integrity`, `check_timeseries_conform`, `get_timestamp_format` | mhfdataset, datastore, meta, utils |
| `cli.py` | argparse, YAML config loading/merging, `main()` | hfcollection, timeseries, utils |
| `utils.py` | `enable_logging`, `ProgressBar`, `get_version`, `log_hfcollection_info`, `log_tscollection_info`, `LOG_LEVEL_IO_WARNING = 5` | — |

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
   │  reuses find_files (discovery) + is_var_secondary (classification)
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
| `case_builder.py` | `gents_conform_build`; missing-value cloning, size guard, netCDF3→4 upgrade |
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
}
```

`execute(optimize=True)` groups orders by `(first hf_path, start, end)` key and merges
up to `optimize_batch_n` (default 200) of them into one worker call so each HF group is
opened once, not once per variable.

### `netCDFMeta` (per-file cache)

Caches at construction (one header read, then the file is closed): global attrs, float
and CFTime time values, optional time bounds, all variable names split into
primary/secondary, per-variable shape/dims/dtype, per-dimension coordinate min/max
(`get_dim_bounds`, used for fragmentation matching). Picklable — it crosses process
boundaries as a `ProcessPoolExecutor` result.

### `MHFDataset`

Opens all files of one group together (`open`/`close`, or context manager). Builds
`__time_mapping: {float_time: [file_index, ...]}`; more than one index per time ⇒
fragmented group. `get_var_vals(var, time_index_start, time_index_end)` reads a time
slice, either by picking the right file per step (normal) or by placing each tile into
a pre-allocated array via coordinate matching (fragmented).

## Timestamp / output naming

`get_timestamp_format` picks a strftime format from the group's time-step delta:
sub-minute `%Y%m%d%H%M%S`, hourly `%Y%m%d%H`, daily `%Y%m%d`, monthly `%Y%m`,
yearly `%Y`. `ts_string` is `f"{start.strftime(fmt)}-{end.strftime(fmt)}"` using the
chosen time-alignment method. `append_timestep_dirs` optionally inserts a frequency
directory (`hour_6/`, `day_1/`, `month_1/`, `year_1/`) before the filename.

## Parallelism model

Two independent process pools, both plain `concurrent.futures.ProcessPoolExecutor`:

1. **Metadata pull** (`HFCollection.pull_metadata`, CLI `--hfcores`, default 64):
   scales strongly — header reads are tiny and independent.
2. **TS generation** (`TSCollection.execute`, CLI `--tscores`, default 8): scales
   weakly — bound by disk write throughput.

Everything submitted to a pool must be picklable: module-level functions
(`get_meta_from_path`, `generate_time_series`) with plain-data arguments. Failures in
workers are logged (`raise_errors=False` default) rather than raised, so a bad file
doesn't kill a long batch job. Dask was removed; `dask_client` kwargs remain only as
deprecation shims.

## Error-handling philosophy

Skip-and-warn, not fail-fast: invalid files are dropped by `check_validity`, minority
variable sets are dropped by `check_groups_by_variables`, worker exceptions are logged.
Logging uses the `"gents"` logger hierarchy; `enable_logging(verbose=True)` turns on
per-file I/O traces at custom level `LOG_LEVEL_IO_WARNING = 5`.
