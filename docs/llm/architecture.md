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
| `timeseries.py` | `TSCollection`, order construction/execution, `write_timeseries_file` (chunking + integrity stamp), `check_timeseries_integrity`, `check_timeseries_conform`, `get_timestamp_format` | mhfdataset, datastore, meta, utils |
| `cli.py` | argparse, YAML config loading/merging, `main()` | hfcollection, timeseries, utils |
| `utils.py` | `enable_logging`, `ProgressBar`, `get_version`, `log_hfcollection_info`, `log_tscollection_info`, `LOG_LEVEL_IO_WARNING = 5` | — |

Dependency direction is one-way: `cli → (hfcollection, timeseries) → (meta, mhfdataset) → datastore`.
`utils` is a leaf used by the upper layers. Keep it that way.

`gents/validation/case_builder.py` (the `gents_valid_build` tool) sits *outside* this
pipeline: it reuses `find_files` (discovery) and `is_var_secondary` (classification) but
builds missing-value clones of a case directory rather than time series. See the
"missing-value clone" concept in [concepts.md](concepts.md). It is the one module allowed
to construct `netCDF4.Dataset` directly (it needs low-level `createVariable` control).

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
