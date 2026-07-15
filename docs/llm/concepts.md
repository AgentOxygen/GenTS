# Domain Concepts

> Glossary of climate-model and GenTS-specific terminology. GenTS code and docs assume
> these meanings; misreading them leads to wrong changes.

## Climate model output

- **History file (HF):** Raw netCDF output from an Earth system model (CESM, E3SM).
  One file covers one (or a few) time steps and contains *all* variables of an output
  stream. Example: `b.e21.CASE.cam.h0.1850-01.nc` = all atmosphere monthly-mean fields
  for January 1850.
- **Time series file (TS):** Post-processed netCDF file: *one* variable spanning *many*
  time steps. Example: `b.e21.CASE.cam.h0.TREFHT.185001-185912.nc`. Analysts want this
  layout; converting HF → TS is the entire purpose of GenTS.
- **Output stream:** A model's named output frequency channel, encoded in filenames as
  `h0`, `h1`, `h2`, ... (CESM) or `eam.h0`... (E3SM). Different streams have different
  variable sets and time step sizes, so they must never be mixed in one output file.
- **Model component:** Subdirectory per model subsystem in a case's output tree:
  `atm`, `ocn`, `lnd`, `ice`, `glc`, `rof`. Conventionally raw files live under
  `<component>/hist/` and processed output under `<component>/proc/tseries/`.
- **Case:** One model simulation/run and its output directory tree.

## netCDF / time handling

- **netCDF:** Self-describing binary array format. GenTS uses `netCDF4-python`
  exclusively (no xarray, no dask). Files hold dimensions, variables (with dtype,
  dimensions, attributes), and global attributes.
- **CFTime:** Calendar-aware datetime objects from the `cftime` package. Climate models
  use non-standard calendars (`noleap`, `360_day`, ...), so ordinary `datetime` is wrong.
  Raw `time` values are floats ("days since 1850-01-01"); the `units` and `calendar`
  attributes on the time variable are required to decode them — files missing either
  are rejected (`netCDFMeta.__init__` raises `AttributeError`).
- **Time bounds:** A companion variable (`time_bnds`/`time_bnd`/`time_bounds`/`time_bound`,
  matched case-insensitively) giving the interval each time step averages over. Critical
  quirk: for monthly means, the `time` value is often the *end* of the interval
  (Feb 1 for a January mean), so GenTS defaults to the **midpoint** of the bounds to
  decide which month/year a step belongs to.
- **Time alignment method:** How a representative timestamp is chosen from bounds:
  `midpoint` (default), `direct_time` (raw time value, ignores bounds), `start_bound`,
  `end_bound`. Appears in `slice_groups`, `update_ts_orders`, and the CLI `--align_method`.

## GenTS-specific concepts

- **Primary variable:** A time-varying, multi-dimensional scientific field (e.g. surface
  temperature). Each primary variable gets its own TS output file. Classified by
  `gents.meta.is_var_secondary` (see rules in its docstring).
- **Secondary variable:** Everything else — coordinates (`lat`, `lon`, `time`), bounds,
  character/metadata variables. Copied unchanged into *every* TS file of the group so
  each output is self-describing.
- **Auxiliary order:** When a group has *no* primary variables, one TS file named with
  variable placeholder `auxiliary` is written containing only the secondary variables.
- **Group:** Files sharing a parent directory and filename prefix (prefix = filename
  minus its last 2 `.`-delimited tokens, per `sort_hf_groups`). Group keys are glob-like
  strings: `"/data/case/atm/hist/case.cam.h0*"`. One group ≙ one output stream in one
  directory.
- **Slicing:** Partitioning a group's files into fixed-width year windows
  (default 10 years) so TS files stay a manageable size. Sliced group keys get a
  `"[sorting_pivot]<start>-<end>"` suffix, parsed later by `TSCollection`.
- **Multistep slicing:** A single HF can contain many time steps that straddle a slice
  boundary; `HFCollection` records per-file `(start_index, end_index)` tuples
  (`get_multistep_slices`) so `TSCollection` can split within a file.
- **Fragmented (tiled) files:** Some models split one time step *spatially* across
  several files (e.g. per latitude band); detected by paths not ending in `.nc` and
  merged by matching non-time dimension bounds (`merge_fragmented_groups`).
  `MHFDataset` stitches tiles back together by coordinate matching, and requires the
  fragment count to be identical at every time step ("time consistent").
- **Order:** A plain dict describing one TS output file to generate — the unit of work
  of `TSCollection`. See [architecture.md](architecture.md) for the exact schema.
- **Integrity stamp:** Every completed TS file gets a `gents_version` global attribute.
  Dual use: (1) output files lacking it are considered corrupt/partial and are
  regenerated; (2) *input* files carrying it are recognized as GenTS output and
  excluded from processing (`netCDFMeta.is_valid`).
- **Conforming chunking:** Output chunking convention checked by
  `check_timeseries_conform`: `time` stored contiguously; large variables chunked along
  time so each chunk is ≈ 4 MiB (CMOR-friendly). Files < 4 MiB are stored contiguously.
- **Dry run:** CLI `--dryrun` — full metadata read and order construction, but no writes;
  prints how many TS files would be generated.
