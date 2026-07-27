# Conventions, Invariants & Gotchas

> Rules the codebase relies on that are easy to violate, plus known stale spots.
> Verify a gotcha still exists before acting on it — this file describes the code as of
> July 2026 (branch `tscollection-interface`).

## API invariants — do not break

- **Immutable fluent API.** Every filter/modifier on `HFCollection` and `TSCollection`
  returns a *new* instance via `.copy()`; user code chains calls
  (`hfc.include(...).exclude(...)`). New methods on these classes must follow the same
  pattern — mutate nothing, return `self.copy(...)` with overrides. (Known internal
  exceptions that cheat: `sort_along_time` and `slice_groups` also mutate private state
  before copying — don't imitate them.)
- **Lazy metadata.** `HFCollection` maps paths to `None` until `pull_metadata()` runs.
  Path-glob filters must stay usable *before* the pull (that's the performance model:
  filter first, read headers second). Methods needing metadata call `check_pulled()`,
  which auto-pulls.
- **Copies inherit metadata.** Filtering a pulled collection yields pulled
  sub-collections; never re-read headers that a parent already read.
- **Worker picklability.** Anything submitted to `ProcessPoolExecutor` must be a
  module-level function with picklable args. `get_meta_from_path` exists precisely as
  the picklable factory for `netCDFMeta`; keep that pattern.
- **`gents_version` stamp semantics.** Written last in `write_timeseries_file`, so its
  presence ⇒ complete file. It is simultaneously the input-exclusion marker
  (`netCDFMeta.is_valid` returns `False` for files that have it). Changing when/where
  it is written breaks resume-after-crash *and* input filtering.
- **4 MiB chunking rule.** `write_timeseries_file` and `check_timeseries_conform`
  implement the same convention (contiguous below 4 MiB, ~4 MiB time-chunks above).
  Change them together or `test_conform_check`-style tests will catch you.
- **`[sorting_pivot]` group-key suffix** is the string protocol between
  `HFCollection.slice_groups` and `TSCollection.update_ts_orders`. Both sides parse it
  literally; change it in both places or nowhere.
- **All user-facing filters are `fnmatch` globs** applied to *absolute path strings*
  (or variable names for `var_glob`). Not regex, not `pathlib.match`.
- **Only `GenTSDataStore` opens netCDF files** in the *pipeline*. Never instantiate
  `netCDF4.Dataset` directly outside `datastore.py` — except in
  `gents/validation/case_builder.py`, which deliberately uses `netCDF4.Dataset` directly
  because cloning needs low-level `createVariable` control (filters, chunking, fill value)
  that the thin datastore wrapper doesn't expose. Don't "fix" that to use `GenTSDataStore`.
- **`is_var_secondary` matching is case-insensitive.** Variable-name, secondary-dimension,
  and primary-dimension (`time`) comparisons all lower-case both sides. This is load-bearing
  for MOM6-style output (`Time`/`Time_Bounds`); without it every field misclassifies as
  secondary. Keep it case-insensitive if you touch the function.
- **Scope guard.** GenTS reads data values only to copy them. Any feature that computes
  on, regrids, or renames data is out of scope per the developer guide.

## Process conventions

- **Test-driven development** is the stated project process: new features and bug fixes
  start with new failing tests in `gents/tests/`, then code changes make them pass.
- Docstrings are Sphinx/reST style (`:param:`/`:returns:`) and are rendered into the
  API docs — keep them current; they are the primary API reference.
- Private attributes use double-underscore name mangling (`self.__hf_to_meta_map`).
- Loggers are `logging.getLogger(__name__)` under the `"gents"` hierarchy.

## Known gotchas / stale spots (verified 2026-07)

- **`build/lib/` is a stale copy** of the whole package tree. Never read, edit, or grep
  it as if it were source. Same for `GenTS.egg-info/` and `__pycache__/`.
- **CLI references missing configs.** `cli.main` maps `--model cesm2` →
  `gents_cesm2.yaml` and `--model e3sm` → `gents_e3sm.yaml`, but only
  `gents_example.yaml` and `gents_cesm3.yaml` exist in `gents/configs/`. Selecting
  cesm2/e3sm currently dies with `FileNotFoundError` at `open()`, not a friendly error.
- **`cli.main`'s docstring is outdated** — it describes the old `run_config` import
  mechanism; the code now loads YAML configs (yaml-refactor, PR #92).
- **`calculate_year_slices` quirks:** the guard's error message is inverted
  ("Maximum year cannot exceed minimum year" fires when max < min), and the early
  return triggers when `slice_size_years >= max_year - min_year`, so a span exactly
  equal to the slice size returns a single unsliced range.
- **CLI `--slice_start_year` default is `None`, but the API `slice_groups(start_year=0)`
  defaults to `0`** — meaning API users get slice windows aligned to year 0 unless they
  pass `start_year=None` explicitly. The bundled YAMLs set `start_year: null`.
- **Deprecations kept as shims:** `include_patterns`/`exclude_patterns` (use
  `include`/`exclude`) and the `dask_client` kwarg on both collections (Dask removed;
  use `num_processes`). Their deprecation warnings misleadingly say "TSCollection"
  even in `HFCollection`.
- **`enable_logging` semantics are inverted vs. intuition:** `verbose=True` sets the
  level to 5 (`LOG_LEVEL_IO_WARNING`, *more* output than DEBUG); `verbose=False` sets
  DEBUG. There is no quiet mode.
- **`utils.log_hfcollection_info` / `log_tscollection_info` size estimates** multiply
  per-file variable sizes by file counts — approximations, not exact output sizes
  (`log_tscollection_info` multiplies by `len(hf_paths)` twice for the total).
- **Fragment detection heuristic:** a group is "fragmented" iff its first file's path
  does **not** end in `.nc` (tiled outputs look like `*.nc.0001`). Files named
  unconventionally can be misclassified. Default discovery glob `*.nc*` deliberately
  catches these.
- **`HFCollection.__init__` raises `FileNotFoundError`** when the directory contains no
  matches — constructing a collection is never a silent no-op.
- **`MHFDataset` trusts the first file** of a group for variable dims/dtype/attrs;
  variable-set consistency is enforced earlier by `check_groups_by_variables`
  (majority wins, minority files dropped with a warning).
- **Time-bounds variable names are matched case-insensitively** against exactly:
  `time_bnds`, `time_bnd`, `time_bounds`, `time_bound`; the time variable must have
  `units` and `calendar` attributes or the file is rejected.
- **Docs/README drift:** `docs/user.rst` shows `exclude(glob=[...])` but the parameter
  is positional `glob_patterns`; README's API example passes `include_years(0, 5)`
  which only works for simulations whose calendar years actually start near 0.
