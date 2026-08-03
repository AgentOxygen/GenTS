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
  Change them together or `test_conform_check`-style tests will catch you. (`check_timeseries_conform`
  currently mis-implements the contiguous half of this rule — see gotchas below.)
- **Fill value at creation + skip-empty writes.** `write_timeseries_file` passes the
  source `_FillValue` to `createVariable` (and omits it from the `setncatts` copy — it
  can't be set twice) and skips writing any slice that is entirely the fill value, via
  the `_is_missing` helper. This is what keeps time series built from missing-value
  clones small; it is a no-op when no `_FillValue` is present, so real data is never
  dropped. Keep the create-time fill + skip together if you touch this — they're one
  mechanism.
- **`[sorting_pivot]` group-key suffix** is the string protocol between
  `HFCollection.slice_groups` and `TSCollection.update_ts_orders`. Both sides parse it
  literally; change it in both places or nowhere.
- **All user-facing filters are `fnmatch` globs** applied to *absolute path strings*
  (or variable names for `var_glob`). Not regex, not `pathlib.match`.
- **Only `GenTSDataStore` opens netCDF files** in the *pipeline*. Never instantiate
  `netCDF4.Dataset` directly outside `datastore.py` — except under `gents/conformity/`,
  which is not pipeline code: `case_builder.py` needs low-level `createVariable` control
  (filters, chunking, fill value) that the thin wrapper doesn't expose, and the model
  specifications in `models/*.py` deliberately inspect output files with plain
  `netCDF4.Dataset` so a researcher reading a check isn't routed through a GenTS
  abstraction. Don't "fix" either to use `GenTSDataStore`.
- **`is_var_secondary` matching is case-insensitive.** Variable-name, secondary-dimension,
  and primary-dimension (`time`) comparisons all lower-case both sides. This is load-bearing
  for MOM6-style output (`Time`/`Time_Bounds`); without it every field misclassifies as
  secondary. Keep it case-insensitive if you touch the function.
- **Scope guard.** GenTS reads data values only to copy them. Any feature that computes
  on, regrids, or renames data is out of scope per the developer guide.

## Conformity conventions (`gents/conformity/`) — do not break

`gents/conformity/README.md` is authoritative; these are the rules most likely to be
violated by a well-meaning refactor.

- **Conformity is not unit testing.** Separate tree, separate runner (`gents_conform`, not
  `pytest`), separate question. Checks look *only* at files on disk — never import GenTS
  internals to verify them, and never move an internals-level check (config parsing, data
  transposition) into a specification. It belongs in `gents/tests/`.
- **Never derive a check's expectation from the YAML config.** It is circular. Restate what
  the model requires by hand. Sole exception: the stream-coverage section reads
  `input_hf.include`/`exclude` because there the patterns are an *input* to the check, not
  its subject. Applying that exception anywhere else defeats the whole design.
- **Don't refactor specifications into shared helpers.** No model-agnostic check layer is
  wanted. Duplication across `models/*.py` is deliberate.
- **Don't "modernise" the plain style.** Explicit `for`/`if`, no lambdas, comprehensions, or
  lookup tables driving logic. These files are audited by researchers, not just developers.
  Checks needing file I/O share one pass; cheap string checks each keep their own loop
  (measured: merging the cheap loops saved 0.6 ms against 1442 ms of file opening).
- **One result per check, not per file.** Report offenders inside a single result. Emitting
  a result per file makes the pass percentage scale with case size instead of spec coverage.
- **Bump `SPEC_VERSION`** whenever checks change — recorded results reference it.
- **Empty populations report SKIP, not PASS.** A vacuous pass inflates the score and hides
  a coverage gap.
- **No sampling.** There is deliberately no flag to check a subset of output. A conformity
  result is published as evidence; a result drawn from a sample cannot support the claim it
  appears to make. If a case is too slow to check in full, build a smaller case.

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
- **`check_timeseries_conform` is broken for contiguous/scalar variables.** It does
  `list(var.chunking())`, but `chunking()` returns the *string* `"contiguous"` for
  contiguously-stored variables, so `list(...)` yields `['c','o','n',...]` — never equal to
  the shape. Such variables then fall through to `"time" not in dimensions → return False`.
  Its own docstring says contiguous storage should pass, so this is a bug, not a
  convention. Any time series carrying a scalar (CAM's `ndbase`/`nsbase`/`nbdate`/`nbsec`/
  `mdt`) is wrongly reported non-conforming — 188 of 1224 files on the CESM3 conformity
  sample. Fixing it means handling the `"contiguous"` sentinel *and* scalar/1-D variables.
- **`append_timestep_dirs` is dead config in the CLI.** `gents_cesm3.yaml` sets
  `output_ts.append_timestep_dirs: true`, but `cli.main` only reads `path_swaps` and
  `compression` from `output_ts` — `TSCollection.append_timestep_dirs()` is never called
  anywhere outside its own definition. So CLI output has no `month_1/`-style frequency
  directories despite the config asking for them, and any CLI-driven output fails the
  conformity check for it. Either wire the key up in `cli.main` or drop it from the YAML.
- **`EMFILE` on wide streams.** A group with very many files (e.g. ~1800 daily
  `cpl.hx.*.nc` in the CESM3 sample) opens them all at once via `MHFDataset` and dies with
  `OSError: [Errno 24] Too many open files`; `execute` logs the worker failure and still
  prints "GenTS done!", so the loss is silent. Workaround is `ulimit -n 65536` /
  `docker run --ulimit nofile=65536:65536`. Note those `cpl` files reach the pipeline via
  the trailing catch-all `*.nc` in `gents_cesm3.yaml`'s `input_hf.include`, not via any of
  the six component globs above it.
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
- **`sort_hf_groups` ordering is part of its contract.** Group keys come out ordered by
  parent directory (order of first appearance in `hf_paths`) then by prefix; within a
  group, paths keep their input order. Downstream order (`get_groups` → `TSCollection`
  orders → output) inherits this, so keep it if you touch the grouping. Prefixes are
  derived with `name.rsplit(delimiter, substring_index)[0]` in a single bucketing pass —
  don't reintroduce a per-unique-prefix rescan of the file list, which made grouping
  quadratic in the number of streams per directory.
- **A filename with no delimiter groups under its whole name.** `sort_hf_groups` strips
  only the tokens that are actually there, so `README` groups under `README*` and
  `gridfile.nc` under `gridfile*`. (Before July 2026 such a name raised `IndexError`;
  `substring_index=0` likewise stripped everything after the *first* delimiter instead of
  stripping nothing.) Every caller uses the default `substring_index=2`.
- **`pull_metadata` hard-raises on single-timestep groups.** Per-file metadata failures are
  logged and the file dropped (`raise_errors=False`), but the *timestep delta* loop
  afterwards re-raises `ValueError` from `get_group_timestep_delta` for any group with
  fewer than two total time steps, taking the whole collection down. Raw case trees hit
  this routinely (initial-condition dumps, single-snapshot history), which is why
  `run_gents --model CESM3` works — its config filters those out — while a bare
  `HFCollection` over the same tree does not. Callers that can't guarantee filtered input
  must catch `ValueError` around the first metadata-triggering call.
- **`include([])` empties a collection; `_passes_filters([])` keeps everything.** The two
  filter paths use opposite conventions for an empty pattern list: `HFCollection.include`
  requires a match against at least one pattern (none ⇒ nothing retained), while
  `gents/conformity/case_builder.py`'s `_passes_filters` treats an empty list as "no filter
  applied". Guard with `if patterns:` when feeding case_builder-style filters into an
  `HFCollection`, or the collection silently comes back empty.
- **Don't reach for `HFCollection` to inspect a raw case tree.** It is built for *viable
  history files*: `check_validity` drops anything `netCDFMeta` rejects (no time coordinate,
  or a time variable missing `units`/`calendar`), and `pull_metadata` then raises on any
  single-timestep group. A raw case directory is full of both — grids, restarts, statics,
  initial-condition dumps — so the collection either silently discards most of the tree or
  dies. `gents/conformity/case_builder.py` therefore inspects cases with the path-only
  `sort_hf_groups` plus its own `_summarize_case_file`, which repeats `netCDFMeta`'s time
  lookup over a `GenTSDataStore` but returns empty-handed where `netCDFMeta` raises. Keep
  that split: the clone must describe every file it copies, not just the GenTS-legible
  ones.
- **`MHFDataset` trusts the first file** of a group for variable dims/dtype/attrs;
  variable-set consistency is enforced earlier by `check_groups_by_variables`
  (majority wins, minority files dropped with a warning).
- **Time-bounds variable names are matched case-insensitively** against exactly:
  `time_bnds`, `time_bnd`, `time_bounds`, `time_bound`; the time variable must have
  `units` and `calendar` attributes or the file is rejected.
- **Docs/README drift:** `docs/user.rst` shows `exclude(glob=[...])` but the parameter
  is positional `glob_patterns`; README's API example passes `include_years(0, 5)`
  which only works for simulations whose calendar years actually start near 0.
