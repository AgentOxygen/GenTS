# Conventions, Invariants & Gotchas

> Rules the codebase relies on that are easy to violate, plus known stale spots.
> Verify a gotcha still exists before acting on it — this file describes the code as of
> August 2026 (branch `validation`).

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
- **`num_processes <= 1` means no pool at all.** Both `pull_metadata` and `execute`
  branch to a plain serial loop rather than a one-worker pool. Profiling depends on it
  (see [workflows.md](workflows.md)); keep both branches in step, including their error
  handling — they have historically drifted.
- **`gents_version` stamp semantics.** Written last in `write_timeseries_file`, so its
  presence ⇒ complete file. It is simultaneously the input-exclusion marker
  (`netCDFMeta.is_valid` returns `False` for files that have it). Changing when/where
  it is written breaks resume-after-crash *and* input filtering.
- **4 MiB chunking rule.** `CHUNK_TARGET_BYTES` in `gents/timeseries.py` is the single
  source of the constant, but the *rule* is still implemented twice: `compute_chunksizes`
  (contiguous below the target, time-chunks above) and `check_timeseries_conform`
  (verifying the same). Change them together. Note `check_timeseries_conform` always
  checks against `CHUNK_TARGET_BYTES`, so output written with a custom
  `chunk_target_bytes` will not conform by construction. (It also mis-implements the
  contiguous half of the rule — see gotchas below.)
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

### `MHFDataset` cache invariants

- **A cache entry is all-or-nothing.** `__data_var_cache[var]` is a list with exactly one
  array per file in the group, or the variable is absent from the cache entirely. A
  partial (prefix-only) list makes `__get_hf_data` return a *different file's* data by
  index, silently.
- **One file open at a time.** `open()` and `__cache_variable` both use
  `with GenTSDataStore(path)` per file. Nothing in the class holds a handle open across
  files, and nothing should — that property is what retired the `EMFILE` gotcha.
- **Eviction is per-variable, on switch.** Do not free a file's entry after reading it:
  write chunks don't align with source file boundaries, so the same entry is legitimately
  read more than once per variable.
- **`memory_limit_bytes` is per `MHFDataset`**, i.e. per worker process. The pipeline-wide
  ceiling is roughly `tscores × memory_limit`.

### `netCDFMeta` load flags

`decode_dates`, `load_time_bounds`, `load_variable_attrs`, `compute_dim_bounds` exist to
skip measured per-file cost for callers that don't need the result (`MHFDataset` needs
none of them). Opting out must stay **strict**: the corresponding getter raises
`RuntimeError` rather than returning empty or stale data. Don't "helpfully" soften that —
silent empties would surface as wrong output, not an error.

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
  (measured: merging the cheap loops saved 0.6 ms against 1442 ms of file opening). This
  includes the docstrings and comments in `models/cesm3.py`: they are didactic on purpose
  and are exempt from the lean-docstring convention below.
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
- **Docstring style (as of August 2026):** Sphinx/reST, rendered into the API docs by
  `docs/api.rst` (`automodule ... :members:`). Keep the *prose* lean — a one-sentence
  summary, then only the detail a caller cannot infer — but keep the *fields* complete:
  every `:param:` carries a matching `:type:`, and anything that returns a value carries
  `:rtype:`, because the code is unannotated and autodoc has no other source for types on
  the published documentation pages. A short getter can be a summary line plus a bare
  `:rtype:`. Don't restate defaults the signature already shows. Rationale that belongs
  to the design rather than to the call site (why a mechanism exists, what would break if
  it changed) belongs in this `docs/llm/` set, not in the docstring.
- Private attributes use double-underscore name mangling (`self.__hf_to_meta_map`).
- Loggers are `logging.getLogger(__name__)` under the `"gents"` hierarchy.

## Known gotchas / stale spots (verified 2026-08)

- **`gents/conformity/` is not packaged.** `pyproject.toml` sets
  `packages = ["gents", "gents.configs"]`, an explicit list, so a built wheel/sdist
  contains no `gents.conformity` — while `[project.scripts]` still registers
  `gents_conform_build` and `gents_conform` against it. Both entry points therefore fail
  with `ModuleNotFoundError` on a non-editable install; they work in the repo and in the
  Docker images only because the source tree is present. Fix by adding
  `gents.conformity` and `gents.conformity.models` to `packages` (or switching to
  automatic discovery with an exclude for `gents.tests`).
- **`build/lib/` is a stale copy** of the whole package tree. Never read, edit, or grep
  it as if it were source. Same for `GenTS.egg-info/` and `__pycache__/`.
- **`check_timeseries_conform` is broken for contiguous/scalar variables.** It does
  `list(var.chunking())`, but `chunking()` returns the *string* `"contiguous"` for
  contiguously-stored variables, so `list(...)` yields `['c','o','n',...]` — never equal to
  the shape. Such variables then fall through to `"time" not in dimensions → return False`.
  Its own docstring says contiguous storage should pass, so this is a bug, not a
  convention. Any time series carrying a scalar (CAM's `ndbase`/`nsbase`/`nbdate`/`nbsec`/
  `mdt`) is wrongly reported non-conforming — 188 of 1224 files on the CESM3 conformity
  sample. Reproduced again 2026-08. Fixing it means handling the `"contiguous"` sentinel
  *and* scalar/1-D variables.
- **`append_timestep_dirs` drops orders that don't match `var_glob`.** Its `new_orders.append`
  sits *inside* the `fnmatch` branch, so `tsc.append_timestep_dirs(var_glob="TREFHT")`
  returns a collection containing only `TREFHT` — every other order is silently discarded
  rather than left un-prefixed. Harmless at the default `"*"`, which is the only way it is
  currently called.
- **`append_timestep_dirs` is dead config in the CLI.** `gents_cesm3.yaml` and
  `gents_example.yaml` both set `output_ts.append_timestep_dirs: true`, but `cli.main`
  only reads `path_swaps` and `compression` from `output_ts` — `TSCollection.append_timestep_dirs()`
  is never called anywhere outside its own definition and its tests. So CLI output has no
  `month_1/`-style frequency directories despite the config asking for them, and any
  CLI-driven output fails the conformity check for it. Either wire the key up in
  `cli.main` or drop it from the YAML.
- **CLI references missing configs.** `cli.main` maps `--model cesm2` →
  `gents_cesm2.yaml` and `--model e3sm` → `gents_e3sm.yaml`, but only
  `gents_example.yaml` and `gents_cesm3.yaml` exist in `gents/configs/`. Selecting
  cesm2/e3sm currently dies with `FileNotFoundError` at `open()`, not a friendly error.
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
  `gridfile.nc` under `gridfile*`. Every caller uses the default `substring_index=2`.
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

## Resolved since the last revision (don't re-document as bugs)

- **`EMFILE` on wide streams.** `MHFDataset` used to open every file of a group at once,
  so a ~1800-file daily stream died with `OSError: [Errno 24] Too many open files`. It now
  opens one file at a time and caches data instead of handles, so the ulimit workaround is
  no longer needed. `gents/conformity/README.md` still carries the old warning.
- **`hfcollection.check_config` / `get_default_config`.** Removed (2026-08): they asserted a
  `{name, include, exclude}` config shape that no version of GenTS still uses, were called
  from nowhere, and collided by name with the live `gents.cli.check_config`. The YAML
  schema check is `gents.cli.check_config`, tested in `gents/tests/test_config.py`.
- **Hardcoded chunk-size constant.** Now `gents.timeseries.CHUNK_TARGET_BYTES`, read by
  both `compute_chunksizes` and `check_timeseries_conform`, and overridable per order via
  `apply_chunk_target_bytes`.
