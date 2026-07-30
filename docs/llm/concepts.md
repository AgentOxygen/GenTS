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
  `gents.meta.is_var_secondary` (see rules in its docstring). All name/dimension
  comparisons there are **case-insensitive**, so a record dimension named `Time`
  (MOM6) or a bounds variable named `Time_Bounds` is recognized like its lowercase form.
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
- **Skip-empty writes:** `write_timeseries_file` creates each output variable with the
  source's `_FillValue` and leaves any data slice that is *entirely* that fill value
  unwritten (netCDF stores nothing and returns the fill on read). A no-op for real data;
  it propagates the missing-value-clone trick through TS generation so time series built
  from clones stay as small as their inputs. Only engages when a `_FillValue` is present
  (NaN is compared with `isnan`, since `NaN != NaN`). Note this keeps the *output* small
  but still *reads* the source data — for clones that read is wasted work; see `--no-data`.
- **`--no-data` (skip primary data):** CLI/`execute(no_data=True)` option that creates each
  primary variable (with its fill value) but neither reads nor writes its data, leaving it
  unwritten. Secondary variables (coordinates, time, bounds) are still written, so output is
  a structurally valid, self-describing time series whose primaries read back as fill. It is
  `case_builder`'s missing-value technique applied at TS-generation time. Profiling the
  conformity sample showed ~48% of wall time is materialising primary fill arrays that
  skip-empty then discards; `--no-data` skips that read, giving a measured ~3.6x speedup
  (5-yr sample: ~4 min → ~1 min at 16 cores). Used by the conformity suite; the data
  transpose itself stays covered by the unit tests on real (tiny) fixtures.
- **Dry run:** CLI `--dryrun` — full metadata read and order construction, but no writes;
  prints how many TS files would be generated.
- **Missing-value clone (conformity case builder):** A structurally identical copy of a
  history file produced by `gents.conformity.case_builder` (`gents_conform_build`) in which
  the primary/large fields hold no real data, so a multi-GB case directory mirrors down to
  KB for cheap end-to-end testing. Primary variables (per `is_var_secondary`) are
  *created but never written* — HDF5's lazy allocation stores nothing and returns the
  fill value (NaN / integer fill) on read. Secondary variables (coordinates, time, bounds)
  are copied verbatim so the clone stays self-describing. No compression is applied
  (source filters/chunking are mirrored); the savings come purely from unwritten data.
  A **size guard** (`--max-copy-mib`, default 0.5) additionally fills any *multi-dimensional*
  variable above the threshold even if classified secondary, catching large static grid
  geometry (e.g. CICE's ~1 MiB `TLON`/`tarea`/`tmask` arrays). Because lazy allocation
  needs an HDF5 backend, netCDF3/CDF-5 sources are rewritten as `NETCDF4` unless
  `--preserve-format` is set. The clones deliberately mirror the *raw* case tree
  (discovered via `find_files`, not an `HFCollection`) so they also exercise files GenTS's
  filters are meant to ignore. Running GenTS over the clones stays cheap only with
  **`--no-data`** (above): skip-empty alone keeps the output small but still pays to read
  the fill data. The invocation that built a clone is recorded in a `cmd.txt` at the top of
  the clone directory (`CLONE_COMMAND_FILENAME`), the clone-tree equivalent of the
  `gents_command` attribute `run_gents` stamps into each output file. Lines are appended,
  one per invocation that actually cloned files, because a clone is often built up over
  several runs; arguments are `shlex.join`-quoted so a line can be pasted back into a shell
  without its globs expanding.
- **Conformity testing:** End-to-end verification that GenTS handled a *specific model's*
  case the way that model's users need. Distinct from unit testing, and deliberately kept
  in a separate tree (`gents/conformity/`, run by `gents_conform`, not `pytest`). Unit
  tests ask "is this function correct" and inspect GenTS internals against synthetic
  fixtures; conformity asks "did GenTS mishandle this case" and inspects *only the files
  on disk*. The boundary cuts both ways: whether `gents_cesm3.yaml` is parsed correctly is
  a unit test; whether the resulting output is laid out the way a CESM3 researcher needs
  is conformity. A conformity failure is a finding to investigate, not a crash — every
  check that can run does, and the run reports a pass percentage rather than stopping at
  the first problem.
- **Model specification (`gents/conformity/models/*.py`):** One module per model stating
  what correct output looks like for that model, as plain `for`/`if` checks recording into
  a `Report`. Exposes `MODEL`, `SPEC_VERSION` (bumped on any check change, printed in every
  report so a recorded result stays interpretable), and `run(ts_dir, hf_dir, report)`.
  Three properties are load-bearing and easy to "helpfully" break:
  1. **Model-specific by design.** No shared/model-agnostic check layer exists. Duplication
     between specifications is accepted so each stands alone as a readable statement of one
     model's requirements, and so coverage stays explicit (an unverified model shows as
     unverified rather than inheriting generic checks).
  2. **Expectations are restated by hand, never derived from the YAML config.** Asserting
     that GenTS did what its own config said is circular — it can only show GenTS follows
     its config, never that the config is right for the model. The one deliberate exception
     is the stream-coverage section, which reads `input_hf.include`/`exclude` because there
     the patterns are not the subject of the check, only how it works out which streams to
     expect.
  3. **Written plainly on purpose** — explicit loops and `if`s, no lambdas, comprehensions,
     or table-driven dispatch — because researchers audit and extend these files. Checks
     that need to open files share one pass; cheap string checks each keep their own loop.
- **Report / pass-fail-skip:** `gents.conformity.report.Report` collects one result per
  check. **SKIP** means a check could not be evaluated (nothing in the case for it to look
  at, or `hf_dir` not supplied); skips are excluded from the pass percentage rather than
  counted as passes, so a run over a narrow case cannot inflate its own score. A skip is
  itself a coverage signal — "no `day_*` folders were found" means the case proves nothing
  about daily streams. `check_each`-style checks report one result naming the offending
  files, so the check count stays fixed regardless of case size (a 20-check spec scores
  out of 20 whether the case has 10 files or 100,000).
