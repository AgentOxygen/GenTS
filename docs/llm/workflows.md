# Workflows & Recipes

> Commands and step-by-step recipes for common tasks. Commands verified against the
> Dockerfile, CI workflow, and pyproject as of July 2026.

## Environment setup

```bash
# Local editable install (inside a venv/conda env)
pip install -e ".[dev]"        # extras: test, docs, bench
# or minimal:
pip install -r requirements.txt && pip install -e .
```

## Testing

```bash
pytest gents/tests/                         # full suite, local env
pytest gents/tests/test_workflow.py         # one file
```

`pytest` covers unit testing only. End-to-end verification against real model cases is a
separate system with its own runner (`gents_conform`) — see
[Conformity checking](#conformity-checking-gents_conform) below. Don't add conformity
checks to `gents/tests/`, or unit-level checks to a model specification.

CI-equivalent (what `.github/workflows/tests.yml` runs):

```bash
docker build --target test -t gents-tests .
docker run --rm gents-tests
```

Docker stages: `runtime` (CLI), `test` (pytest), `bench` (asv), `dev` (bash, editable
install), `deptest-floor`/`deptest-latest` (run tests during build against lowest/newest
dependency resolutions). Bind-mount for live-editing: `docker run --rm -v .:/usr/local/gents -t <image> ...`

Tests generate synthetic netCDF fixtures via `gents/tests/test_cases.py`
(`generate_history_file` + ~15 pytest fixtures: `simple_case`, `structured_case`,
`scrambled_case`, `spatial_fragment_case`, `multistep_case`, `mixed_timestep_case`,
`no_time_bounds_case`, `auxiliary_only_case`, ...). Each fixture returns
`(head_hf_dir, head_ts_dir)` tmp paths. No real model data is needed or included.

**Adding a feature/fix (project process is TDD):**
1. Write a failing test in `gents/tests/`, reusing or extending a `test_cases.py`
   fixture for input data.
2. Implement in `gents/` until the suite passes.
3. Update the reST docstrings and, if behavior/conventions changed, this `docs/llm/` set.

## Docs & benchmarks

```bash
# Sphinx docs with live rebuild at http://localhost:8000
docker run --rm -v .:/usr/local/gents -p 8000:8000 -it gents \
    sphinx-autobuild docs docs/_build/html --host 0.0.0.0

# ASV benchmarks (benchmarks/benchmarks.py) for a tag
asv run v0.9.9^! --machine <machine-name>
asv publish && asv preview
```

ASV suites: `SimpleSuite` (100 history files through create/pull/TS-execute/MHFDataset),
`LargeGroupSuite` (one group of many multi-step files, stressing the timestep-delta loop
in `pull_metadata`), `GroupSortSuite` (48k synthetic *paths* through `sort_hf_groups` —
pure string work, writes no files, and guards against the grouping going quadratic in
streams-per-directory again).

**What ASV does not cover (verified 2026-07):** no suite exercises the chunked write path.
`SimpleSuite`'s variables are `(1, 3, 4)` float64 — 96 bytes — so `write_timeseries_file`
always takes the *contiguous* branch and the 4 MiB time-chunking branch is never reached.
`LargeGroupSuite` benchmarks only `pull_metadata` (headers, no data), and `GroupSortSuite`
touches no filesystem at all. Treat ASV as the commit-to-commit ratchet, not as the
instrument for I/O work — see [Performance profiling](#performance-profiling-py-spy).

### Running ASV inside a container

```bash
asv run --machine gents-container HEAD^!
```

- `--machine <fixed name>` — asv defaults the machine name to the hostname, which is a
  fresh random hex string in every container, so results never accumulate or compare.
  Still needed; unrelated to the two fixes below.
- `asv.conf.json` lists `branches: ["main", "validation"]` — a working/feature branch
  needs to be listed (or use an explicit range like `HEAD^!`) for `asv run` to discover
  its commits without one.

Two things that used to require workarounds here are now fixed in the tree (2026-07),
not just documented around:

- `asv.conf.json` no longer pins `pythons`. It used to force `["3.14"]` against
  3.12-based images, requiring `--python=same` on every invocation; asv's own default
  when `pythons` is unset is the interpreter it's actually running under
  (`asv/config.py`: `[f"{sys.version_info[0]}.{sys.version_info[1]}"]`), so `asv run`
  now works out of the box in any image and still supports testing multiple Python
  versions on a host that actually has them installed — the pin was blocking that, not
  enabling it.
- The `bench` Dockerfile stage now sets `ENV HOME=/usr/local/gents`. Previously `asv
  machine` failed with `PermissionError: [Errno 13] ... '/.asv-machine.json'` (uid 1000
  has no home directory entry, so `HOME` was unset/`/`); `-e HOME=/usr/local/gents` is
  no longer required on `docker run`. Verified against a rebuilt image with the
  documented bind-mount pattern (`-v .:/usr/local/gents`); without a bind mount the
  image's own baked-in `/usr/local/gents` is root-owned from `COPY` and still isn't
  writable by uid 1000 — an existing, separate issue, not something this fix touches.

## Performance profiling (py-spy)

For iterating on a hot path — file chunking, read/write streaming — ASV is the wrong
instrument: it rebuilds an environment per commit, reports min-of-N, and tells you
*whether* something changed, not *where* the time goes. Drive the inner loop with a
standalone driver script under `py-spy`, then add an ASV benchmark to lock in the win.

`py-spy` ships in the `bench` extra (`pyproject.toml`), so it is present in the `bench`
and `dev` Docker stages alongside `asv`.

### Container setup

```bash
mkdir -p ~/.cache/gents-bench      # scratch on a real disk -- NOT tmpfs
docker run --rm -v .:/usr/local/gents -v ~/.cache/gents-bench:/scratch \
    --ulimit nofile=65536:65536 -it gents-dev bash
```

Verified in the `dev` image (2026-07): Python 3.12.13, py-spy 0.4.2, asv 0.6.6,
netCDF4 1.7.4 / HDF5 1.14.6, numpy 2.5.1.

Why each flag:

- `-v ~/.cache/gents-bench:/scratch` — benchmark data must live on a real filesystem.
  Container overlayfs and tmpfs both measure RAM, not I/O, and on a typical Linux host
  `/tmp` *is* tmpfs. Measured on tmpfs: 84 MiB out in 0.13 s; the same code on ext4/NVMe
  reports a very different profile.
- `--ulimit nofile=65536:65536` — wide groups open every file at once and hit the
  `EMFILE` gotcha in [conventions.md](conventions.md).

py-spy attaches under Docker's **default** seccomp profile — no `--cap-add SYS_PTRACE`
and no `--security-opt seccomp=unconfined` required.

### Recording a profile

```bash
py-spy record --native -r 250 --idle -o /scratch/base.svg -- python pipeline_bench.py
py-spy record --native -r 250 --idle -f speedscope -o /scratch/base.json -- python pipeline_bench.py
```

- `--native` resolves into `_netCDF4.c` Cython frames and numpy `.so` frames — this is
  what separates "inside HDF5" from "in our Python loop". Works in the container
  unprivileged. It does not yield libhdf5 symbols (the wheel's bundled library is
  stripped); the Cython call site is the useful granularity.
- `-r 250` — the 100 Hz default yields only a few hundred samples on a sub-second run.
- `--idle` — includes threads blocked in a read/write syscall, which is most of the
  interesting time.
- `--subprocesses` — **required** when profiling through `TSCollection.execute`; see below.
- `-f speedscope` when you want to diff two profiles side by side.

### Rules for a valid measurement

1. **The work runs in a subprocess.** `TSCollection.execute` always wraps execution in
   `ProcessPoolExecutor`, even at `num_processes=1`. Profiling the parent yields a
   flamegraph of `as_completed` and nothing else — verified. Use `--subprocesses`, or
   an in-process code path.
2. **Build the case outside the profiled region.** With generation inline,
   `generate_history_file` dominates the flamegraph and the pipeline barely appears.
   Generate the case once into `/scratch`, then profile a driver that only reads it.
3. **Time durable writes.** `execute()` returns once data is in the page cache. Measured:
   486 MiB "written" in 0.51 s, with a further 0.18 s in `os.sync()` afterwards. Put an
   explicit `os.sync()` (or per-file `fsync`) inside the timed region or the number is
   a `memcpy` benchmark.
4. **Size the case so the chunked path is reached.** `write_timeseries_file` stores
   contiguously below 4 MiB. A working starting point: 24 files x 24 steps x 192x288
   x 2 float64 variables = 487 MiB in / 486 MiB out, giving a real chunking of
   `[9, 192, 288]`. Scale steps-per-file separately — that axis is what stresses the
   per-timestep read loop in `MHFDataset.get_var_vals`.
5. **One group means one worker.** `execute(optimize=True)` batches every order sharing
   a first source file into a single worker call, so a single-group case runs
   single-process regardless of `num_processes`. Build multi-group cases to measure
   parallel scaling.

### Iteration loop

1. Generate the bench case once into `/scratch` (skip if already present).
2. Record a baseline profile and a baseline metrics row.
3. Per run, record: `execute()` wall time, `os.sync()` wall time, bytes out, peak RSS.
   Report throughput as `bytes / (execute + sync)`. The flamegraph says *where*; this
   row says *whether*.
4. Change one knob at a time — chunk target size, chunk shape (time-major vs. spatial),
   read coalescing span, compression on/off.
5. Re-record and diff against the baseline.
6. **Gate on correctness before believing any win:** `pytest gents/tests/` plus a
   `gents_conform` run over a clone. Chunking changes are exactly the class of change
   that benchmarks well and corrupts output. The 4 MiB rule is implemented in *two*
   places — `write_timeseries_file` and `check_timeseries_conform` — and both must move
   together (see [conventions.md](conventions.md)).
7. Add or extend an ASV benchmark covering the improved path so it cannot regress.

**Status (as of 2026-07):** the in-process execution path and the shared bench-case
builder (`benchmarks/fixtures.py`, `build_bench_case()`) are done. `pipeline_bench.py`
does not exist yet — everything above about the container, py-spy flags, and the
measurement rules is verified; the driver is not.

| Needed | Where | Status |
|---|---|---|
| In-process execution path (skip `ProcessPoolExecutor`) | `TSCollection.execute` | done |
| Reusable bench-case builder, sized past 4 MiB | `benchmarks/fixtures.py` | done |
| Profiling driver: read case, time + sync, emit metrics row | `pipeline_bench.py` (new) | not started |
| Chunk-size constant read from one place | `timeseries.py` (currently hardcoded twice) | not started |

## Running GenTS

### CLI

```bash
run_gents <hf_head_dir> --model CESM3 --dryrun        # validate config, no writes
run_gents <hf_head_dir> -o <out_dir> --model CESM3 \
    --hfcores 128 --tscores 16                        # real run, tuned parallelism
run_gents <hf_head_dir> --model CESM3 --append \
    --exclude "*log*" --include "*h4i*.nc"            # extend model config filters
run_gents <hf_head_dir> --slice 5 \
    --compression zlib --level 4                      # 5-year files, compressed
run_gents <clone_head_dir> -o <out_dir> --model CESM3 \
    --no-data                                         # conformity: full structure, skip primary data
```

Flag semantics worth knowing: without `--append`, any `--include`/`--exclude` *replaces*
the model config's filter lists and `--slice`/`--slice_start_year` replace its slicing
batches; with `--append`, they are added on top. `--compression` requires `--level`.
`--model` is case-insensitive; omitted → `gents_example.yaml`. Output dir defaults to
the input dir (path swaps like `/hist/` → `/proc/tseries/` come from the YAML config).
`-nd/--no-data` (→ `execute(no_data=True)`) builds the full directory/file structure but
skips reading/writing primary-variable data — primaries read back as their fill value; the
fast path for conformity runs over missing-value clones (see the `--no-data` concept in
[concepts.md](concepts.md)). Don't run on HPC login nodes — this spawns many I/O-heavy processes.

### Building test-fixture clones (`gents_conform_build`)

Mirror a real (possibly multi-GB) case directory into tiny missing-value clones for
end-to-end testing — see the "missing-value clone" concept in [concepts.md](concepts.md).

```bash
gents_conform_build <case_head_dir> -o <clone_dir>          # mirror, default settings
gents_conform_build <case_head_dir> -o <clone_dir> -n 16    # 16 parallel worker processes
gents_conform_build <case_head_dir> -o <clone_dir> \
    --pattern "*.nc*" --max-copy-mib 0.5                   # tune discovery/guard
gents_conform_build <case_head_dir> -o <clone_dir> --preserve-format   # keep netCDF3 (won't shrink)
```

Flags: `-p/--pattern` (discovery glob, default `*.nc*`); `-n/--num-processes` (default 1,
used for both cloning and the `--verbose` summary's header reads);
`--include`/`--exclude` (fnmatch on absolute paths); `--max-copy-mib` (multi-dim fill
threshold, default 0.5, `0` disables); `--preserve-format` (don't upgrade netCDF3→NETCDF4);
`-v/--verbose` (settings echo + post-filter case summary, and enables logging — which is
otherwise off, unlike previously); `-d/--dryrun` (inspect only, writes nothing; implies
`--verbose`).

**Every run rebuilds every file its filters select.** There is deliberately no resume, no
skip-existing, no corruption repair and no `--overwrite`: clones are small and cheap, so
building a fresh variant beats reasoning about what an existing directory already holds.
Re-running into a populated directory is safe — `Dataset(path, "w")` clobbers valid,
corrupt and non-netCDF files alike, so a clone is never left half-updated. Files outside the
current filters are simply untouched, so a second command with different filters adds to a
directory rather than replacing it (`cmd.txt` records both).

`--dryrun` reports the summary plus `Dry run: N file(s) would be cloned.` and returns
before any filesystem write — no `cmd.txt`, no mirrored directory tree, no clones, and the
output directory is not even created.

`--verbose` adds a `log_case_summary` block reporting what the filtered case *covers* —
count of files carrying a time coordinate, unique variable count, output frequencies
(`month_1, day_1, hour_3`), and years spanned — which is what determines how much a
conformity run over the case can prove. It reads every file header (progress-barred), so it
is not free — those reads are spread over `-n` worker processes, the same pool size the
clone uses, and the progress bar counts files rather than groups. Grouping is decided from
paths alone before any file is opened, so results are folded back into their group on
completion and the printed summary is identical at any `-n`.

It deliberately does **not** build an `HFCollection`: that would drop every file lacking a
time coordinate and abort outright on a single-timestep group, both routine in a raw case
tree and both files the clone still has to copy. Instead it groups with the path-only
`sort_hf_groups` and reduces each file with `_summarize_case_file`, which mirrors
`netCDFMeta`'s time-variable lookup but *returns* empty-handed instead of raising when a
file has no usable time axis. Frequency per group is the gap between the group's two latest
times, as in `get_group_timestep_delta`; a group with one time step reports `unsorted`.

`_summarize_case_file` decodes nothing. `num2date` is monotonic for a fixed units/calendar,
so raw values sort exactly as the dates they denote and are compared directly; each file
comes back as three floats (its earliest and its two latest) plus the `(units, calendar)`
they are expressed in. The second-latest value is carried because a frequency is the gap
between *consecutive* steps: for a multi-step file that gap is inside the file, so
`max - min` would give the file's whole span instead.

**Component models within one case do not reliably share a time reference** — an ocean
stream written against a different start date than the atmosphere is real, observed
behaviour, not a hypothetical. Files are therefore pooled by `(group key, units, calendar)`,
so raw values are only ever compared against others written with the same reference, and a
single `num2date` per pool yields both the years it spans and its frequency. Pooling by the
reference also keeps mismatched calendars apart, which is load-bearing: `cftime` raises
`TypeError` rather than comparing or subtracting dates across calendars, so mixing them
would crash.
Sampling one reference for the whole case was tried and reverted — it silently reported
years off by the offset between references (e.g. `1850-2004` for a case actually spanning
`1850-1854`). `test_log_case_summary_handles_mixed_time_references` and
`..._handles_mixed_calendars` cover both. Net effect vs. decoding every step serially:
~4x at one step per file, ~9x at 5000 steps per file (both at `-n 8`); the bucketing costs
nothing measurable over the (incorrect) single-sample version.
Discovery uses `find_files` on the raw tree — unlike the pipeline, it does *not* filter to
viable history files, so clones include files the pipeline is meant to ignore. Each run
that clones files appends its own invocation to `cmd.txt` at the top of the clone dir
(`record_clone_command`), preserving how the clone was built. Tests live in
`gents/tests/test_case_builder.py`.

### Conformity checking (`gents_conform`)

Verify that GenTS handled a specific model's case correctly. Separate from the pytest
suite — see the "conformity testing" concept in [concepts.md](concepts.md) for the
distinction, and `gents/conformity/README.md` for the authoritative guide.

```bash
# 1. Clone a real case down to testable size (once, offline)
gents_conform_build /glade/derecho/scratch/me/my_case -o ./my_case_clone -n 16

# 2. Generate time series from the clone (--no-data is what makes this CI-fast)
run_gents ./my_case_clone -o ./my_case_output --model CESM3 --no-data

# 3. Check the output against the model specification
gents_conform ./my_case_output -i ./my_case_clone --model CESM3
gents_conform ./my_case_output --model CESM3 --json report.json   # record a result
gents_conform --list-models                                        # specs + versions
```

`-i/--hf_dir` is optional but enables the checks comparing output against its source —
the only ones that catch a stream that silently produced nothing; without it they report
SKIP. Exits `0` if every check passed, `1` if any failed, so it drops straight into CI.
Steps 2–3 may need `ulimit -n 65536` on cases with very wide streams (see the `EMFILE`
gotcha in [conventions.md](conventions.md)).

Editing a specification: copy the nearest existing check in `models/<model>.py`, write a
comment saying *why the model requires it* in the model's own vocabulary, and bump
`SPEC_VERSION`. Read the conformity conventions in [conventions.md](conventions.md)
first — several obvious-looking refactors are explicitly unwanted there.

### YAML model configs (`gents/configs/*.yaml`)

Required top-level keys (asserted by `cli.check_config`): `version`, `model`,
`input_hf`, `output_ts`. Under `input_hf`: `match` (discovery glob), `include`,
`exclude`, `slicing` (list of `slice_groups` kwarg dicts). Under `output_ts`:
`append_timestep_dirs`, `path_swaps` (list of `apply_path_swap` kwargs), `compression`
(list of `apply_compression` kwargs). To add support for a new model, add a YAML here,
register it in `model_config_files` in `cli.main`, include it in
`[tool.setuptools.package-data]` (already `*.yaml`), and add tests mirroring
`test_config.py`.

### Python API — canonical workflow

```python
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import enable_logging

enable_logging(verbose=True)                       # always do this when debugging

hfc = HFCollection("/scratch/case/output/", num_processes=64)
hfc = hfc.include(["*/atm/*", "*/ocn/*"]).exclude(["*/rest/*", "*tmp.nc"])
hfc = hfc.slice_groups(slice_size_years=10, start_year=None)   # None = natural start

tsc = TSCollection(hfc, "/scratch/case/timeseries/", num_processes=16)
tsc = tsc.apply_path_swap("/hist/", "/proc/tseries/")
tsc = tsc.apply_compression(level=2, alg="zlib", path_glob="*")
tsc = tsc.append_timestep_dirs()
output_paths = tsc.execute()                       # returns written file paths
```

Filter *before* `pull_metadata()`/`TSCollection` construction to avoid needless header
reads. Re-running is cheap: existing complete outputs (integrity-stamped) are skipped
unless `apply_overwrite` / `--overwrite` is set.

### Inspecting state interactively

```python
hfc.pull_metadata()               # explicit parallel header read
meta = hfc[list(hfc)[0]]          # netCDFMeta for first file
meta.get_primary_variables(); meta.get_cftimes(); meta.get_attributes()
hfc.get_groups()                  # {group_key: [paths]}
len(tsc); tsc[0]                  # order count / first order dict
from gents.utils import log_hfcollection_info, log_tscollection_info
log_hfcollection_info(hfc); log_tscollection_info(tsc)   # summary stats
```

## Release / versioning

Version comes from git tags via `setuptools-scm` (`fallback_version = "0.0.0"`);
`.github/workflows/release.yml` publishes. Never hand-edit a version string —
there isn't one.
