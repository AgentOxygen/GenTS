# Workflows & Recipes

> Commands and step-by-step recipes for common tasks. Commands verified against the
> Dockerfile, CI workflow, and pyproject as of August 2026.

## Environment setup

```bash
# Local editable install (inside a venv/conda env)
pip install -e ".[dev]"        # extras: test, docs, bench
# or minimal:
pip install -r requirements.txt && pip install -e .
```

## Testing

```bash
pytest gents/tests/                         # full suite (242 tests, ~25 s)
pytest gents/tests/test_workflow.py         # one file
```

CI-equivalent (what `.github/workflows/tests.yml` runs):

```bash
docker build --target test -t gents-test .
docker run --rm gents-test                          # image's own copy
docker run -v .:/usr/local/gents -t gents-test      # working tree (editable install)
```

Docker stages, all `CMD`-driven: `runtime` (`run_gents`), `test` (`pytest -v
gents/tests/`), `bench` (`asv run`), `dev` (`bash`, editable install with all extras),
`deptest-floor`/`deptest-latest` (run the tests *during build* against the lowest and
newest dependency resolutions). The bind-mount above is the standard way to test the
working tree rather than the baked-in copy.

`pytest` covers unit testing only. End-to-end verification against real model cases is a
separate system with its own runner (`gents_conform`) — see
[Conformity checking](#conformity-checking-gents_conform) below. Don't add conformity
checks to `gents/tests/`, or unit-level checks to a model specification.

Fixtures come from `gents/tests/test_cases.py` (`generate_history_file` plus ~24 pytest
fixtures: `simple_case`, `structured_case`, `scrambled_case`, `spatial_fragment_case`,
`multistep_case`, `multistep_large_case`, `mixed_timestep_case`, `no_time_bounds_case`,
`no_time_case`, `auxiliary_only_case`, `with_auxiliary_case`, `unstructured_grid_case`,
`long_case`, `large_file_for_chunking_case`, `extraneous_file_case`, the
`simple_{3hourly,6hourly,daily,monthly,yearly}_case` frequency set, ...). Each returns
`(head_hf_dir, head_ts_dir)` tmp paths. `generate_history_file` takes `dtype` and
`fill="constant"|"random"` — use `random` whenever compression is being measured. No real
model data is needed or included.

**Adding a feature/fix (project process is TDD):**
1. Write a failing test in `gents/tests/`, reusing or extending a `test_cases.py`
   fixture for input data.
2. Implement in `gents/` until the suite passes.
3. Update the reST docstrings (see the docstring style in
   [conventions.md](conventions.md)) and, if behavior/conventions changed, this
   `docs/llm/` set.

## Docs & benchmarks

```bash
# Sphinx docs with live rebuild at http://localhost:8000
docker run --rm -v .:/usr/local/gents -p 8000:8000 -it gents-dev \
    sphinx-autobuild docs docs/_build/html --host 0.0.0.0

# ASV benchmarks (benchmarks/benchmarks.py) for a tag
asv run v0.9.9^! --machine <machine-name>
asv publish && asv preview
```

ASV suites: `SimpleSuite` (100 history files through create/pull/TS-execute/MHFDataset),
`LargeGroupSuite` (one group of many multi-step files, stressing the timestep-delta loop
in `pull_metadata`), `MultistepSuite` (20 files × 1000 steps through TSCollection
create + execute — the order-construction time handling and the coalesced read path),
`ChunkedWriteSuite` (6 files of `(24, 192, 288)` float64, ~10.6 MiB per variable per
file, so `write_timeseries_file` takes the 4 MiB time-chunking branch no other suite
reaches), `GroupSortSuite` (48k synthetic *paths* through `sort_hf_groups` —
pure string work, writes no files, and guards against the grouping going quadratic in
streams-per-directory again). The file-writing suites build their cases through
`benchmarks/fixtures.build_bench_case()`, which is idempotent against a manifest file so
repeated `setup()` calls don't regenerate; the execute benchmarks apply
`apply_overwrite("*")` so persisted outputs from a previous repeat aren't skipped as
already-complete.

ASV remains the commit-to-commit ratchet, not the instrument for I/O work — use
`pipeline_bench.py` for that (below).

### Running ASV inside a container

```bash
asv run --machine gents-container HEAD^!
```

- `--machine <fixed name>` — asv defaults the machine name to the hostname, which is a
  fresh random hex string in every container, so results never accumulate or compare.
- `asv.conf.json` lists `branches: ["main", "validation"]` — a working/feature branch
  needs to be listed (or use an explicit range like `HEAD^!`) for `asv run` to discover
  its commits.

Two things that used to require workarounds are fixed in the tree, not just documented
around: `asv.conf.json` no longer pins `pythons` (asv defaults to the interpreter it is
running under, so any image works out of the box), and the `bench` Dockerfile stage sets
`ENV HOME=/usr/local/gents`, so `asv machine` no longer fails with `PermissionError` on
`/.asv-machine.json`. Without a bind mount the image's baked-in `/usr/local/gents` is
root-owned from `COPY` and still isn't writable by uid 1000 — a separate, pre-existing
issue.

## Performance profiling (py-spy + `pipeline_bench.py`)

For iterating on a hot path — file chunking, read/write streaming — ASV is the wrong
instrument: it rebuilds an environment per commit, reports min-of-N, and tells you
*whether* something changed, not *where* the time goes. `pipeline_bench.py` at the repo
root is the driver for that: it builds (or reuses) a synthetic case, runs one
`TSCollection.execute()` configuration against it, and prints a single greppable metrics
row.

`py-spy` ships in the `bench` extra, so it is present in the `bench` and `dev` stages
alongside `asv`.

### Container setup

```bash
mkdir -p ~/.cache/gents-bench      # scratch on a real disk -- NOT tmpfs
docker run --rm -v .:/usr/local/gents -v ~/.cache/gents-bench:/scratch \
    -it gents-dev bash
```

- `-v ~/.cache/gents-bench:/scratch` — benchmark data must live on a real filesystem.
  Container overlayfs and tmpfs both measure RAM, not I/O, and on a typical Linux host
  `/tmp` *is* tmpfs. Measured on tmpfs: 84 MiB out in 0.13 s; the same code on ext4/NVMe
  reports a very different profile.

py-spy attaches under Docker's **default** seccomp profile — no `--cap-add SYS_PTRACE`
and no `--security-opt seccomp=unconfined` required. (`--ulimit nofile=65536:65536` used
to be required here for wide groups; `MHFDataset` no longer holds many handles open, so
it isn't.)

### Recording a profile

```bash
python pipeline_bench.py --case-dir /scratch/case --keep-output   # metrics row only
py-spy record --native -r 250 --idle -o /scratch/base.svg -- \
    python pipeline_bench.py --case-dir /scratch/case
py-spy record --native -r 250 --idle -f speedscope -o /scratch/base.json -- \
    python pipeline_bench.py --case-dir /scratch/case
```

`pipeline_bench.py` flags worth knowing: `--n-files/--n-steps/--n-lat/--n-lon/--n-vars`
size the case; `--dtype float32` and `--fill random` make it representative;
`--num-processes 1` (the default) runs in-process, which is what py-spy needs;
`--compression`/`--complevel` and `--chunk-target-bytes` are the knobs under test;
`--keep-output` preserves the written tree. Output line is prefixed `RESULT` so it
survives anything else on stdout.

py-spy flag notes:

- `--native` resolves into `_netCDF4.c` Cython frames and numpy `.so` frames — this is
  what separates "inside HDF5" from "in our Python loop". It does not yield libhdf5
  symbols (the wheel's bundled library is stripped); the Cython call site is the useful
  granularity.
- `-r 250` — the 100 Hz default yields only a few hundred samples on a sub-second run.
- `--idle` — includes threads blocked in a read/write syscall, which is most of the
  interesting time.
- `-f speedscope` when you want to diff two profiles side by side.
- `--subprocesses` is only needed if you profile at `--num-processes > 1`; at 1 the
  pipeline runs in the calling process (see the parallelism model in
  [architecture.md](architecture.md)).

### Rules for a valid measurement

1. **Profile in-process.** At `num_processes > 1` both `pull_metadata` and `execute` use
   a pool, and profiling the parent yields a flamegraph of `as_completed` and nothing
   else. `pipeline_bench.py` defaults to 1 for this reason.
2. **Build the case outside the profiled region.** `build_bench_case` is idempotent, so
   generate into `/scratch` once and reuse it; otherwise `generate_history_file`
   dominates the flamegraph.
3. **Time durable writes.** `execute()` returns once data is in the page cache. Measured:
   486 MiB "written" in 0.51 s, with a further 0.18 s of `fsync` afterwards.
   `pipeline_bench.py` reports `execute_s` and `sync_s` separately and derives throughput
   from their sum.
4. **Size the case so the chunked path is reached.** `write_timeseries_file` stores
   contiguously below 4 MiB. The defaults (24 files × 24 steps × 192×288 × 2 float64
   variables = 487 MiB in / 486 MiB out) give a real chunking of `[9, 192, 288]`. Scale
   steps-per-file separately — that axis is what stresses the per-timestep read loop in
   `MHFDataset.get_var_vals`.
5. **One group means one worker.** `execute(optimize=True)` batches every order sharing a
   first source file into a single worker call, so a single-group case runs
   single-process regardless of `num_processes`. Build multi-group cases to measure
   parallel scaling.
6. **Watch memory, not just time.** `pipeline_bench.py` reports `peak_rss_mib` (max of
   self and reaped children). The `MHFDataset` cache is what moves it; `--memory-limit`
   on the CLI / `memory_limit_bytes=` on `execute()` bounds it per worker.

### Iteration loop

1. Record a baseline profile and a baseline `RESULT` row.
2. Change one knob at a time — chunk target size, chunk shape, read coalescing span,
   compression, memory limit.
3. Re-record and diff against the baseline.
4. **Gate on correctness before believing any win:** `pytest gents/tests/` plus a
   `gents_conform` run over a clone. Chunking changes are exactly the class of change
   that benchmarks well and corrupts output; the 4 MiB rule is implemented in two places
   (see [conventions.md](conventions.md)).
5. Add or extend an ASV benchmark covering the improved path so it cannot regress —
   `ChunkedWriteSuite` reaches the chunked write path and `MultistepSuite` the
   coalesced-read/order-construction path.

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
run_gents <hf_head_dir> -o <out_dir> --tscores 16 \
    --memory-limit 8                                  # cap cache at 8 GB per worker
run_gents <clone_head_dir> -o <out_dir> --model CESM3 \
    --no-data                                         # conformity: full structure, skip primary data
```

Flag semantics worth knowing: without `--append`, any `--include`/`--exclude` *replaces*
the model config's filter lists and `--slice`/`--slice_start_year` replace its slicing
batches; with `--append`, they are added on top. `--compression` requires `--level`.
`--model` is case-insensitive; omitted → `gents_example.yaml`. Output dir defaults to
the input dir (path swaps like `/hist/` → `/proc/tseries/` come from the YAML config).
`--memory-limit` is in GB and applies **per TS worker**, so the process-wide ceiling is
about `tscores ×` that; defaults to 4 GB per worker (`DEFAULT_MEMORY_LIMIT_BYTES`). `-nd/--no-data` (→ `execute(no_data=True)`)
builds the full directory/file structure but skips reading/writing primary-variable data
— primaries read back as their fill value; the fast path for conformity runs over
missing-value clones (see the `--no-data` concept in [concepts.md](concepts.md)). Don't
run on HPC login nodes — this spawns many I/O-heavy processes.

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
`-v/--verbose` (settings echo + post-filter case summary, and enables logging);
`-d/--dryrun` (inspect only, writes nothing; implies `--verbose`).

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

`--verbose` adds a `log_case_summary` block reporting what the filtered case *covers*:

```
  Files to clone                  : 1383
  Files with time coordinates     : 1383
  Unique variables                : 820
  Output frequencies              : day_1, hour_3, month_1
  Years spanned                   : 1 - 7
```

That coverage is what determines how much a conformity run over the case can prove. It
reads every file header (progress-barred), so it is not free — those reads are spread over
`-n` worker processes, the same pool size the clone uses, and the progress bar counts files
rather than groups. Grouping is decided from paths alone before any file is opened, so
results are folded back into their group on completion and the printed summary is identical
at any `-n`.

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
would crash. Sampling one reference for the whole case was tried and reverted — it silently
reported years off by the offset between references (e.g. `1850-2004` for a case actually
spanning `1850-1854`). `test_log_case_summary_handles_mixed_time_references` and
`..._handles_mixed_calendars` cover both. Net effect vs. decoding every step serially:
~4x at one step per file, ~9x at 5000 steps per file (both at `-n 8`).

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

Note both `gents_conform*` entry points only work from a source checkout or editable
install today — see the packaging gotcha in [conventions.md](conventions.md).

Editing a specification: copy the nearest existing check in `models/<model>.py`, write a
comment saying *why the model requires it* in the model's own vocabulary, and bump
`SPEC_VERSION`. Read the conformity conventions in [conventions.md](conventions.md)
first — several obvious-looking refactors are explicitly unwanted there.

### YAML model configs (`gents/configs/*.yaml`)

Required top-level keys (asserted by `cli.check_config`): `version`, `model`,
`input_hf`, `output_ts`. Under `input_hf`: `match` (discovery glob), `include`,
`exclude`, `slicing` (list of `slice_groups` kwarg dicts). Under `output_ts`:
`append_timestep_dirs` (currently ignored by the CLI — see
[conventions.md](conventions.md)), `path_swaps` (list of `apply_path_swap` kwargs),
`compression` (list of `apply_compression` kwargs). To add support for a new model, add a
YAML here, register it in `model_config_files` in `cli.main`, include it in
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
output_paths = tsc.execute(memory_limit_bytes=8 * 1024**3)     # returns written file paths
```

Filter *before* `pull_metadata()`/`TSCollection` construction to avoid needless header
reads. Re-running is cheap: existing complete outputs (integrity-stamped) are skipped
unless `apply_overwrite` / `--overwrite` is set. Pass `show_progress=False` to
`pull_metadata`/`execute` when driving GenTS from a script that owns stdout.

### Inspecting state interactively

```python
hfc.pull_metadata()               # explicit parallel header read
meta = hfc[list(hfc)[0]]          # netCDFMeta for first file
meta.get_primary_variables(); meta.get_cftimes(); meta.get_attributes()
hfc.get_groups()                  # {group_key: [paths]}
hfc.get_timestep_delta(path); hfc.get_multistep_slices(path)
len(tsc); tsc[0]                  # order count / first order dict
from gents.utils import log_hfcollection_info, log_tscollection_info
log_hfcollection_info(hfc); log_tscollection_info(tsc)   # summary stats
```

## Release / versioning

Version comes from git tags via `setuptools-scm` (`fallback_version = "0.0.0"`);
`.github/workflows/release.yml` publishes. Never hand-edit a version string —
there isn't one.
