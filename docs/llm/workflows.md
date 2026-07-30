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
    --pattern "*.nc*" --max-copy-mib 0.5 --overwrite      # tune discovery/guard, rebuild
gents_conform_build <case_head_dir> -o <clone_dir> --preserve-format   # keep netCDF3 (won't shrink)
```

Flags: `-p/--pattern` (discovery glob, default `*.nc*`); `-n/--num-processes` (default 1);
`--include`/`--exclude` (fnmatch on absolute paths); `--max-copy-mib` (multi-dim fill
threshold, default 0.5, `0` disables); `--overwrite` (else existing *valid* clones are
skipped, corrupt ones rebuilt); `--preserve-format` (don't upgrade netCDF3→NETCDF4).
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
