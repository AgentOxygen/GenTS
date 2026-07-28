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
```

Flag semantics worth knowing: without `--append`, any `--include`/`--exclude` *replaces*
the model config's filter lists and `--slice`/`--slice_start_year` replace its slicing
batches; with `--append`, they are added on top. `--compression` requires `--level`.
`--model` is case-insensitive; omitted → `gents_example.yaml`. Output dir defaults to
the input dir (path swaps like `/hist/` → `/proc/tseries/` come from the YAML config).
Don't run on HPC login nodes — this spawns many I/O-heavy processes.

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
viable history files, so clones include files the pipeline is meant to ignore. Tests live
in `gents/tests/test_case_builder.py`.

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
