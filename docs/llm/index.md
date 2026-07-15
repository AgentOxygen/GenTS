# GenTS — LLM Context Index

> GenTS (Generate Time Series) is a Python package that post-processes climate model
> output from the "history file" format (one file per time step, all variables) into
> the "time series" format (one file per variable, many time steps). It targets
> CESM/E3SM-style netCDF output on HPC systems, exposes an immutable fluent Python API
> plus a `run_gents` CLI, and parallelizes with `ProcessPoolExecutor`.

This directory is the entry point for AI agents working on GenTS. Read this file
first, then load only the spoke files relevant to your task. All facts here were
verified against the source; when docs and code disagree, trust the code and fix the doc.

## How to use these docs

| Task | Read |
|---|---|
| Understand the domain (history files, time bounds, primary variables, slicing...) | [concepts.md](concepts.md) |
| Understand the pipeline, modules, and key data structures | [architecture.md](architecture.md) |
| Modify code without breaking invariants; known gotchas and stale spots | [conventions.md](conventions.md) |
| Run tests/CLI/docs/benchmarks; common dev recipes | [workflows.md](workflows.md) |

## Package identity

- **PyPI name:** `GenTS`, import name `gents`, CLI entry point `run_gents` (→ `gents.cli:main`).
- **Python:** ≥ 3.10. **Dependencies (only these):** `numpy`, `netCDF4`, `cftime`, `pyyaml`.
  Minimal dependency stack is a stated design principle — do not add dependencies casually.
- **Version:** derived from git tags via `setuptools-scm` (`gents.utils.get_version()` reads
  installed package metadata). There is no `__version__` in source.
- **Scope (from the developer guide):** GenTS *only* converts history files to time series.
  No regridding, no CMORization, no computation on data values. Reject scope creep.
- **Docs:** Sphinx under `docs/` (published to ReadTheDocs), human-oriented.
  This `docs/llm/` directory is the agent-oriented layer.

## Repository map

```
gents/                  Package source (8 modules, ~3,200 lines)
  hfcollection.py       HFCollection: discover/filter/group/slice history files
  timeseries.py         TSCollection: build + execute time-series "orders"; file writing
  meta.py               netCDFMeta: cached per-file metadata; primary/secondary classification
  mhfdataset.py         MHFDataset: virtual aggregated dataset over a file group
  datastore.py          GenTSDataStore: thin context-manager wrapper around netCDF4.Dataset
  cli.py                argparse CLI + YAML-config-driven main()
  utils.py              logging setup, ProgressBar, version, collection info loggers
  configs/              Bundled YAML model configs (gents_example.yaml, gents_cesm3.yaml)
  tests/                pytest suite; test_cases.py generates synthetic netCDF fixtures
docs/                   Sphinx docs (index/install/user/dev/api .rst)
benchmarks/             ASV performance benchmarks
Dockerfile              Multi-stage: runtime / test / bench / dev / deptest-floor / deptest-latest
.github/workflows/      tests.yml (docker --target test), dependency_checks, release
build/lib/              STALE build artifact copy of the package — never read or edit
```

## The pipeline in one paragraph

`HFCollection(hf_dir)` recursively finds `*.nc*` files → `include()`/`exclude()` glob
filters (cheap, path-only) → `pull_metadata()` reads netCDF headers in parallel into
`netCDFMeta` objects → `get_groups()` clusters files by directory + filename prefix →
`slice_groups()` partitions each group into N-year windows → `TSCollection(hfc, out_dir)`
expands groups into one "order" dict per primary variable → optional order modifiers
(`apply_compression`, `apply_path_swap`, `add_attrs`, ...) → `execute()` runs
`generate_time_series` per group in a process pool, each opening an `MHFDataset` and
writing one netCDF file per variable via `write_timeseries_file`.

## Quick commands

```bash
pip install -e ".[dev]"          # editable install with test/docs/bench extras
pytest gents/tests/              # full test suite
docker build --target test -t gents-tests . && docker run --rm gents-tests   # CI-equivalent
run_gents <hf_dir> --model CESM3 --dryrun   # CLI dry run (read-only validation)
```
