# Contributing to GenTS

Thanks for your interest in improving GenTS. This file is the short version; the
[Developer Guide](https://gents.readthedocs.io/en/latest/dev.html) (source:
[`docs/dev.rst`](docs/dev.rst)) describes the full setup, testing, and benchmarking
instructions.

By participating in this project you agree to abide by the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting a bug

Open an issue on the [issue tracker](https://github.com/AgentOxygen/GenTS/issues) and
include:

- What you ran (the `run_gents` command line, or the API calls).
- What you expected versus what happened.
- **Verbose log output.** Put this at the top of your script and attach the result:
  ```python
  from gents.utils import enable_logging
  enable_logging(verbose=True)
  ```
- Your GenTS version (`run_gents --version`), Python version, and how you installed
  (PyPI, container, or source).

A description of the model output that triggered it: which model components, the file naming
convention, and roughly how many files are in the case.

## Requesting a feature

Open an issue describing the workflow you are trying to complete. Ideally, the new feature should be discussed ahead of any PRs to reduce work and complexity.

## Development setup

Docker is recommended and matches what CI runs:

```bash
git clone https://github.com/AgentOxygen/GenTS.git
cd GenTS
docker build -t gents .
docker run --rm -v .:/usr/local/gents -t gents
```

A local environment works too. See the Developer Guide for the `venv`/`uv`/`conda`
instructions and how to build the documentation and run ASV benchmarks.

## Running the tests

The full suite must pass before a pull request is merged. CI runs it on every push, but
run it locally first to save time:

```bash
docker run --rm -v .:/usr/local/gents -t gents pytest gents/tests/
# or, in a local environment:
pytest gents/tests/
```

Unit tests live in `gents/tests/` and rely on `gents/tests/test_cases.py` to generate
sample history files.

End-to-end verification against real model cases is a separate system with its own
runner (`gents_conform`) (see [`gents/conformity/README.md`](gents/conformity/README.md)).
Do not add conformity checks to `gents/tests/`, or add unit-level checks to a model
specification.

## Expectations for a pull request

- **Every bug fix and new feature comes with a test.** A bug-fix test should fail before
  your change and then pass after it (this package follow test-driven development).
- Match the surrounding code style. Docstrings are Sphinx/reST and are rendered into
  the API documentation, so new public functions need them.
- Update the documentation when you change behavior, `docs/` for users, and
  [`RELEASE.md`](RELEASE.md) with a line describing the change.
- Keep the pull request focused on one thing. Unrelated cleanups are welcome as separate
  pull requests.

## Behavior changes

If a change alters results for existing callers, say so explicitly in the
pull request and add it to the **Behavior Changes** section of [`RELEASE.md`](RELEASE.md).

## Release process

1. Update [`RELEASE.md`](RELEASE.md) with notes for the new version.
2. Tag the release; the version comes from the git tag via `setuptools-scm`.
3. Publishing a GitHub release triggers `.github/workflows/release.yml`, which uploads
   to PyPI and pushes the DockerHub container image.

## Questions

Open an issue with the question.
