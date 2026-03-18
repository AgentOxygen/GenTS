# **Gen**erate **T**ime **S**eries (GenTS)

[![Available on pypi](https://img.shields.io/pypi/v/GenTS.svg)](https://pypi.org/project/GenTS/)
[![Docs](https://readthedocs.org/projects/GenTS/badge/?version=latest)](https://gents.readthedocs.io/en/latest/)
![GitHub License](https://img.shields.io/github/license/AgentOxygen/GenTS)


The GenTS (Generate Time Series) is an open-source Python Package designed to simplify the post-processing of history files into time series files. This package includes streamlined functions that require minimal input to operate and a documented API for custom workflows.

## Installation

GenTS can be installed in a Python environment using `pip`. This requires either a Conda or Python virtual environment for installing GenTS depedencies (namely `numpy`, `netCDF4`, and `cftime`).

For maximum portability and to avoid environment issues, use the containerized version of GenTS.

### PyPI

```
pip install gents
```

To install from source, please view the [ReadTheDocs Documentation](https://gents.readthedocs.io/en/latest/).

### Container

Apptainer and Singularity container platforms are typically employed over Docker in HPC environments. Luckily, these platforms (and most others) support running directly from Docker images. The form thus varies across institutions and systems:

**For Derecho and Casper (NCAR)**:
```
module load apptainer
apptainer run --bind /glade/derecho --cleanenv docker://agentoxygen/gents:latest run_gents --help
```

**For TACC Systems**:
```
module load apptainer
apptainer run docker://agentoxygen/gents:latest run_gents --help
```

**For Perlmutter (NERSC)**:
```
shifterimg -v pull docker:agentoxygen/gents:latest
shifter --image=docker:agentoxygen/gents:latest run_gents --help
```

## Running GenTS

GenTS comes with a pre-configured CLI that can be run on most CESM model output and E3SM (atm-only) model output by calling `run_gents`. The CLI is built on a robust API which can also be configured in a Python script or Jupyter Notebook for custom cases/workflows.

### CLI

To view options for running in the command line:

```
run_gents --help
```

### API Example

Example `run.py`:

```
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection


if __name__ == "__main__":
    input_head_dir = "... case directory with model output ..."
    output_head_dir = "... scratch directory to output time series to ..."

    hf_collection = HFCollection(input_head_dir, num_processes=64)
    hf_collection = hf_collection.include(["*/atm/*", "*/ocn/*", "*.h4.*"])

    ts_collection = TSCollection(hf_collection.include_years(0, 5), output_head_dir, num_processes=32)
    ts_collection = ts_collection.apply_overwrite("*")
    ts_collection.execute()
```

Then execute the script in a Conda or Python virtual environment with `gents` installed:

```
python run.py
```

Or run from the container:

```
apptainer run docker://agentoxygen/gents:latest run.py
```

## Contributor/Bug Reporting Guidelines

Please report all issues to the [GitHub issue tracker](https://github.com/AgentOxygen/GenTS/issues). When submitting a bug, run `gents.utils.enable_logging(verbose=True)` at the top of your script to include all log output. This will aid in reproducing the bug and quickly developing a solution.

For development, it is recommended to use the [Docker method for testing](https://gents.readthedocs.io/en/latest/). These tests are automatically run in the GitHub workflow, but should be run before committing changes.

