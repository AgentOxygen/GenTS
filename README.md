# **Gen**erate **T**ime **S**eries (GenTS)

[![Available on pypi](https://img.shields.io/pypi/v/GenTS.svg)](https://pypi.org/project/GenTS/)
[![Docs](https://readthedocs.org/projects/GenTS/badge/?version=latest)](https://gents.readthedocs.io/en/latest/)
![GitHub License](https://img.shields.io/github/license/AgentOxygen/GenTS)


The GenTS (Generate Time Series) is an open-source Python Package designed to simplify the post-processing of history files into time series files. This package includes streamlined functions that require minimal input to operate and a documented API for custom workflows.

## Features

- Robust Python API with immutable framework that simplifies integration with existing post-processing workflows and development of new ones
- Command line interface with sensible defaults for common CESM/E3SM cases and flags for edge/custom cases
- Supports parallel processing for maximizing data throughput
- Checks existing time series output for integrity to generate only the remaining files needed
- Adaptive re-chunking for compliance with CMOR (Climate Model Output Rewriter) standards
- Comprehensive testing suite for ensuring accuracy and reliability
- Docker/Apptainer/Singularity containers for bypassing complicated environment setups
- Detailed documentation for self-debugging and quickly on-boarding contributors

## Installation

GenTS can be installed in a Python environment using `pip`. This requires a conda, uv, or Python virtual environment for installing GenTS dependencies (namely `numpy`, `netCDF4`, and `cftime`).

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

> **Note**
> 
> It is bad practice to run GenTS on a login node due to the large number of IO-heavy processes it can create. Instead, submit GenTS in a batch job or run it interactively on a compute node. Here are reference pages for deploying interactive sessions on compute nodes for popular HPC centers:
> 
> [Derecho/Casper Instructions](https://ncar-hpc-docs.readthedocs.io/en/latest/pbs/#batch-jobs)
> 
> [TACC Instructions](https://docs.tacc.utexas.edu/software/idev/)
> 
> [Perlmutter Instructions](https://docs.nersc.gov/jobs/interactive/)

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
apptainer run docker://agentoxygen/gents:latest python run.py
```

## Contributor/Bug Reporting Guidelines

Please report all issues to the [GitHub issue tracker](https://github.com/AgentOxygen/GenTS/issues). When submitting a bug, run `gents.utils.enable_logging(verbose=True)` at the top of your script to include all log output. This will aid in reproducing the bug and quickly developing a solution.

For development, it is recommended to use the [Docker method for testing](https://gents.readthedocs.io/en/latest/). These tests are automatically run in the GitHub workflow, but should be run before committing changes.

## Citation

If our software package helps you with your research, please consider citing it:

 - Cummins, C. (2026). GenTS: Generate Time Series Python Package [Software]. Available from https://github.com/AgentOxygen/GenTS.

In BibTeX:

```
@Manual{         cummins2026gents,
 title         = {{GenTS}: Generate Time Series Python Package (Software)},
 author        = {Cameron Cummins},
 year          = {2026},
 url           = {https://github.com/AgentOxygen/GenTS}
}
```

## Acknowledgements

The following people made the development of GenTS possible. Thank you!

- [Nan Rosenbloom](https://staff.cgd.ucar.edu/nanr/)
- [Adam Phillips](https://github.com/phillips-ad)
- [Geeta Persad](https://www.jsg.utexas.edu/researcher/geeta_persad/)
- [Jim Edwards](https://github.com/jedwards4b)
- [Mariana Vertenstein](https://github.com/mvertens)
- [Michael Levy](https://github.com/mnlevy1981)

Portions of this work were supported by the Regional and Global Model Analysis (RGMA) component of the Earth and Environmental System Modeling Program of the U.S. Department of Energy's Office of Biological & Environmental Research (BER) under Lawrence Livermore National Lab subaward DE-AC52-07NA27344, Lawrence Berkeley National Lab subaward DE-AC02-05CH11231, and Pacific Northwest National Lab subaward  DE-AC05-76RL01830. This work was also supported by the National Science Foundation (NSF) National Center for Atmospheric Research, which is a major facility sponsored by NSF under Cooperative Agreement No. 1852977.