# **Gen**erate **T**ime **S**eries Tool (GenTS)

[![Available on pypi](https://img.shields.io/pypi/v/GenTS.svg)](https://pypi.org/project/GenTS/)
[![Docs](https://readthedocs.org/projects/GenTS/badge/?version=latest)](https://gents.readthedocs.io/en/latest/)
![GitHub License](https://img.shields.io/github/license/AgentOxygen/GenTS)

The GenTS (Generate Time Series) is an open-source Python Package designed to simplify the post-processing of history files into time series files. This package includes streamlined functions that require minimal input to operate and a documented API for custom workflows.

## Installation

GenTS can be installed using `pip`:

```
pip install gents
```

To install from source, please view the [ReadTheDocs Documentation](https://gents.readthedocs.io/en/latest/).

## Example

Barebones starting example:

```
from gents.hfcollection import HFCollection
from gents.gents import generate_ts_from_hfcollection
from dask.distributed import LocalCluster
from dask.distributed import Client

cluster = LocalCluster(n_workers=30, threads_per_worker=1, memory_limit="2GB")
client = cluster.get_client()

input_head_dir = "... case directory with model output ..."
output_head_dir = "... scratch directory to output time series to ..."

hf_collection = HFCollection(input_head_dir)
hf_collection.include_patterns(["*/lnd/*"])
hf_collection.include_years(0, 20)

paths = generate_ts_from_hfcollection(hf_collection, output_head_dir, overwrite=True, dask_client=client)
```

## Future Planning
Features:

- [x] Automatic directory structure and file name parsing
- [x] Automatic hsitory file grouping (h0, h1, h2, etc.)
- [ ] Custom time slicing
- [x] Custom compression
- [x] Custom output directory structure
- [x] Customizeable per history file group
- [x] Customizeable per variable
- [x] Resumeable process, can handle interrupts
- [ ] Output validation
- [ ] Automated unit testing
- [ ] Command line interface
- [ ] Automatic Dask cluster configuration

Tasks
- [x] Build barebones functional version
- [ ] Benchmark against other tools (PyReshaper, NCO)
- [x] Build well-documented API
- [x] Test on CESM1/2/3 model components, compare against existing time series
- [x] Couple with CMOR process
- [x] Test portability on other machines