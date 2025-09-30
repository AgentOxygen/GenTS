# **Gen**erate **T**ime **S**eries (GenTS)

[![Available on pypi](https://img.shields.io/pypi/v/GenTS.svg)](https://pypi.org/project/GenTS/)
[![Docs](https://readthedocs.org/projects/GenTS/badge/?version=latest)](https://gents.readthedocs.io/en/latest/)
![GitHub License](https://img.shields.io/github/license/AgentOxygen/GenTS)

The GenTS (Generate Time Series) is an open-source Python Package designed to simplify the post-processing of history files into time series files. This package includes streamlined functions that require minimal input to operate and a documented API for custom workflows.

## Installation

GenTS can be installed using `pip`:

```
pip install gents['parallel']
```

Although it is reccomended to use the Dask implementation, you may wish to implement your own parallel solution for large datasets. If you then don't want to include Dask in your installation, you may omit `['parallel']` from the command (this limits GenTS to only run in serial).

To install from source, please view the [ReadTheDocs Documentation](https://gents.readthedocs.io/en/latest/).

## Example

Barebones starting example:

```
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from dask.distributed import LocalCluster, Client

cluster = LocalCluster(n_workers=30, threads_per_worker=1, memory_limit="2GB")
client = cluster.get_client()

input_head_dir = "... case directory with model output ..."
output_head_dir = "... scratch directory to output time series to ..."

hf_collection = HFCollection(input_head_dir)
hf_collection = hf_collection.include_patterns(["*/atm/*", "*/ocn/*", "*.h4.*"])
hf_collection.pull_metadata()

ts_collection = TSCollection(hf_collection.include_years(0, 5), output_head_dir)
ts_collection = ts_collection.apply_overwrite("*")
ts_collection.execute()
```

The serial equivalent (without Dask) is the same, just without the Dask `Client` or `LocalCluster`:

```
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection

input_head_dir = "... case directory with model output ..."
output_head_dir = "... scratch directory to output time series to ..."

hf_collection = HFCollection(input_head_dir)
hf_collection = hf_collection.include_patterns(["*/atm/*", "*/ocn/*", "*.h4.*"])
hf_collection.pull_metadata()

ts_collection = TSCollection(hf_collection.include_years(0, 5), output_head_dir)
ts_collection = ts_collection.apply_overwrite("*")
ts_collection.execute()
```

## Contributor/Bug Reporting Guidelines

Please report all issues to the [GitHub issue tracker](https://github.com/AgentOxygen/GenTS/issues). When submitting a bug, run `gents.utils.enable_logging(verbose=True)` at the top of your script to include all log output. This will aid in reproducing the bug and quickly developing a solution.

For development, it is recommended to use the [Docker method for testing](https://gents.readthedocs.io/en/latest/). These tests are automatically run in the GitHub workflow, but should be run before committing changes.

