# **Gen**erate **T**ime **S**eries Tool (GenTS)

This branch of `GenTS` features a refactored version of the package that targets a more modular API design and friendlier user experience.

Features:

- [x] Automatic directory structure and file name parsing
- [x] Automatic hsitory file grouping (h0, h1, h2, etc.)
- [x] Custom time slicing
- [x] Custom compression
- [x] Custom output directory structure
- [x] Customizeable per history file group
- [ ] Customizeable per variable
- [ ] Resumeable process, can handle interrupts
- [ ] Output validation
- [ ] Automated unit testing
- [ ] Command line interface
- [ ] Automatic Dask cluster configuration

Tasks
- [x] Build barebones functional version
- [ ] Benchmark against other tools (PyReshaper, NCO)
- [ ] Build well-documented API
- [ ] Test on CESM1/2/3 model components, compare against existing time series
- [ ] Couple with CMOR process
- [ ] Test portability on other machines

## Quick Start

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