# **Gen**erate **T**ime **S**eries Tool (GenTS)

### Development "dev" Branch

This branch of `GenTS` features a refactored version of the package that targets a more modular API design and friendlier user experience.

Features:

- [x] Automatic directory structure and file name parsing
- [x] Automatic hsitory file grouping (h0, h1, h2, etc.)
- [x] Custom time slicing
- [x] Custom compression
- [ ] Custom output directory structure
- [ ] Customizeable per history file group
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
from dask.distributed import LocalCluster, Client
from gents import generate_time_series_from_directory

cluster = LocalCluster(n_workers=20, threads_per_worker=1, memory_limit="2GB")
client = cluster.get_client()

input_head_dir = "/projects/dgs/persad_research/CMOR_TEST_DATA/MODEL_OUTPUT/CESM3/BLT1850_1degree/lnd/"
output_head_dir = "/local1/BLT1850_1degree/lnd/"

generate_time_series_from_directory(input_head_dir, output_head_dir, dask_client=client)
```

For analyzing metadata:

```
from dask.distributed import LocalCluster, Client
from gents.read import get_groups_from_path

cluster = LocalCluster(n_workers=20, threads_per_worker=1, memory_limit="2GB")
client = cluster.get_client()

input_head_dir = "/projects/dgs/persad_research/CMOR_TEST_DATA/MODEL_OUTPUT/CESM3/BLT1850_1degree/lnd/"
output_head_dir = "/local1/BLT1850_1degree/lnd/"

group_metas = get_groups_from_path(input_head_dir, dask_client=client)
```