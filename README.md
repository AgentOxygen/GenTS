# Time Series Generation

>Cameron Cummins<br>
Computational Engineer<br>
Contact: cameron.cummins@utexas.edu<br>
Webpage: [https://www.jsg.utexas.edu/student/cameron_cummins](https://www.jsg.utexas.edu/student/cameron_cummins)<br>
Affiliation: Persad Aero-Climate Lab, The University of Texas at Austin

>Adam Phillips<br>
Mentor and Advisor<br>
Contact: asphilli@ucar.edu<br>
Webpage: [https://staff.cgd.ucar.edu/asphilli/](https://staff.cgd.ucar.edu/asphilli/)<br>
Affiliation: Climate and Global Dynamics Lab, National Center for Atmospheric Research

## *This Project is still in Development*
It works pretty well and can likely handle most cases, but some features aren't finished yet. Stay tuned for more!

## Converting from History Files to Time Series Files
Original model output produced by CESM2 is stored by timestep, with multiple geophysical variables stored in a single netCDF3 file together. These files are referred to as **history files** as each file effectively represents a snapshot of many variables at separate moments in time. This is intuitive from the model's perspective because variables are computed together, after solving many differential equations, at each step in the time series. However, this format is cumbersome for scientists because analysis is often performed over a large number of timesteps for only a few variables. A more practical format is to store all the timesteps for single variables in separate files. Datasets stored in this manner are known as **time series files**.

## Capabilities and Current Limitations
- [x] Functional core to convert history file to time series file in parallel with Dask
- [x] Automatic path parsing
- [x] Compatible with all model components of CESM
- [x] Compatible with large ensembles
- [x] Compatible with non-rectilinear grids
- [x] Automatic path resolving and group detection (detects different experiments, timesteps, and other sub-directories)
- [x] Customizable output directory structure that automatically reflects input directory structure
- [x] Adjustable time chunk size
- [x] Adjustable time range selection
- [x] Customizable variable selection
- [x] Adjustable file compression
- [x] Resumable process (such as exceeding wall time or encountering an error)
- [x] Documented API
- [x] Fully written in Python (no NCO, CDO, or other subprocess commands)
- [ ] (WIP) Verification tools for ensuring file integrity
- [ ] (WIP) Command line interface
- [ ] (WIP) Automatic cluster configuration/recommendation

## Dependencies
Ensure that you have the following packages installed. If you are using Casper, activate conda environment "npl-2024b".
- `python >= 3.11.9`
- `dask >= 2024.7.0`
- `dask_jobqueue >= 0.8.5`
- `netCDF4 >= 1.7.1`
- `numpy >= 1.26.4`


## How To Use
The process of converting history files into time series files is almost entirely I/O dependent and benefits heavily from parallelism that increases data throughput. We leverage Dask to parallelize this process: reading in history files and writing out their respective time series datasets across multiple worker processes. These functions can be run without Dask, but it will execute in serial and likely be significantly slower.

Start by cloning this repository to a directory of your choosing. This can be done by navigating to a directory and running the following command, assuming you have `git` installed:
````
git clone https://github.com/AgentOxygen/timeseries_generation.git
````
You can then either import `timeseries_generation.py` as a module in an interactive Jupyter Notebook (recommended) or in a custom Python script. ~~You may also execute it as a command from the terminal~~(not implemented yet).

For those that learn faster by doing than reading, there is a template notebook available called `template_notebook.ipynb`.

### Jupyter Notebook
To use this package in an interactive Jupyter notebook, create a notebook in the repository directory and start a Dask cluster using either a LocalCluster or your HPC's job-queue. Be sure to activate an environment with the correct dependencies (this can be done either at the start of the Jupyter Server or by selecting the appropriate kernel in the bottom left bar). Here is an example of a possible cluster on Casper using the NPL 2024b conda environment and the PBS job scheduler:
````
from timeseries_generation import ModelOutputDatabase
from dask_jobqueue import PBSCluster
from dask.distributed import Client


cluster = PBSCluster(
    cores=20,
    memory='40GB',
    processes=20,
    queue='casper',
    resource_spec='select=1:ncpus=20:mem=40GB',
    account='PROJECT CODE GOES HERE',
    walltime='02:00:00',
    local_directory="/local_scratch/"
)

cluster.scale(200)
client = Client(cluster)
````
Here, we spin up 200 workers, each on one core with 2 GB of memory (so 200 total cores) with maximum wall clocks of 2 hours (note that this is done in PBS batches of 20 cores per machine). The `local_directory` parameter allows each worker to use their respective node's NVMe storage for scratch work if needed. Note that these cores may be spread across multiple machines. This process should not use more than two gigabytes of memory and does not utilize multi-threading (one process per core is optimal). How the other properties of the cluster should be configured (number of cores, number of different machines, and wall time) is difficult to determine, ~~but benchmarks are available below to get a good idea~~ (WIP). If generation process is interrupted, it can be resumed without re-generating existing time series data. Note that the ``Client`` object is important for connecting the Jupyter instance to the Dask cluster. You can monitor the Dask dashboard using the URL indicated by the `client` object (run just `client` in a cell block to get the Jupyter GUI). Once created, you can then initialize a ``ModelOutputDatabase`` class object with the appropriate parameters. Here is an example of common parameters you may use:
````
model_database = ModelOutputDatabase(
    hf_head_dir="PATH TO HEAD DIRECTORY FOR HISTORY FILES (MULTIPLE MODEL RUNS, ONE RUN, OR INDIVIDUAL COMPONENTS)",
    ts_head_dir="PATH TO HEAD DIRECTORY FOR REFLECTIVE DIRECTORY STRUCTURE AND TIME SERIES FILES",
    dir_name_swaps={
        "hist": "proc/tseries"
    },
    file_exclusions=[".once.nc", ".pop.hv.nc", ".initial_hist."]
)
````
The ``ModelOutputDatabase`` object centralizes the API and acts as the starting point for all history-to-time-series operations. Creating the database will automatically search the input directories under `hf_head_dir` and group the history files by ensemble member, model component, timestep, and any other differences between subdirectories and naming patterns. The structures and names are arbitrary, so you can input a directory containing an ensemble of model archives or a single model component from a single run (or a random directory with history files in it, it should be able to handle it). It does not trigger any computation. `ts_head_dir` tells the class where to create a directory structure that matches the one under `hf_head_dir`. Only directories with `.nc` files will be reflected and optionally any other file names that contain the keywords specified by `file_exclusions` will be excluded. `dir_name_swaps` takes a Python dictionary with keys equal to directory names found under `hf_head_dir` that should be renamed. In the example above, all directories named `hist/` are renamed to two new directories `proc/tseries` to remain consistent with current time series generation conventions. There are many other parameters that can be specified to filter out which history files/variables are converted and how the time series files are formatted (see the section below titled *ModelOutputDatabase Parameters*). To trigger the time series generation process, call ``.run()``:
````
model_database.run()
client.shutdown()
````
It is good practice to shutdown the cluster after the process is complete. If metadata of the history files needs to be inspected before generating time series (to verify which history files are being detected), ``.build()`` can be used to obtain metadata for all history files. This process is parallelized and is lightweight, typically taking less than a minute to run on a large-enough cluster. This function is automatically called by ``.run()`` before generating the time series datasets.

You can monitor the Dask dashboard using the URL provided by the `client` object mentioned previously. This is a good way to track progress and check for errors.

### What to Expect

When you create ``ModelOutputDatabase``, it recursively searches the directory tree at `hf_head_dir`. If this tree is large, it may take some time. It is run in serial, so it won't show any information in the Dask dashboard, but you should see some print output. Calling `.run()` will first check to see if the database has been built, if not, it will automatically call `.build()` (which you can optionally call before `.run()` to inspect metadata). The `.build()` function aggregates the metadata for *all* of the history files under `hf_head_dir`. This process is parallelized using the Dask cluster and doesn't read much data, but opens a lot of files. This can take anywhere from less than a minute to 10 minutes depending on how the filesystem is organized. Once the metadata is aggregated in parallel, it is then processed in serial to for the arguments for time series generation. This step shouldn't take more than a few minutes and all progress will be communicated through print statements. After calculating the arguments, the `generateTimeSeries` is parallelized across the cluster with progress updated in the Dask dashboard. This is the final function call that actually preforms the time series generation by concatenating and merging variable data from the history files and writing them to disk. This process can take minutes to hours depending on the size of the history file output, the number of variables exported, the configuration of the Dask cluster, and traffic on the file system.

Errors may occur due to hiccups in file-reads, but they tend to be rare. These errors are not resolved in real-time, but reported to the user so that a smaller second-pass can be preformed to fix any incomplete or missing time series files. You might also notice that some tasks take longer than others or that the rate of progress sometimes slows down. This is likely due to different variable shapes/sizes and variability in disk I/O.

You may see a warning:
````
UserWarning: Sending large graph of size ....
````
It's a performance warning from Dask and doesn't mean anything is broken or that you did anything wrong. It might be the cause of some latency which I am hoping to eliminate in a future update, but just ignore it for now.

By default, existing files are not overwritten, but this can be configured as shown in *ModelOutputDatabase Parameters*. Immidiately before the program releases the handle for a time series file, an attribute is appended called `timeseries_process` as explained in *Additional Attributes*. This attribute is used to verify that a file was properly created (being the last thing added, it indicates the process completed successfully). If it does not exist in the file with the correct value, the program will assume it is broken and needs to be replaced. This is to protect against unexpected interruptions such as system crashes, I/O errors, or wall-clock limits.

## ModelOutputDatabase Parameters

The ``ModelOutputDatabase`` constructor has multiple optional parameters that can be customized to fit your use-case:
````
class ModelOutputDatabase:
    def __init__(self,
                 hf_head_dir: str,
                 ts_head_dir: str,
                 dir_name_swaps: dict = {},
                 file_exclusions: list = [],
                 dir_exclusions: list = ["rest", "logs"],
                 include_variables: list = None,
                 exclude_variables: list = None,
                 year_start: int = None,
                 year_end: int = None,
                 compression_level: int = None,
                 variable_compression_levels: dict = None) -> None:
````
 - `hf_head_dir` : Required string/Path object, path to head directory to structure with subdirectories containing history files.
 - `ts_head_dir` : Required string/Path object, path to head directory where structure reflecting `hf_head_dir` will be created and time series files will be written to.

Note that for a Python class, the `self` parameter is an internal parameter skipped when creating the class object (`hf_head_dir` is the first parameter you can pass to `ModelOutputDatabase()`). You are only *required* to provide the first two parameters, `hf_head_dir` and `ts_head_dir`, but many assumptions will be made which you can also deduce from the default values in the constructor:
1. All netCDF files under `hf_head_dir`, including all sub-directories except for "rest" and "logs", will be treated as history files.
3. An identical directory structure to that of `hf_head_dir` will be created under `ts_head_dir` with the exception of time-step sub-directories (such as "month_1" and "day_1")
4. The entire time series for all history files will be concatenated.
5. All variables containing the time dimension will be concatenated and have time series files.
6. No compression will be used (compression level 0).

These defaults can be overwritten to customize how your time series files are generated:
 - `dir_name_swaps` : Optional dictionary, dictionary for swapping out keyword directory names in the structure under `hf_head_dir` (e.g. `{"hist" : "proc/tseries"}`)
 - `file_exclusions` : Optional list/array, file names containing any of the keywords in this list will be excluded from the database.
 - `dir_exclusions` : Optional list, directory names containing any of the keywords in this list will be excluded from the database.
 - `include_variables` : Optional list, variables to include in either creating individual time series files for adding as auxiliary variables.
 - `exclude_variables` : Optional list, variables to exclude from either creating individual time series files for adding as auxiliary variables.
 - `year_start` : Optional int, starting year for time series generation, must be later than first history file timestamp to have an effect.
 - `year_end` : Optional int, ending year for time series generation, must be later than last history file timestamp to have an effect.
 - `compression_level` : Optional int, compression level to pass to netCDF4 engine when generating time series files.
 - `variable_compression_levels` : Optional dictionary, compression levels to apply to specific variables (variable name is key and the compression level is the value).

## Additional Attributes
When generating a new time series file, two new attributes are created in the global attributes dictionary:
1. ``timeseries_software_version`` - Indicates the version of this software used to produce the time series dataset. This is important for identifying if the data is subject to any bugs that may exist in previous iterations of the code.
2. ``timeseries_process`` - If the process is interrupted, some files may exist but not contain all the data or possibly be corrupted due to early termination of the file handle. This boolean attribute is created at initialization with a ``False`` value that is only set to ``True`` immediately before the file handle closes (it is the very last operation). When re-building the database after an interruption, this flag is checked to determine whether the file should be skipped or removed and re-computed.

## Benchmarks
WIP

## Why not use Xarray?
Xarray is an amazing tool that makes *most* big data processes run significantly faster with a lot less hassle (and it works seemlessly with Dask). Time series generation, however, has zero analytical computations involved and is almost entirely I/O bound. Data is simply read, concatenated, merged, and written. I (Cameron) tried many different approaches with Xarray, but they all had performance issues that aren't a problem in most other situations. For example, reading the metadata of *all* of the history files is quite useful for determining which history files go together and what variables are primary versus auxiliary. Reading metadata using xarray is pretty slow at the moment, but that may be due to the fact that it adds so much more functionality on top of standard netCDF operations (which we don't really need for this problem). **Xarray calls the netcdf4-Python engine under the hood, so we just use the engine directly instead**. There are other features that could potentially make Xarray viable in the future, such as taking advantage of chunking. However, current model output is in the netCDF-3 format, which does not support inherently chunking, and I couldn't figure out how to control which workers recieved specific file-sized chunks using the Xarray API (which is important for synchronization in parallel, otherwise resulting in lots of latency and memory usage).

