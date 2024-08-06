# Timeseries Generation

>Cameron Cummins<br>
Computational Engineer<br>
Contact: cameron.cummins@utexas.edu<br>
Webpage: [https://www.jsg.utexas.edu/student/cameron_cummins](https://www.jsg.utexas.edu/student/cameron_cummins)<br>
Affiliation: Persad Aero-Climate Lab, The University of Texas atstin Au

>Adam Phillips<br>
Advisor<br>
Contact: asphilli@ucar.edu<br>
Webpage: [https://staff.cgd.ucar.edu/asphilli/](https://staff.cgd.ucar.edu/asphilli/)<br>
Affiliation: Climate and Global Dynamics Lab, National Center for Atmospheric Resea

## Converting from History Files to Timeseries Files
Original model output produced by CESM2 is stored by timestep, with multiple geophysical variables stored in a single netCDF3 file together. These files are referred to as **history files** as each file effectively represents a snapshot of many variables at separate moments in time. This is intuitive from the model's perspective because variables are computed together, after solving many differential equations, at each step in the timeseries. However, this format is cumbersome for scientists because analysis is often performed over a large number of timesteps for only a few variables. A more practical format is to store all of the timesteps for single variables in separate files. Datasets stored in this manner are known as **timeseries files**.

## Capabilities and Current Limitations
- [x] Convert history file to timeseries file in parallel with Dask
- [x] Compatible with all model components of CESM
- [x] Compatible with large ensembles
- [x] Compatible with non-rectilinear grids
- [x] Automatic path resolving and group detection (detects different experiments, timesteps, and other sub-directories)
- [x] Customizable output directory structure that automatically reflects input directory structure
- [x] Adjustable time chunk size
- [ ] Adjustable time range selection
- [ ] Customizable variable selection
- [ ] Adjustable file compression
- [x] Documented API

## How To Use
The process of converting history files into timeseries files is almost entirely I/O dependent and benefits heavily from parallelism that increases data throughput. We leverage Dask to parallelize this process: reading in history files and writing out their respective timeseries datasets across multiple worker processes. These functions can be run without Dask, but it will execute in serial and likely be significantly slower.

### Jupyter Notebook
First, create a Dask cluster using either a LocalCluster or your HPC's job-queue cluster. Here is an example of a possible cluster on Casper, which uses the PBS job scheduler:
````
from dask_jobqueue import PBSCluster
from dask.distributed import Client


cluster = PBSCluster(
    cores=1,
    memory='2GB',
    processes=1,
    queue='casper',
    resource_spec='select=1:ncpus=1:mem=2GB',
    account='PROJECT_CODE_GOES_HERE',
    walltime='02:00:00',
    local_directory="/local_scratch/"
)

cluster.scale(200)
client = Client(cluster)
````
The ``Client`` object is important for connecting the Jupyter instance to the Dask cluster. Then, initialize a ``ModelOutputDatabase`` class object with the appropriate parameters:
````
model_database = ModelOutputDatabase(
    hf_head_dir="/glade/derecho/scratch/user/archive/b.e21.B1850cmip6.f09_g17.DAMIP-ssp245-vlc.001/",
    ts_head_dir="/glade/derecho/scratch/user/timeseries/b.e21.B1850cmip6.f09_g17.DAMIP-ssp245-vlc.001/",
    dir_name_swaps={
        "hist": "proc/tseries"
    },
    file_exclusions=[".once.nc", ".pop.hv.nc", ".initial_hist."]
)
````
The ``ModelOutputDatabase`` object centralizes the API and acts as the starting point for all history-to-timeseries operations. There are many parameters that can be specified to filter out which history files/variables are converted and how the timeseries files are formatted. Creating the database will automatically search the input directories and group the history files by ensemble member, model component, timestep, and any other differences between subdirectories and naming patterns. It does not trigger any computation. To trigger the conversion process, call ``.run()``:
````
model_database.run()
client.shutdown()
````
It is good practice to shutdown the cluster after the process is complete. If metadata of the history files needs to be inspected before generating timeseries (to verify which history files are being detected), ``.build()`` can be used to obtain metadata for all history files. This process is parallelized and is lightweight, typically taking less than a minute to run on a large-enough cluster. This function is automatically called by ``.run()``.

