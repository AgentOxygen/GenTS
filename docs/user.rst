User Guide
==========

Users can either interact with the command line interface (CLI) ``run_gents`` or develop custom Python workflows by importing the ``gents`` module.

Using the Command Line Interface (CLI)
--------------------------------------

To get help output:

.. code-block:: console

    run_gents --help

This should outline CLI format, available arguments, and GenTS version. Depending on the installation method used, ``run_gents`` may contain a prefix such as ``apptainer run docker://agentoxygen/gents:latest`` for running inside containers.

.. code-block:: console

    run_gents <hf_head_dir> [options]

The path to the head directory containing model output history files (``hf_head_dir``) is the only required argument. GenTS recursively searches this directory for ``.nc`` files, groups them by sub-directory and file name pattern, and generates a corresponding set of time series files. By default, no filters are applied which will likely produce errors for case output directories. To apply the default configuration for a model, specify the ``--model`` argument:

.. code-block:: console

    run_gents /scratch/my_case/raw_output/ --model CESM3 --dryrun

The ``--dryrun`` flag restricts GenTS to read-only operations for validating the configuration before generating new files. By default, the output directory is the same as the case directory (for CESM3, a ``proc/tseries/`` directory is created at the same level as ``hist/``). To specify a separate output directory, use the ``-o`` output path flag:


.. code-block:: console

    run_gents /scratch/my_case/raw_output/ -o /scratch/my_case/timeseries --model CESM3 --dryrun

Parallelization is setup by default with 64 cores for reading metadata and 8 cores for reading/writing time series files. History file metadata reads scale strongly while time series writes scale weakly, so for a 128-core machine, you may want to adjust these sizes to leverage more parallelism (128 for metadata, 16 for writing):

.. code-block:: console

    run_gents /scratch/my_case/raw_output/ -o /scratch/my_case/timeseries/ --model CESM3 --dryrun --tscores 16 --hfcores 128

Remove the ``--dryrun`` argument to read the CESM3 history file structure in ``/scratch/my_case/raw_output/`` and generate time series in a similar directory structure under ``/scratch/my_case/timeseries/``. In some cases, users may want to process individual model components. To do this, simply change the history file directory (example for processing only the ``atm`` directory):

.. code-block:: console

    run_gents /scratch/my_case/raw_output/atm/ -o /scratch/my_case/timeseries/atm/ --model CESM3 --dryrun

GenTS will automatically create missing subdirectories in the output path. In some cases, the default configuration for a model may not perfectly handle the case output and read history files that should not be included. To exclude additional NetCDF files from GenTS, use the ``--exclude`` argument in combination with the ``--append`` flag:

.. code-block:: console

    run_gents /scratch/my_case/raw_output/atm/ --model CESM3 --dryrun --exclude "*log_file*" --append

The ``--append`` flag adds this filter on top of the CESM3 default configuration (as specified by ``--model CESM3``). To replace all of the exclusive filters in the configuration, simply remove the ``--append`` flag. To exclude multiple glob patterns, use the ``--exclude`` argument multiple times:

.. code-block:: console

    run_gents /scratch/my_case/raw_output/atm/ --model CESM3 --dryrun --exclude "*log_file.nc" --exclude "*static.nc"

Similarly, you can include files that are being excluded by default:

.. code-block:: console

    run_gents /scratch/my_case/raw_output/atm/ --model CESM3 --dryrun --include "*h4i*.nc"

To adjust the slice length for time series output (which defaults to 10 year slices), use the ``--slice`` argument:

Limit time series slice length to 5 years:

.. code-block:: console

    run_gents /scratch/my_case/raw_output/atm/ --model CESM3 --dryrun --slice 5



Using the Python Package
------------------------
An example code snippet is featured below:

.. code-block:: python

    from gents.hfcollection import HFCollection
    from gents.timeseries import TSCollection
    
    input_head_dir = "... case directory with model output ..."
    output_head_dir = "... scratch directory to output time series to ..."

    hf_collection = HFCollection(input_head_dir, num_processes=64)
    hf_collection = hf_collection.include(["*/atm/*", "*/ocn/*", "*.h4.*"])

    ts_collection = TSCollection(hf_collection.include_years(0, 5), output_head_dir, num_processes=32)
    ts_collection = ts_collection.apply_overwrite("*")
    ts_collection.execute()


The bulk of functionality in this package is provided by two Python classes: ``gents.hfcollection.HFCollection`` and ``gents.timeseries.TSCollection``. These classes centralize the organization of history files and provide an interface for customizing time series output. In general, the user begins by defining a ``HFCollection`` which searches recursively through a directory structure for history files. The user can then optionally apply filters to the selection to include only specific history file types. Once the desired history files have been identified, ``HFCollection`` automatically groups them by sub-directory and file name patterns. The user then creates a ``TSCollection`` from the populated ``HFCollection`` which organizes the history file groupings into a list of executable functions that create the time series files. These functions run independently of each other in an embarrassingly parallel scheme using the Python Standard Library ``ProcessPoolExecutor``. They may also be ported to third-party distributed computing libraries such as `Dask <https://docs.dask.org/en/stable/>`_ .

Creating the ``HFCollection``
-----------------------------

The ``HFCollection`` class provides an intuitive interface for the user to interactively filter for target history files by mapping paths to metadata. To get started, create a ``HFCollection`` object by pointing it to the head directory of your history file collection:

.. code-block:: python

    from gents.hfcollection import HFCollection
    hf_collection = HFCollection(hf_dir="my/file/system/scratch/GCM_run/output/history_files/")

``hf_collection`` now contains an internal dictionary that maps history files to metadata stored in the ``gents.meta.netCDFMeta`` class. For example, to print all history files by path and obtain the first entry's metadata:

.. code-block:: python

    print(list(hf_collection))
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

The ``gents.meta.netCDFMeta`` stores useful metadata information that can be quickly obtained by reading the netCDF headers. When initialized, ``HFCollection`` does not pull the metadata and leaves the internal dictionary values empty (the keys effectively act as pointers to files from which metadata will eventually be pulled). This allows the user to apply filters purely based on path characteristics before reading every history file in the collection, thereby reducing the total number of header reads. The above code block assumes the user wants all of the history files under the head directory. If the user was only interested in history files with ``.h1.`` in the path, the following code would be optimal:

.. code-block:: python

    hf_collection = hf_collection.include(["*.h1.*"])
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

Note that ``HFCollection.include`` is called before the metadata is pulled. This allows GenTS to filter out history files that do not include the specified patterns and avoid unnecessary header reads. Similarly, we can exclude patterns using ``HFCollection.exclude`` too:

.. code-block:: python

    hf_collection = hf_collection.exclude(glob=["*.once.*", "*/rof/*"])
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

Note that the user can specify multiple entries as glob patterns which can filter directories too (the glob pattern is applied to the absolute path string). Both ``HFCollection.include`` and ``HFCollection.exclude`` should be executed before pulling metadata for optimal performance. Although header reads are lightweight, thousands of files can start to add up. This can be done in serial (as above), but it is recommended to specify multiple cores when initializing ``HFCollection`` to parallelize the process. Since gathering metadata is lightweight and read-only, the throughput generally scales strongly with the number of cores:

.. code-block:: python

    from gents.hfcollection import HFCollection
    hf_collection = HFCollection(hf_dir="my/file/system/scratch/GCM_run/output/history_files/", num_processes=64)
    hf_collection.pull_metadata() # distributed across 64 cores

These functions also return copies of the ``HFCollection`` that allow the user to create multiple objects for better organization:

.. code-block:: python

    hf_atm_only = hf_collection.include(glob=["*/atm/*"])
    hf_ocn_only = hf_collection.include(glob=["*/ocn/*"])
    hf_lnd_only = hf_collection.include(glob=["*/lnd/*"])

Note that pulling metadata for ``hf_atm_only`` in this case does not pull metadata for the other two collections. However, if metadata was pulled for ``hf_collection``, all three sub-collections would inherit those metadata objects (and thus would not need to pull again).

A common step may be to filter by a date-time string in the file name:

.. code-block:: python

    hf_2010_2019 = hf_collection.include(glob=["*20100101-20191231.nc"])

This may work in most cases, but file names are not always reliable and may be difficult to apply across multiple model components. A more robust way of filtering is to operate over the time bounds provided in the metadata. This requires a metadata pull before running, so there is a performance hit for large datasets, but for smaller datasets the decrease is negligible:

.. code-block:: python

    hf_2010_2019 = hf_collection.include_years(2010, 2019)

Additionally, the user may combine this filter with an inclusive filter by using the ``glob`` argument:

.. code-block:: python

    hf_atm_2010_2019 = hf_collection.include_years(2010, 2019, glob=["*/atm/*"])

Note that the glob patterns are applied after pulling metadata, so this argument is designed for convenience rather than performance (``HFCollection.include`` is preferred). ``HFCollection.include_years`` will automatically pull metadata, if it has not already been done by the user.

Creating the ``TSCollection``
-----------------------------

Once an ``HFCollection`` has been created and configured, a ``TSCollection`` may be derived from it to map out and execute the post-processing. ``TSCollection`` only requires a valid ``HFCollection`` object and a head directory to eventually output time series datasets to:

.. code-block:: python

    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=16)

Metadata for ``hf_collection`` will automatically be pulled if not done so already. Note that the ``num_processes`` argument allows the user to parallelize time series generation across multiple cores. This is an I/O heavy process due to fully reading and writing netCDF files, so there is a limit to how strongly it scales with the number of cores allocated (scaling depends on the file system and networking). In general, scaling is much weaker than the metadata reads with ``HFCollection``. Similar to ``HFCollection``, inclusive and exclusive operations may be applied over the history file paths, but ``TSCollection`` adds variable-level filtering to singular path globs (whereas ``HFCollection`` didn't allow for per-variable filtering but could handle multiple path globs):

.. code-block:: python

    ts_tmax_only = ts_collection.include(path_glob="*", var_glob="TMAX")
    ts_prec_only = ts_collection.include(path_glob="*", var_glob="PREC*")
    ts_h1_prec_only = ts_collection.include(path_glob="*.h1.*", var_glob="PREC*")

Note that the last inclusive filter only includes history files with a path that contains ".h1." and only derives time series for variables that start with "PREC". You can also exclude time series in the same manner:

.. code-block:: python

    ts_without_h4_hurs = ts_collection.exclude(path_glob="*.h4.*", var_glob="HURS")

Just like with ``HFCollection``, both ``TSCollection.include`` and ``TSCollection.exclude`` operations return copies, allowing for advanced filtering:

.. code-block:: python

    ts_h2_temps_only = ts_collection.include(path_glob="*.h2.*", var_glob="T*")
    ts_h2_temps_no_pop = ts_h2_temps_only.exclude(path_glob="*.pop.*", var_glob="*")

Once filtered, custom arguments can be applied to all time series or just a subset. Currently supported arguments include whether to overwrite existing time series, compression level, and compression algorithm. These arguments are passed to the `netCDF4 Python API <https://unidata.github.io/netcdf4-python/>`_. The arguments can be applied using glob patterns for both paths and variable names:

.. code-block:: python

    ts_collection = ts_collection.add_args("*", "*", overwrite=True)
    ts_collection = ts_collection.apply_compression(alg="zlib", level=5, path_glob="*/atm/*", var_glob="*")
    ts_collection = ts_collection.add_args("*", "*HD*", alg="zlib", level=2)

The first line sets all time series output to overwrite existing files. The second line applies level 5 compression using the "zlib" algorithm only to time series output derived from history files that contain "/atm/" in their path. The third line applies level 2 compression to all time series output with primary variables that contain the characters "HD". Note that line 3 overrides any possible overlap with line 2.

By default, the output path templates ("templates" are incomplete path strings where only the file prefix is provided so that date time and variable name can be assigned during generation) used for writing the time series netCDF files mirror the directory structure of the given ``HFCollection``. To modify the path template, the user may replace substrings. For example, to replace the "/hist/" subdirectory with "/tseries/":

.. code-block:: python

    ts_collection = ts_collection.apply_path_swap(string_match="/hist", string_swap="/tseries/")

Note that swaps are made using the built-in ``replace`` string function, so matches can be made to any part of the path string and should not use glob or re patterns.

``TSCollection`` stores all time series as dictionaries in a list. Each dictionary contains arguments that can be passed to ``gents.timeseries.generate_time_series`` to generate a time series file. 

.. code-block:: python

    print(list(ts_collection))

The above code will print the list of time series dictionaries. By default, ``TSCollection`` parses this list of arguments into a ``ProcessPoolExecutor`` if ``num_processes > 1``. This allows the user to simply execute all time series generation functions:

.. code-block:: python

    ts_collection.execute()

Custom Dask Workflows with ``TSCollection``
-------------------------------------------

The list-type interface of ``TSCollection`` allows the user to directly modify the inputs to ``gents.timeseries.generate_time_series`` and build custom workflows if necessary. For example, if using Dask:

.. code-block:: python

    from dask import delayed
    from dask.distributed import LocalCluster, Client
    from gents.timeseries import generate_time_series

    cluster = LocalCluster(n_workers=30, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()

    delayed_orders = []
    for args in ts_collection:
        delayed_orders.append(delayed(generate_time_series)(**args))
    
    client.compute(delayed_orders, sync=True)