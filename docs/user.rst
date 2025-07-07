User Guide
==========

The GenTS (Generate Time Series) is an open-source Python Package designed to simplify the post-processing of history files into time series files. This package includes streamlined functions that require minimal input to operate and documented API for custom workflows. An example code snippet is featured below:

.. code-block:: python

    from gents.hfcollection import HFCollection
    from gents.gents import generate_ts_from_hfcollection
    from dask.distributed import LocalCluster
    from dask.distributed import Client
    
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


The bulk of functionality in this package is provided by two Python classes: ``gents.hfcollection.HFCollection`` and ``gents.timeseries.TSCollection``. These classes centralize the organization of history files and provides an interface for customizing time series output through sequences of operations. In general, the user begins by spinning up a Dask cluster and then defining a ``HFCollection`` which searches recursivelhttps://cfconventions.org/y through a directory structure for history files. The user can then optionally apply filters to the selection to include only specific history file types. Once the desired history files have been identified, the class object automatically groups them by sub-directory and file name patterns. The user then creates a ``TSCollection`` from the populated ``HFCollection`` which organizes the history file groupings into a list of Dask delayed function calls that can be sent to a cluster for distributed computing.

Creating a Dask Cluster
-----------------------

GenTS utilizes the `Dask <https://docs.dask.org/en/stable/>`_ Python library for distributed computing. By utilizing multiple cores across multiple nodes, Dask allows GenTS to achieve significantly higher throughput than purely serial post-processing. Creating a cluster on a local machine is relatively simple. For example, here we create a cluster of 30 workers, each with 2GB of memory, on a single machine:

.. code-block:: python

    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(n_workers=30, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()

"Workers" are Dask processes that will recieve tasks to then compute with their allocated resources. Workers can have multiple threads, but since GenTS is mostly I/O, multithreading does not offer much performance uplift. The memory overhead is dependent on the grid size of the datasets, since GenTS chunks over time steps by default. The number of workers for a ``LocalCluster`` should not exceed the number of cores available on the machine. It should be noted this process is I/O bottlenecked, meaning the GenTS is more likely to encounter data-transfer limits than CPU limits. Depending on the machine and its connectivity to the file system, allocating additional workers/cores at a certain point may not improve performance (or could even slow it down). Some testing is required, but some amount of parallelism is likely going to be signficantly faster than pure serial processing.

Once the I/O bottleneck has been identified on a single machine, Dask can interface with job schedulers to leverage multiple nodes allowing it to scale with even larger datasets.

Creating the ``HFCollection``
-----------------------------

The ``HFCollection`` class provides an intuitive interface for the user to interactively filter for target history files by mapping paths to metadata. To get started, create a ``HFCollection`` object by pointing it to the head directory of your history file collection:

.. code-block:: python

    from gents.hfcollection import HFCollection
    hf_collection = HFCollection("my/file/system/scratch/GCM_run/output/history_files/")

``hf_collection`` now contains an internal dictionary that maps history files to metadata stored in the ``gents.meta.netCDFMeta`` class. For example, to print all history files by path and obtain the first entry's metadata:

.. code-block:: python

    print(list(hf_collection))
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

The ``gents.meta.netCDFMeta`` stores useful metadata information that can be quickly obtained by reading the netCDF headers. When initialized, ``HFCollection`` does not pull the metadata and leaves the internal dictionary values empty (the keys effectively act as pointers to files from which metadata will eventually be pulled). This allows the user to apply filters purely based on path characteristics before reading every history file in the collection, thereby reducing the total number of header reads. The above code block assumes the user wants all of the history files under the head directory. If the user was only interested in history files with ``.h1.`` in the path, the following code would be optimal:

.. code-block:: python

    hf_collection = hf_collection.include_patterns(["*.h1.*"])
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

Note that ``HFCollection.include_patterns`` is called before the metadata is pulled. This allows GenTS to filter out history files that do not include the specified patterns and avoid unnecessary header reads. Although header reads are lightweight (~2-10 ms each), with thousands of files they can start to add up and this process must be repeated (at the moment) each time the Python kernel is restarted. This is also just an information-gathering stage, so no actual work is being done to post-process the data (just reading, no writing). This process can be done in serial, but it is reccomended to pull metadata after creating a Dask cluster to save time.

Similarly, we can exclude patterns using ``HFCollection.exclude_patterns`` too:

.. code-block:: python

    hf_collection = hf_collection.exclude_patterns(["*.once.*", "*/rof/*"])
    first_entry_path = list(hf_collection)[0]
    hf_collection.pull_metadata()
    first_entry_meta = hf_collection[first_entry_path]

Note that the user can specify multiple entries as glob patterns which can filter directories too (the glob pattern is applied to the absolute path string). Both ``HFCollection.include_patterns`` and ``HFCollection.exclude_patterns`` should be executed before pulling metadata for optimal performance. These functions also return copies of the ``HFCollection`` that allow the user to create multiple objects for better organization:

.. code-block:: python

    hf_atm_only = hf_collection.include_patterns(["*/atm/*"])
    hf_ocn_only = hf_collection.include_patterns(["*/ocn/*"])
    hf_lnd_only = hf_collection.include_patterns(["*/lnd/*"])

Note that pulling metadata for ``hf_atm_only`` in this case does not pull metadata for the other two collections. However, if metadata was pulled for ``hf_collection``, all three sub-collections would inherit those metadata objects (and thus would not need to pull again).

A common step may be to filter by a date-time string in the file name:

.. code-block:: python

    hf_2010_2019 = hf_collection.include_patterns(["*20100101-20191231.nc"])

This may work in most cases, but file names are not always reliable and may be difficult to apply across multiple model components. A more robust way of filtering is to operate over the time bounds provided in the metadata. This requires a metadata pull before running, so there is a performance hit for large datasets, but for smaller datasets the decrease is negligible:

.. code-block:: python

    hf_2010_2019 = hf_collection.include_years(2010, 2019)

Additionally, the user may combine an inclusive filter by using the ``glob_patterns`` argument:

.. code-block:: python

    hf_atm_2010_2019 = hf_collection.include_years(2010, 2019, glob_patterns=["*/atm/*"])

Note that the glob patterns are applied after pulling metadata, so this function is designed for convenience rather than performance. ``HFCollection.include_years`` will automatically pull metadata if it has not already been done so by the user.

Creating the ``TSCollection``
-----------------------------

Once an ``HFCollection`` has been created and configured, a ``TSCollection`` may be derived from it to map out and execute the post-processing. ``TSColleciton`` only requires a valid ``HFCollection`` object and a head directory to eventually output time series datasets to:

.. code-block:: python

    ts_collection = TSCollection(hf_collection, output_head_dir)

Metadata for ``hf_collection`` will automatically  be pulled if not done so already. Similar to ``HFCollection``, inclusive and exclusive operations may be applied over the history file paths, but ``TSCollection`` adds variable-level filtering to singular path globs (whereas ``HFCollection`` didn't allow for per-variable filtering but could handle multiple path globs):

.. code-block:: python

    ts_tmax_only = ts_collection.include("*", "TMAX")
    ts_prec_only = ts_collection.include("*", "PREC*")
    ts_h1_prec_only = ts_collection.include("*.h1.*", "PREC*")

Note that the last inclusive filter only includes history files with a path that contains ".h1." and only derives time series for variables that start with "PREC". You can also exclude time series in the same manner:

.. code-block:: python

    ts_without_h4_hurs = ts_collection.exclude("*.h4.*", "HURS")

Just like with ``HFCollection``, both ``TSCollection.include`` and ``TSCollection.exclude`` operations return copies, allowing for advanced filtering:

.. code-block:: python

    ts_h2_temps_only = ts_collection.include("*.h2.*", "T*")
    ts_h2_temps_no_pop = ts_h2_only.exclude("*.pop.*", "*")

Once filtered, custom arguments can be applied to all time series or just a subset. Currently supported arguments include whether to overwrite existing time series, compression level, and compression algorithm. These arguments are passed to the ``netCDF4 Python API <https://unidata.github.io/netcdf4-python/>``_. The arguments can be applied using glob patterns for both paths and variable names:

.. code-block:: python

    ts_collection.add_args("*", "*", overwrite=True)
    ts_collection.add_args("*/atm/*", "*", alg="zlib", level=5)
    ts_collection.add_args("*", "*HD*", alg="zlib", level=2)

Note that add arguments modifies the existing ts_collection and does not return a copy. The first line sets all time series output to overwrite existing files. The second line applies level 5 compression using the "zlib" algorithm only to time series output derived from history files that contain "/atm/" in their path. The third line applies level 2 compression to all time series output with primary variables that contain the characters "HD". Note that line 3 overrides any possible overlap with line 2.

By default, the output path templates ("templates" are incompate path strings where only the file prefix is provided so that date time and variable name can be assigned during generation) used for writing the time series netCDF files mirror the directory structure of the given ``HFCollection``. To modify the path template, the user may replace substrings. For example, to replace the "/hist/" subdirectory with "/tseries/":

.. code-block:: python

    ts_collection.apply_path_swap("/hist", "/tseries/")

Note that swaps are made using the built-in ``replace`` string function, so matches can be made to any part of the path string and should not use glob or re patterns.

``TSCollection`` stores all time series as dictionaries in a list. Each dictionary contains contains arguments that can be passed to ``gents.timeseries.generate_time_series`` to generate a time series file. 

.. code-block:: python

    print(list(ts_collection))

The above code will print the list of time series dictionaries. By default, ``TSCollection`` compiles this list of arguments into a list of Dask delayed functions which can be executed across a Dask cluster. This allows the user to simply execute all time series generation functions in parallel:

.. code-block:: python

    ts_collection.execute()

The list-type interface of ``TSCollection`` allows the user to directly modify the inputs to ``gents.timeseries.generate_time_series`` and build custom Dask workflows if necessary.
