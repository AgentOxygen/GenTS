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
    hf_collection.include_patterns(["*/lnd/*"])
    hf_collection.include_years(0, 20)
    
    paths = generate_ts_from_hfcollection(hf_collection, output_head_dir, overwrite=True, dask_client=client)
        
The `HFCollection` class is utilized to read in and group history files into collections that are used to generate time series files. Given a head directory, all netCDF files are discovered recursively, after which various filters and group functions can be applied. The full list of filters and groups is featured below.

Filter by Pattern
-----------------
History files can be filtered by matching a glob pattern to their paths. This can be either inclusive filtering or exclusive filtering. Below, we filter the history file collection to only include history files whose paths include the substring "/lnd/":

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    hf_collection.include_patterns(["*/lnd/*"])

Conversely, we can exclude all history files that contain the substring:

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    hf_collection.exclude_patterns(["*/lnd/*"])

Filter by Years
---------------
History files can be filtered by checking their timestamp falls within in range of years. For example, we can filter the collection to only include history files with timestamps that fall between the years 1960 and 1990 (both are inclusive).

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    hf_collection.include_years(1960, 1990)

Forming Groups from a Collection
--------------------------------
The HFCollection can automatically group the history files using differences in the file paths. By default, this comparison excludes the last substring separated by a "." delimiter, which commonly defines the date substring. Groups are then formed using differences in the directories (if history files exist in separate directories, such as component directories, they will form separate groups), and differences in file names excluding the aforementioned date substring (such as timestep or namelist).

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

These groups are preserved in the HFCollection class, but can modified using slicing operations (described below).

Slice Groups by Years
---------------------
Groups can be sliced into chunks by year, yeilding shorter time series files of the specified length:

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.slice_groups(self, slice_size_years=20)

Different slicing configurations can be applied to subsets of the collection using glob patterns that are compared against the file paths.

.. code-block:: python

    hf_collection = HFCollection(input_head_dir)
    atm_hf_groups = hf_collection.slice_groups(self, slice_size_years=20, pattern="*/atm/*")
    lnd_hf_groups = hf_collection.slice_groups(self, slice_size_years=50, pattern="*/lnd/*")
    h4_hf_groups = hf_collection.slice_groups(self, slice_size_years=10, pattern="*.h4.*")