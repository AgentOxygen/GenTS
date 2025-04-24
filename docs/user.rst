User Guide
==========

User guide will go here with example codes and procedures for NCAR CESM.

.. code-block:: console

    from dask.distributed import LocalCluster, Client
    from gents import generate_time_series_from_directory
    
    cluster = LocalCluster(n_workers=20, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()
    
    input_head_dir = "/projects/dgs/persad_research/CMOR_TEST_DATA/MODEL_OUTPUT/CESM3/BLT1850_1degree/lnd/"
    output_head_dir = "/local1/BLT1850_1degree/lnd/"
    
    generate_time_series_from_directory(input_head_dir, output_head_dir, dask_client=client)
    
