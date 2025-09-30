from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from test_cases import *
from test_workflow import is_monotonic
from netCDF4 import Dataset
from pytest import importorskip


def test_dask_simple_workflow(simple_case):
    daskd = importorskip("dask.distributed", reason="Dask distributed not installed.")
    input_head_dir, output_head_dir = simple_case

    cluster = daskd.LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()

    hf_collection = HFCollection(input_head_dir, dask_client=client)
    ts_collection = TSCollection(hf_collection, output_head_dir, dask_client=client)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds.getncattr("gents_version") == get_version()
            var_name = path.split(".")[-3]
            
            for index in range(ts_ds["time"].size):
                with Dataset(list(hf_collection)[index], 'r') as hf_ds:
                    assert (ts_ds[var_name][index] == hf_ds[var_name]).all()

                    for key in hf_ds.ncattrs():
                        assert ts_ds.getncattr(key) == hf_ds.getncattr(key)
                    
                    for key in hf_ds[var_name].ncattrs():
                        assert ts_ds[var_name].getncattr(key) == hf_ds[var_name].getncattr(key)
    client.shutdown()


def test_dask_scrambled_workflow(scrambled_case):
    daskd = importorskip("dask.distributed", reason="Dask distributed not installed.")
    input_head_dir, output_head_dir = scrambled_case

    cluster = daskd.LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()

    hf_collection = HFCollection(input_head_dir, dask_client=client)
    ts_collection = TSCollection(hf_collection, output_head_dir, dask_client=client)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SCRAMBLED_NUM_VARS

    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SCRAMBLED_NUM_TEST_HIST_FILES
            assert is_monotonic(ts_ds["time"][:])

    client.shutdown()


def test_dask_structured_workflow(structured_case):
    daskd = importorskip("dask.distributed", reason="Dask distributed not installed.")
    input_head_dir, output_head_dir = structured_case

    cluster = daskd.LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="2GB")
    client = cluster.get_client()

    hf_collection = HFCollection(input_head_dir, dask_client=client)
    ts_collection = TSCollection(hf_collection, output_head_dir, dask_client=client)
    ts_paths = ts_collection.execute()
    
    assert len(ts_paths) == SCRAMBLED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS

    client.shutdown()
