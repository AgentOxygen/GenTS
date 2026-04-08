from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import get_version
from gents.tests.test_cases import *
from gents.tests.test_workflow import is_monotonic
from gents.datastore import GenTSDataStore
from unittest.mock import patch, wraps
from os import listdir
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import pytest

NUM_PARALLEL_TASKS=2

def test_parallel_simple_workflow(simple_case):
    """Parallel execution produces correct TS file count, time size, variable values, and attribute propagation."""
    input_head_dir, output_head_dir = simple_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds.getncattr("gents_version") == get_version()
            var_name = path.split(".")[-3]
            
            for index in range(ts_ds["time"].size):
                with GenTSDataStore(list(hf_collection)[index], 'r') as hf_ds:
                    assert (ts_ds[var_name][index] == hf_ds[var_name]).all()

                    for key in hf_ds.ncattrs():
                        assert ts_ds.getncattr(key) == hf_ds.getncattr(key)
                    
                    for key in hf_ds[var_name].ncattrs():
                        assert ts_ds[var_name].getncattr(key) == hf_ds[var_name].getncattr(key)


def test_parallel_scrambled_workflow(scrambled_case):
    """Parallel execution on scrambled input produces TS files with monotonically increasing time."""
    input_head_dir, output_head_dir = scrambled_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SCRAMBLED_NUM_VARS

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SCRAMBLED_NUM_TEST_HIST_FILES
            assert is_monotonic(ts_ds["time"][:])


def test_parallel_structured_workflow(structured_case):
    """Parallel execution on a multi-directory structure produces the correct total TS file count."""
    input_head_dir, output_head_dir = structured_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_paths = ts_collection.execute()
    
    assert len(ts_paths) == SCRAMBLED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS


def test_dataset_opens(simple_case):
    """optimize=True opens each source group once per batch; optimize=False opens once per primary variable."""
    input_head_dir, output_head_dir = simple_case

    hf_paths = [f"{input_head_dir}/{filename}" for filename in listdir(input_head_dir) if ".nc" in filename]
    with patch("gents.hfcollection.ProcessPoolExecutor", ThreadPoolExecutor):
        with patch("gents.meta.GenTSDataStore", wraps=GenTSDataStore) as mock_ds:
            assert mock_ds.call_count == 0
            hf_collection = HFCollection(input_head_dir, num_processes=1)
            hf_collection.pull_metadata()
            assert mock_ds.call_count == SIMPLE_NUM_TEST_HIST_FILES
    
    hf_collection = HFCollection(input_head_dir, num_processes=1)
    with patch("gents.timeseries.ProcessPoolExecutor", ThreadPoolExecutor):
        with patch("gents.mhfdataset.GenTSDataStore", wraps=GenTSDataStore) as mock_ds:
            assert mock_ds.call_count == 0
            ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
            ts_collection.execute(optimize=True) 
            assert mock_ds.call_count == SIMPLE_NUM_TEST_HIST_FILES

    with patch("gents.timeseries.ProcessPoolExecutor", ThreadPoolExecutor):
        with patch("gents.mhfdataset.GenTSDataStore", wraps=GenTSDataStore) as mock_ds:
            assert mock_ds.call_count == 0
            ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
            ts_collection.execute(optimize=False) 
            assert mock_ds.call_count == SIMPLE_NUM_TEST_HIST_FILES*SIMPLE_NUM_VARS