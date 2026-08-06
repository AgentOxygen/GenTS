from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import get_version
from gents.tests.test_cases import *
from gents.tests.test_workflow import is_monotonic
from gents.datastore import GenTSDataStore
from unittest.mock import patch, wraps
from os import listdir, remove
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import logging
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


def test_execute_memory_limit_bytes_threaded_to_MHFDataset(simple_case):
    """execute(memory_limit_bytes=...) reaches every MHFDataset it constructs, and defaults to DEFAULT_MEMORY_LIMIT_BYTES when not given."""
    from gents.mhfdataset import MHFDataset
    from gents.timeseries import DEFAULT_MEMORY_LIMIT_BYTES

    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=1)

    with patch("gents.timeseries.MHFDataset", wraps=MHFDataset) as mock_mhfdataset:
        ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
        ts_collection.execute(memory_limit_bytes=12345)
        assert mock_mhfdataset.call_count > 0
        for call in mock_mhfdataset.call_args_list:
            assert call.kwargs["memory_limit_bytes"] == 12345

    with patch("gents.timeseries.MHFDataset", wraps=MHFDataset) as mock_mhfdataset:
        ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1).apply_overwrite("*")
        ts_collection.execute()
        assert mock_mhfdataset.call_count > 0
        for call in mock_mhfdataset.call_args_list:
            assert call.kwargs["memory_limit_bytes"] == DEFAULT_MEMORY_LIMIT_BYTES


def test_execute_memory_limit_bytes_not_yet_enforced_by_preload(simple_case):
    """
    Known current limitation, pinned down so a future change doesn't silently
    alter it either way: memory_limit_bytes reaches MHFDataset correctly (see
    test_execute_memory_limit_bytes_threaded_to_MHFDataset), but MHFDataset's
    preload_var_list path -- what every order taken through TSCollection.execute()
    uses -- does not itself check the limit while preloading. The limit is only
    enforced by the on-demand __cache_variable fallback, which preload bypasses.
    An absurdly small limit should therefore NOT prevent a normal run from
    succeeding today. If this test starts failing because execute() now raises
    or drops data under a tiny limit, that's preload becoming memory-aware --
    update this test to assert the new, real enforcement instead of removing it.
    """
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=1)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
    ts_paths = ts_collection.execute(memory_limit_bytes=1)
    assert len(ts_paths) == SIMPLE_NUM_VARS


def test_pull_metadata_pool_error_raises(no_time_case):
    """pull_metadata(num_processes>1, raise_errors=True) propagates a worker exception through the pool branch."""
    input_head_dir, output_head_dir = no_time_case
    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    with pytest.raises(ValueError, match=".nc"):
        hf_collection.pull_metadata(raise_errors=True)


def test_pull_metadata_pool_error_logged(no_time_case, caplog):
    """pull_metadata(num_processes>1, raise_errors=False) logs and drops failures instead of raising."""
    caplog.set_level(logging.WARNING, logger="gents")
    input_head_dir, output_head_dir = no_time_case
    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    hf_collection.pull_metadata()

    assert len(hf_collection) == 0
    assert "Failed to load metadata" in caplog.text


def test_execute_serial_error_raises(simple_case):
    """execute(num_processes=1, raise_errors=True) propagates a worker exception in-process."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=1)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
    remove(list(hf_collection)[0])

    with pytest.raises(FileNotFoundError):
        ts_collection.execute(raise_errors=True)


def test_execute_serial_error_logged(simple_case, caplog):
    """execute(num_processes=1, raise_errors=False) logs a legible identifier and returns partial results instead of raising."""
    caplog.set_level(logging.WARNING, logger="gents")
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=1)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=1)
    remove(list(hf_collection)[0])

    ts_paths = ts_collection.execute()

    assert ts_paths == []
    assert "Failed to generate time series for" in caplog.text
    assert str(output_head_dir) in caplog.text
    assert "'hf_paths'" not in caplog.text


def test_execute_pool_error_raises(simple_case):
    """execute(num_processes>1, raise_errors=True) propagates a worker exception through the pool branch."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    remove(list(hf_collection)[0])

    with pytest.raises(FileNotFoundError):
        ts_collection.execute(raise_errors=True)


def test_execute_pool_error_logged(simple_case, caplog):
    """execute(num_processes>1, raise_errors=False) logs the failed order's output path, not the raw order dict."""
    caplog.set_level(logging.WARNING, logger="gents")
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    remove(list(hf_collection)[0])

    ts_paths = ts_collection.execute()

    assert ts_paths == []
    assert "Failed to generate time series for" in caplog.text
    assert str(output_head_dir) in caplog.text
    assert "'hf_paths'" not in caplog.text