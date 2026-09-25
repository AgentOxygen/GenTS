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


def test_execute_peak_memory_stays_near_memory_limit(tmp_path):
    """Regression test for a user-reported OOM on a missing-value clone: the peak of
    everything allocated while generating time series stays within memory_limit_bytes
    plus a few write chunks. Lazily cached variables used to carry an uncounted
    mask array (+25% for float32, full for all-fill data) that pushed the cache past
    the limit."""
    import tracemalloc
    from gents.timeseries import CHUNK_TARGET_BYTES

    hf_dir = tmp_path / "hf"
    makedirs(hf_dir)
    dim_shapes = {"time": None, "bnds": 2, "lat": 1000, "lon": 1000, "lev": 1}
    for findex in range(24):
        path = str(hf_dir / f"case.cam.h0.{findex:04d}.nc")
        generate_history_file(path, [findex * 30.0 + 15], [[findex * 30.0, (findex + 1) * 30.0]],
                              num_vars=0, dim_shapes=dim_shapes)
        with GenTSDataStore(path, "a") as ds:
            # Never written, so every value reads back as the NaN fill, as in a clone.
            for name in ("VAR0", "VAR1"):
                ds.createVariable(name, "f4", ("time", "lat", "lon"), fill_value=np.nan)

    # Room for one variable (24 x 4 MB = 91.6 MiB) but not both, so VAR1 is cached lazily.
    limit = 96 * 1024**2
    ts_collection = TSCollection(HFCollection(hf_dir), tmp_path / "ts", num_processes=1)
    tracemalloc.start()
    ts_collection.execute(memory_limit_bytes=limit, show_progress=False)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert peak < limit + 3 * CHUNK_TARGET_BYTES, f"peak {peak / 1024**2:.1f} MiB"


def test_execute_tiny_memory_limit_still_writes_every_file(simple_case):
    """A memory_limit_bytes too small to cache anything still produces every file:
    open() preloads nothing and each read streams from disk instead."""
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