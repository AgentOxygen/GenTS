from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import get_version
from gents.tests.test_cases import *
from gents.tests.test_workflow import is_monotonic
from netCDF4 import Dataset
from unittest.mock import patch, wraps
from os import listdir
import pytest

NUM_PARALLEL_TASKS=2

def test_parallel_simple_workflow(simple_case):
    input_head_dir, output_head_dir = simple_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
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


def test_parallel_scrambled_workflow(scrambled_case):
    input_head_dir, output_head_dir = scrambled_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SCRAMBLED_NUM_VARS

    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SCRAMBLED_NUM_TEST_HIST_FILES
            assert is_monotonic(ts_ds["time"][:])


def test_parallel_structured_workflow(structured_case):
    input_head_dir, output_head_dir = structured_case

    hf_collection = HFCollection(input_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_collection = TSCollection(hf_collection, output_head_dir, num_processes=NUM_PARALLEL_TASKS)
    ts_paths = ts_collection.execute()
    
    assert len(ts_paths) == SCRAMBLED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS
