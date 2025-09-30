from gents.utils import get_version
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from test_cases import *
from netCDF4 import Dataset
from os import listdir, makedirs
import pytest
import numpy as np
import random


# Need to add tests for netCDF_3, netCDF4, netCDF4_classic on agg_dim for mfdataset

def is_monotonic(series):
    return (np.diff(series) > 0).all()


def test_monotonic_check():
    assert is_monotonic(np.arange(10))
    assert is_monotonic([-5, -2, 0, 5, 100])
    assert not is_monotonic([0, 0, 0])
    assert not is_monotonic([5, 2, 0, -5, -100])
    assert not is_monotonic([1, -1, 1, -1, 1])


def test_simple_workflow(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
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


def test_no_time_bounds_workflow(no_time_bounds_case):
    input_head_dir, output_head_dir = no_time_bounds_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()
    hf_collection = hf_collection.include_years(0, 99999)
    hf_collection = hf_collection.slice_groups(99999)

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


def test_scrambled_workflow(scrambled_case):
    input_head_dir, output_head_dir = scrambled_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SCRAMBLED_NUM_VARS

    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SCRAMBLED_NUM_TEST_HIST_FILES
            assert is_monotonic(ts_ds["time"][:])


def test_structured_workflow(structured_case):
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    
    assert len(ts_paths) == SCRAMBLED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS