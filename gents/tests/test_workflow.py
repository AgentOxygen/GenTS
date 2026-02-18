from gents.utils import get_version
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.tests.test_cases import *
from netCDF4 import Dataset
from os import listdir, makedirs, rename
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


def test_simple_workflow_slicing(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir).slice_groups(slice_size_years=1)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS*int(np.ceil(SIMPLE_NUM_TEST_HIST_FILES / 12))


def test_unstructured_grid_workflow(unstructured_grid_case):
    input_head_dir, output_head_dir = unstructured_grid_case

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS


def test_time_bounds_workflow(time_bounds_case):
    input_head_dir, output_head_dir = time_bounds_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert "Time_Bounds" in ts_ds.variables
            assert "Time" in ts_ds.variables
            assert "Time" in ts_ds.dimensions


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


def test_multistep_workflow(multistep_case):
    input_head_dir, output_head_dir = multistep_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert is_monotonic(ts_ds["time"][:])


def test_with_auxiliary_workflow(with_auxiliary_case):
    input_head_dir, output_head_dir = with_auxiliary_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == 1

    with Dataset(ts_paths[0], 'r') as ts_ds:
        assert is_monotonic(ts_ds["time"][:])
        for var_index in range(SIMPLE_NUM_VARS):
            assert f"VAR_AUX_{var_index}" in ts_ds.variables
            assert len(ts_ds[f"VAR_AUX_{var_index}"].shape) == 1


def test_modified_extensions_workflow(simple_case):
    input_head_dir, output_head_dir = simple_case

    for index, file_name in enumerate(listdir(input_head_dir)):
        rename(f"{input_head_dir}/{file_name}", f"{input_head_dir}/test.hf.modified_ext_simple.nc.{index}")

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        with Dataset(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES


def test_spatially_fragmented_workflow(spatial_fragment_case):
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    
    hf_collection.get_groups(check_fragmented=True)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        with Dataset(path, 'r') as ds:
            assert ds["lat"].size == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF
            assert ds["lon"].size == FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
            assert ds["time"].size == FRAGMENTED_NUM_TIMESTEPS


def test_auxiliary_only_workflow(auxiliary_only_case):
    input_head_dir, output_head_dir = auxiliary_only_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_collection.include("*").exclude("").apply_overwrite("*").execute()

    assert len(listdir(output_head_dir)) == 1
    assert "auxiliary" in listdir(output_head_dir)[0]