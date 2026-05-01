from gents.utils import get_version
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.tests.test_cases import *
from gents.datastore import GenTSDataStore
from os import listdir, makedirs, rename
import pytest
import numpy as np
import random


# Need to add tests for netCDF_3, netCDF4, netCDF4_classic on agg_dim for mfdataset

def is_monotonic(series):
    """Returns True if all consecutive differences in the series are strictly positive."""
    return (np.diff(series) > 0).all()


def test_monotonic_check():
    """Validates the is_monotonic() helper against increasing, flat, decreasing, and alternating sequences."""
    assert is_monotonic(np.arange(10))
    assert is_monotonic([-5, -2, 0, 5, 100])
    assert not is_monotonic([0, 0, 0])
    assert not is_monotonic([5, 2, 0, -5, -100])
    assert not is_monotonic([1, -1, 1, -1, 1])


def test_simple_workflow(simple_case):
    """End-to-end: correct TS file count, monotonic time, gents_version attribute, variable values, and attribute propagation."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        assert "*" not in path
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds["time_bounds"].shape[0] == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds.getncattr("gents_version") == get_version()
            assert is_monotonic(ts_ds["time"][:])
            var_name = path.split(".")[-3]
            
            for index in range(ts_ds["time"].size):
                time_bounds = ts_ds["time_bounds"][index]
                assert time_bounds.count() == 2
                assert time_bounds[0] <= ts_ds["time"][index] <= time_bounds[1]
                
                with GenTSDataStore(list(hf_collection)[index], 'r') as hf_ds:
                    assert (ts_ds[var_name][index] == hf_ds[var_name]).all()

                    for key in hf_ds.ncattrs():
                        assert ts_ds.getncattr(key) == hf_ds.getncattr(key)
                    
                    for key in hf_ds[var_name].ncattrs():
                        assert ts_ds[var_name].getncattr(key) == hf_ds[var_name].getncattr(key)


def test_simple_workflow_slicing(simple_case):
    """Sliced workflow produces one TS per variable per year slice with no sorting_pivot tokens in output paths."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir).slice_groups(slice_size_years=1, start_year=None)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS*int(np.ceil(SIMPLE_NUM_TEST_HIST_FILES / 12))

    for path in ts_paths:
        assert "[sorting_pivot]" not in path
        assert "*" not in path
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == ts_ds["time_bounds"].shape[0]
            assert ts_ds["time"].size == 12 or ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES % 12

            for index in range(ts_ds["time"].size):
                time_bounds = ts_ds["time_bounds"][index]
                assert time_bounds.count() == 2
                assert time_bounds[0] <= ts_ds["time"][index] <= time_bounds[1]   


def test_unstructured_grid_workflow(unstructured_grid_case):
    """Unstructured-grid history files produce the expected number of TS files."""
    input_head_dir, output_head_dir = unstructured_grid_case

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS


def test_time_bounds_workflow(time_bounds_case):
    """Non-default Time and Time_Bounds variable names are preserved in TS output."""
    input_head_dir, output_head_dir = time_bounds_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert "Time_Bounds" in ts_ds.variables
            assert "Time" in ts_ds.variables
            assert "Time" in ts_ds.dimensions


def test_no_time_bounds_workflow(no_time_bounds_case):
    """Workflow succeeds and values/attributes are correct when history files have no time_bounds variable."""
    input_head_dir, output_head_dir = no_time_bounds_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()
    hf_collection = hf_collection.include_years(0, 99999)
    hf_collection = hf_collection.slice_groups(99999)

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


def test_scrambled_workflow(scrambled_case):
    """Scrambled-order input produces TS files with monotonically increasing time."""
    input_head_dir, output_head_dir = scrambled_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SCRAMBLED_NUM_VARS

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SCRAMBLED_NUM_TEST_HIST_FILES
            assert is_monotonic(ts_ds["time"][:])


def test_structured_workflow(structured_case):
    """Multi-directory structured input produces the correct total number of TS files."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    
    assert len(ts_paths) == SCRAMBLED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS


def test_multistep_workflow(multistep_case):
    """Multi-timestep-per-file history files produce TS files with monotonically increasing time."""
    input_head_dir, output_head_dir = multistep_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    hf_collection.sort_along_time()

    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert is_monotonic(ts_ds["time"][:])


def test_with_auxiliary_workflow(with_auxiliary_case):
    """Auxiliary (1-D) variables are included in TS output alongside primary variables."""
    input_head_dir, output_head_dir = with_auxiliary_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == 1

    with GenTSDataStore(ts_paths[0], 'r') as ts_ds:
        assert is_monotonic(ts_ds["time"][:])
        for var_index in range(SIMPLE_NUM_VARS):
            assert f"VAR_AUX_{var_index}" in ts_ds.variables
            assert len(ts_ds[f"VAR_AUX_{var_index}"].shape) == 1


def test_modified_extensions_workflow(simple_case):
    """History files with .nc.N fragment extensions are handled correctly and produce complete TS output."""
    input_head_dir, output_head_dir = simple_case

    for index, file_name in enumerate(listdir(input_head_dir)):
        rename(f"{input_head_dir}/{file_name}", f"{input_head_dir}/test.hf.modified_ext_simple.nc.{index}")

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES


def test_spatially_fragmented_workflow(spatial_fragment_case):
    """Spatial tile files are assembled into TS files covering the full combined lat/lon extent."""
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    
    hf_collection.get_groups(check_fragmented=True)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ds:
            assert ds["lat"].size == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF
            assert ds["lon"].size == FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
            assert ds["time"].size == FRAGMENTED_NUM_TIMESTEPS


def test_auxiliary_only_workflow(auxiliary_only_case):
    """History files with no primary variables produce a single auxiliary TS file."""
    input_head_dir, output_head_dir = auxiliary_only_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_collection.include("*").exclude("").apply_overwrite("*").execute()

    assert len(listdir(output_head_dir)) == 1
    assert "auxiliary" in listdir(output_head_dir)[0]

def test_include_years_workflow(long_case):
    """include_years() filters correctly and the resulting TS filename contains the expected date range string."""
    input_head_dir, output_head_dir = long_case
    hf_collection = HFCollection(input_head_dir).include_years(1850, 1852)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    for path in ts_paths:
        assert path.split(".")[-2] == "185001-185212"