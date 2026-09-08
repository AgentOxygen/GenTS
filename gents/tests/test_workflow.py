from gents.utils import get_version
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.tests.test_cases import *
from gents.conformity.case_builder import clone_netcdf_with_missing
from gents.datastore import GenTSDataStore
from netCDF4 import default_fillvals
from os import listdir, makedirs, rename
from os.path import getsize
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


def test_no_data_workflow(simple_case):
    """
    execute(no_data=True) produces the full TS structure but leaves primary data
    unwritten: correct file count, shapes, populated time axis, and integrity
    stamp, with primaries reading back as their fill value rather than real data.
    """
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute(no_data=True)

    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        var_name = path.split(".")[-3]
        with GenTSDataStore(path, 'r') as ts_ds:
            # Structure is intact: time axis, secondary data, integrity stamp.
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds["time_bounds"].shape[0] == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds.getncattr("gents_version") == get_version()

            # Primary variable exists at full shape but carries no real data:
            # it reads back as its fill value (default fill here, since the
            # synthetic source has no _FillValue), not the source's values.
            var = ts_ds[var_name]
            var.set_auto_mask(False)
            assert var.shape[0] == SIMPLE_NUM_TEST_HIST_FILES
            expected_fill = getattr(var, "_FillValue", default_fillvals[var.dtype.str[1:]])
            assert np.all(np.asarray(var[:]) == expected_fill)


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


def test_missing_value_clone_workflow(tmp_path):
    """
    Time series built from missing-value clones stay tiny: GenTS leaves the
    all-fill primary unwritten instead of materialising NaN chunks, while the
    time coordinate and full shape are preserved.
    """
    real_dir = tmp_path / "real"
    clone_dir = tmp_path / "clone"
    real_dir.mkdir()
    clone_dir.mkdir()

    dims = {"time": None, "bnds": 2, "lat": 100, "lon": 100}
    for i in range(6):
        name = f"case.cam.h0.{i:04d}.nc"
        generate_history_file(str(real_dir / name), [(i + 0.5) * 30], [[i * 30, (i + 1) * 30]],
                              num_vars=1, dim_shapes=dims, var_dims=("time", "lat", "lon"),
                              var_shape=(1, 100, 100))
        clone_netcdf_with_missing(str(real_dir / name), str(clone_dir / name))

    real_ts = TSCollection(HFCollection(str(real_dir)), str(tmp_path / "ts_real")).execute()
    clone_ts = TSCollection(HFCollection(str(clone_dir)), str(tmp_path / "ts_clone")).execute()

    # The clone's TS output is a small fraction of the real one (unwritten primary).
    assert getsize(clone_ts[0]) < getsize(real_ts[0]) / 10

    with GenTSDataStore(clone_ts[0], "r") as ts_ds:
        var = ts_ds["VAR0"]
        var.set_auto_mask(False)
        assert var.shape == (6, 100, 100)                 # full shape preserved
        assert np.all(np.isnan(np.asarray(var[:])))       # reads back as missing
        assert ts_ds["time"].size == 6                    # time coordinate intact
        assert not np.any(np.isnan(np.asarray(ts_ds["time"][:])))


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


def test_multistep_large_slicing_workflow(multistep_large_case):
    """Slicing along large multi-timestep-per-file history files produce TS files correct bounds."""
    input_head_dir, output_head_dir = multistep_large_case
    hf_collection = HFCollection(input_head_dir).slice_groups(slice_size_years=1, start_year=None)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_paths = ts_collection.execute(optimize=True)
    assert len(ts_paths) == SIMPLE_NUM_VARS*int(np.ceil(MS_LARGE_NUM_TEST_HIST_FILES*MS_LARGE_NUM_TIMESTEPS / 12))


def test_trailing_slash_input_dir(structured_case):
    """A trailing '/' on the input dir must still nest output under the output dir, not glue onto its name."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(f"{input_head_dir}/"), str(output_head_dir))
    assert len(ts_collection) > 0
    for order in ts_collection:
        assert order["ts_path_template"].startswith(f"{output_head_dir}/")
