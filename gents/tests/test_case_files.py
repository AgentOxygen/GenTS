from os import listdir
from gents.datastore import GenTSDataStore
from cftime import num2date
import numpy as np
from gents.tests.test_cases import *
from pytest import approx
from unittest.mock import patch, wraps


def test_simple_case(simple_case):
    """Validates time values, units, calendar, bounds ordering, and total date span of the simple case fixture."""
    input_head_dir, output_head_dir = simple_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    times = []
    time_bounds = []
    calendar = None
    units = None
    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert "time" in hf_ds.variables
            assert "units" in hf_ds["time"].ncattrs()
            units = hf_ds["time"].units

            assert "calendar" in hf_ds["time"].ncattrs()
            calendar = hf_ds["time"].calendar

            time = hf_ds["time"][:][0]
            bounds = hf_ds["time_bounds"][:][0]

            assert time >= 0
            assert time >= bounds[0]
            assert time <= bounds[1]
            assert bounds[0] != bounds[1]

            times.append(time)
            time_bounds.append(bounds)
    
    start_date = num2date(np.min(times), units=units, calendar=calendar)
    end_date = num2date(np.max(times), units=units, calendar=calendar)
    assert (end_date - start_date).days == (SIMPLE_NUM_TEST_HIST_FILES-1)*30
    assert start_date == num2date(0.5*30, units=units, calendar=calendar)


def test_baseline_dataset_opens(simple_case):
    """Confirms the Dataset mock correctly tracks call counts, verifying the test patching infrastructure."""
    input_head_dir, output_head_dir = simple_case

    hf_paths = [f"{input_head_dir}/{filename}" for filename in listdir(input_head_dir) if ".nc" in filename]
    with patch("gents.tests.test_case_files.GenTSDataStore", wraps=GenTSDataStore) as mock_ds:
        assert mock_ds.call_count == 0
        with GenTSDataStore(hf_paths[0], "r") as ds:
            assert mock_ds.call_count == 1
        assert mock_ds.call_count == 1

        with GenTSDataStore(hf_paths[0], "r") as ds:
            pass
        with GenTSDataStore(hf_paths[0], "r") as ds:
            pass
        assert mock_ds.call_count == 3


def test_unstructured_grid_case(unstructured_grid_case):
    """Confirms auxiliary variable size matches ncol and that primary/auxiliary variable dimensions are correct."""
    input_head_dir, output_head_dir = unstructured_grid_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES

    with GenTSDataStore(f"{input_head_dir}/{listdir(input_head_dir)[0]}", 'r') as hf_ds:
        assert hf_ds["VAR_AUX_0"].size == UNSTRUCT_GRID_NUM_NCOLS
        assert hf_ds["VAR_AUX_0"].dimensions == ("ncol",)
        assert hf_ds["VAR0"].dimensions == ("time", "ncol",)


def test_time_bounds_case(time_bounds_case):
    """Confirms both non-default ``Time`` and ``Time_Bounds`` variables are present in each file."""
    input_head_dir, output_head_dir = time_bounds_case
    assert len(listdir(input_head_dir)) == TIME_NUM_TEST_HIST_FILES
    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'Time_Bounds' in hf_ds.variables
            assert 'Time' in hf_ds.variables


def test_scrambled_case(scrambled_case):
    """Confirms the scrambled case produces the expected number of files."""
    input_head_dir, output_head_dir = scrambled_case
    assert len(listdir(input_head_dir)) == SCRAMBLED_NUM_TEST_HIST_FILES


def test_structured_case(structured_case):
    """Confirms the structured case directory tree depth and file counts at each level."""
    input_head_dir, output_head_dir = structured_case
    assert len(listdir(input_head_dir)) == STRUCTURED_NUM_DIRS
    for top_dir in listdir(input_head_dir):
        assert len(listdir(f"{input_head_dir}/{top_dir}")) == STRUCTURED_NUM_SUBDIRS
        for sub_dir in listdir(f"{input_head_dir}/{top_dir}/"):
            assert len(listdir(f"{input_head_dir}/{top_dir}/{sub_dir}")) == STRUCTURED_NUM_TEST_HIST_FILES


def test_time_bounds_missing_attrs_case(simple_case_missing_attrs):
    """Confirms time_bounds variables have no ``units`` or ``calendar`` attributes."""
    input_head_dir, output_head_dir = simple_case_missing_attrs
    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'units' not in hf_ds['time_bounds'].ncattrs()
            assert 'calendar' not in hf_ds['time_bounds'].ncattrs()


def test_no_time_case(no_time_case):
    """Confirms neither ``time`` nor ``time_bounds`` variables are present."""
    input_head_dir, output_head_dir = no_time_case
    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'time' not in hf_ds.variables
            assert 'time_bounds' not in hf_ds.variables


def test_multistep_case(multistep_case):
    """Confirms time is a 1-D variable and the expected number of files were created."""
    input_head_dir, output_head_dir = multistep_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert len(hf_ds['time'].dimensions) == 1


def test_with_auxiliary_case(with_auxiliary_case):
    """Confirms each auxiliary variable has only the ``time`` dimension."""
    input_head_dir, output_head_dir = with_auxiliary_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES

    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            for var_index in range(SIMPLE_NUM_VARS):
                assert hf_ds[f"VAR_AUX_{var_index}"].dimensions == ('time',)


def test_fragmented_case(spatial_fragment_case):
    """Confirms each tile file has the expected lat/lon size, unique coordinate values, and the correct total number of distinct spatial tiles."""
    input_head_dir, output_head_dir = spatial_fragment_case
    assert len(listdir(input_head_dir)) == FRAGMENTED_NUM_TIMESTEPS*FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LON_FILES
    
    dim_hashes = []

    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            for var_index in range(SIMPLE_NUM_VARS):
                assert hf_ds["lat"].size == FRAGMENTED_NUM_LAT_PTS_PER_HF
                assert hf_ds["lon"].size == FRAGMENTED_NUM_LON_PTS_PER_HF
                assert np.unique(hf_ds["lat"][:]).size == hf_ds["lat"].size
                assert np.unique(hf_ds["lon"][:]).size == hf_ds["lon"].size
            
            dim_hash = str([hf_ds["lat"][:], hf_ds["lon"][:]])
            if dim_hash not in dim_hashes:
                dim_hashes.append(dim_hash)
    
    assert len(dim_hashes) == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LON_FILES


def test_mixed_timestep_case(mixed_timestep_case):
    """Validates the time-bound width for each of the four timestep frequency groups (sub-hour, day, month, year)."""
    input_head_dir, output_head_dir = mixed_timestep_case
    assert len(listdir(input_head_dir)) == MIXED_TS_NUM_TEST_HIST_FILES*4

    for index in range(MIXED_TS_NUM_TEST_HIST_FILES):
        with GenTSDataStore(f"{input_head_dir}/testing.hf0.{str(index).zfill(5)}.nc", 'r') as hf_ds:
            bounds = hf_ds["time_bounds"][:][0]
            assert 0 < bounds[1] - bounds[0] < 1
        with GenTSDataStore(f"{input_head_dir}/testing.hf1.{str(index).zfill(5)}.nc", 'r') as hf_ds:
            bounds = hf_ds["time_bounds"][:][0]
            assert 1 <= bounds[1] - bounds[0] < 28
        with GenTSDataStore(f"{input_head_dir}/testing.hf2.{str(index).zfill(5)}.nc", 'r') as hf_ds:
            bounds = hf_ds["time_bounds"][:][0]
            assert 28 <=bounds[1] - bounds[0] < 365
        with GenTSDataStore(f"{input_head_dir}/testing.hf3.{str(index).zfill(5)}.nc", 'r') as hf_ds:
            bounds = hf_ds["time_bounds"][:][0]
            assert 365 <= bounds[1] - bounds[0]


def test_auxiliary_only_case(auxiliary_only_case):
    """Confirms auxiliary variables have ``time`` as their only dimension and no primary variables exist."""
    input_head_dir, output_head_dir = auxiliary_only_case

    for file_name in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            for var_index in range(SIMPLE_NUM_VARS):
                assert "time" == hf_ds[f"VAR_AUX_{var_index}"].dimensions[0]
                assert len(hf_ds[f"VAR_AUX_{var_index}"].dimensions) == 1
                assert f"VAR_{var_index}" not in hf_ds.variables


def test_simple_3hourly_case(simple_3hourly_case):
    """Confirms consecutive files are separated by exactly 3 hours in both time and time_bounds."""
    input_head_dir, output_head_dir = simple_3hourly_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    paths = listdir(input_head_dir)
    paths.sort()

    time0 = None
    time_bnds0 = None
    with GenTSDataStore(f"{input_head_dir}/{paths[0]}", 'r') as hf_ds:
        time0 = hf_ds["time"][:][0]
        time_bnds0 = hf_ds["time_bounds"][:][0][0]
    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert hf_ds["time"][:][0] - time0 == approx(3/24, 0.001)
        assert hf_ds["time_bounds"][:][0][0] - time_bnds0 == approx(3/24, 0.001)


def test_simple_6hourly_case(simple_6hourly_case):
    """Confirms consecutive files are separated by exactly 6 hours in both time and time_bounds."""
    input_head_dir, output_head_dir = simple_6hourly_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    paths = listdir(input_head_dir)
    paths.sort()

    time0 = None
    time_bnds0 = None
    with GenTSDataStore(f"{input_head_dir}/{paths[0]}", 'r') as hf_ds:
        time0 = hf_ds["time"][:][0]
        time_bnds0 = hf_ds["time_bounds"][:][0][0]
    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert hf_ds["time"][:][0] - time0 == approx(6/24, 0.001)
        assert hf_ds["time_bounds"][:][0][0] - time_bnds0 == approx(6/24, 0.001)


def test_simple_daily_case(simple_daily_case):
    """Confirms consecutive files are separated by exactly 1 day in both time and time_bounds."""
    input_head_dir, output_head_dir = simple_daily_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    paths = listdir(input_head_dir)
    paths.sort()

    time0 = None
    time_bnds0 = None
    with GenTSDataStore(f"{input_head_dir}/{paths[0]}", 'r') as hf_ds:
        time0 = hf_ds["time"][:][0]
        time_bnds0 = hf_ds["time_bounds"][:][0][0]
    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert hf_ds["time"][:][0] - time0 == approx(1, 0.001)
        assert hf_ds["time_bounds"][:][0][0] - time_bnds0 == approx(1, 0.001)


def test_simple_monthly_case(simple_monthly_case):
    """Confirms consecutive files are separated by exactly 30 days in both time and time_bounds."""
    input_head_dir, output_head_dir = simple_monthly_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    paths = listdir(input_head_dir)
    paths.sort()

    time0 = None
    time_bnds0 = None
    with GenTSDataStore(f"{input_head_dir}/{paths[0]}", 'r') as hf_ds:
        time0 = hf_ds["time"][:][0]
        time_bnds0 = hf_ds["time_bounds"][:][0][0]
    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert hf_ds["time"][:][0] - time0 == approx(30, 0.001)
        assert hf_ds["time_bounds"][:][0][0] - time_bnds0 == approx(30, 0.001)


def test_simple_yearly_case(simple_yearly_case):
    """Confirms consecutive files are separated by exactly 365 days in both time and time_bounds."""
    input_head_dir, output_head_dir = simple_yearly_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    paths = listdir(input_head_dir)
    paths.sort()

    time0 = None
    time_bnds0 = None
    with GenTSDataStore(f"{input_head_dir}/{paths[0]}", 'r') as hf_ds:
        time0 = hf_ds["time"][:][0]
        time_bnds0 = hf_ds["time_bounds"][:][0][0]
    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert hf_ds["time"][:][0] - time0 == approx(365, 0.001)
        assert hf_ds["time_bounds"][:][0][0] - time_bnds0 == approx(365, 0.001)


def test_long_case(long_case):
    """Confirms 240 files were created, each with only VAR0 and the expected shape."""
    input_head_dir, output_head_dir = long_case
    paths = listdir(input_head_dir)
    assert len(paths) == LONG_TEST_NUM_HIST_FILES

    with GenTSDataStore(f"{input_head_dir}/{paths[1]}", 'r') as hf_ds:
        assert "VAR0" in hf_ds.variables
        assert "VAR1" not in hf_ds.variables
        assert hf_ds["VAR0"].shape == (1, 1, 1)


def test_large_case(large_file_for_chunking_case):
    """Confirms 2 files were created with VAR0 only, and each variable's data size exceeds 4 MiB."""
    input_head_dir, output_head_dir = large_file_for_chunking_case
    paths = listdir(input_head_dir)
    assert len(paths) == 2

    for path in listdir(input_head_dir):
        with GenTSDataStore(f"{input_head_dir}/{path}", 'r') as hf_ds:
            assert "VAR0" in hf_ds.variables
            assert "VAR1" not in hf_ds.variables
            variable_size = np.prod(hf_ds["VAR0"].shape) * hf_ds["VAR0"].dtype.itemsize
            assert variable_size > 4*(1024**2)