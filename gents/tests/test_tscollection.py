from gents.tests.test_cases import *
from gents.hfcollection import HFCollection, find_files
from gents.timeseries import *
from os.path import isfile, getsize, isdir
from os import listdir, remove, makedirs
from netCDF4 import Dataset
from shutil import rmtree
from cftime import num2date
import warnings


def clear_output_dir(output_dir):
    """Helper function to clear the output directory after testing"""
    for name in listdir(output_dir):
        if isfile(f"{output_dir}/{name}"):
            remove(f"{output_dir}/{name}")
        else:
            rmtree(f"{output_dir}/{name}")


def test_clear_output_dir(tmp_path_factory):
    """Validates the clear_output_dir() helper deletes all files and subdirectories."""
    output_dir = tmp_path_factory.mktemp("output")
    for i in range(5):
        makedirs(f"{output_dir}/{i}/")
        for j in range(10):
            with open(f"{output_dir}/{i}/{j}.nc", 'w') as f:
                f.write("")
    
    assert len(find_files(output_dir, "*")) == 5*10
    clear_output_dir(output_dir)
    assert len(find_files(output_dir, "*")) == 0


def test_generate_time_series(simple_case):
    """generate_time_series() produces a complete TS file with correct time size, variable values, and secondary vars; re-running with compression produces a smaller file."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    var_name = "VAR1"
    ts_args = {f"{var_name}": {"ts_string": "test", "complevel": 0, "compression": None, "overwrite": False}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    
    assert isfile(ts_path)
    assert check_timeseries_integrity(ts_path)

    with Dataset(ts_path, 'r') as ts_ds:
        assert ts_ds["time"][:].size == len(hf_paths)
        
        for index in range(len(hf_paths)):
            with Dataset(hf_paths[index], 'r') as hf_ds:
                assert (ts_ds[var_name][:][index] == hf_ds[var_name][:]).all()
        assert "time" in ts_ds.variables
        assert "time_bounds" in ts_ds.variables

    original_size = getsize(ts_path)

    ts_args[var_name]["complevel"] = 9
    ts_args[var_name]["compression"] = "zlib"
    ts_args[var_name]["overwrite"] = True
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]

    assert getsize(ts_path) < original_size
    assert len(listdir(output_head_dir)) == 1
    assert check_timeseries_integrity(ts_path)


def test_integrity_check(simple_case):
    """check_timeseries_integrity() returns True for GenTS-generated TS files and False for raw history files."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_args = {"VAR1": {"ts_string": "test"}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    assert check_timeseries_integrity(ts_path)
    for path in hf_paths:
        assert not check_timeseries_integrity(path)


def test_conform_check(simple_case):
    """check_timeseries_conform() returns True for a freshly generated TS file."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_args = {"VAR1": {"ts_string": "test"}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    assert check_timeseries_conform(ts_path)


def test_tscollection_copy(simple_case):
    """All TSCollection modifier operations return new instances distinct from the original."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_copy = ts_collection.copy()
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection

    ts_copy = ts_collection.include("*.nc", "*")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection

    ts_copy = ts_collection.exclude("*.txt", "txt")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection
    
    ts_copy = ts_collection.apply_compression(1, "zlib", "*.txt", "txt")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection
    
    ts_copy = ts_collection.apply_overwrite("*.txt", "txt")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection

    ts_copy = ts_collection.remove_overwrite("*.txt", "txt")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection

    ts_copy = ts_collection.apply_path_swap("a", "b", "*.txt", "txt")
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection
    

def test_tscollection_compression(simple_case):
    """Applying zlib compression at level 9 produces smaller output files than the uncompressed default."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.execute()

    uncompressed_size = 0
    for file_name in listdir(output_head_dir):
        uncompressed_size += getsize(f"{output_head_dir}/{file_name}")

    clear_output_dir(output_head_dir)

    compressed_collection = ts_collection.apply_compression(9, "zlib", "*", "*")
    compressed_collection.execute()

    compressed_size = 0
    for file_name in listdir(output_head_dir):
        compressed_size += getsize(f"{output_head_dir}/{file_name}")

    assert compressed_size > 0
    assert compressed_size < uncompressed_size


def test_tscollection_overwrite(simple_case):
    """Overwriting existing files with compressed settings produces smaller files than the originals."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.execute()
    uncompressed_size = 0
    for file_name in listdir(output_head_dir):
        uncompressed_size += getsize(f"{output_head_dir}/{file_name}")

    compressed_collection = ts_collection.apply_compression(9, "zlib", "*", "*").apply_overwrite("*", "*")
    compressed_collection.execute()
    compressed_size = 0
    for file_name in listdir(output_head_dir):
        compressed_size += getsize(f"{output_head_dir}/{file_name}")

    assert compressed_size > 0
    assert compressed_size < uncompressed_size


def test_tscollection_filters(structured_case):
    """TSCollection exclude() reduces output count; include() reduces it further; both are less than unfiltered."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.execute()
    unfiltered_num_files = len(find_files(output_head_dir, "*"))
    clear_output_dir(output_head_dir)

    ts_collection.exclude("*/1_dir/*").execute()
    exclude_num_files = len(find_files(output_head_dir, "*"))
    clear_output_dir(output_head_dir)

    ts_collection.include("*/1_dir/*").execute()
    include_num_files = len(find_files(output_head_dir, "*"))

    assert unfiltered_num_files > exclude_num_files
    assert exclude_num_files > include_num_files
    assert include_num_files > 0


def test_ts_collection_path_swapping(structured_case):
    """apply_path_swap() redirects output files to the substituted directory path."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.apply_path_swap("/0_subdir/", "/proc/tseries/").execute()
    assert isdir(f"{output_head_dir}/0_dir/proc/tseries/")
    assert len(listdir(f"{output_head_dir}/0_dir/proc/tseries/")) == STRUCTURED_NUM_TEST_HIST_FILES

    clear_output_dir(output_head_dir)

    ts_collection.apply_path_swap("dir", "folder").execute()
    assert isdir(f"{output_head_dir}/0_folder/0_subfolder/")
    assert len(listdir(output_head_dir)) == STRUCTURED_NUM_DIRS
    for top_dir in listdir(output_head_dir):
        assert len(listdir(f"{output_head_dir}/{top_dir}")) == STRUCTURED_NUM_SUBDIRS
        for sub_dir in listdir(f"{output_head_dir}/{top_dir}/"):
            assert len(listdir(f"{output_head_dir}/{top_dir}/{sub_dir}")) == STRUCTURED_NUM_VARS


def test_ts_collection_append_timestep_dirs(mixed_timestep_case):
    """append_timestep_dirs() creates hour_1, day_1, month_1, and year_1 subdirectories for mixed-frequency inputs."""
    input_head_dir, output_head_dir = mixed_timestep_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.append_timestep_dirs().execute()
    
    assert isdir(f"{output_head_dir}/hour_1")
    assert isdir(f"{output_head_dir}/day_1")
    assert isdir(f"{output_head_dir}/month_1")
    assert isdir(f"{output_head_dir}/year_1")

    assert len(listdir(f"{output_head_dir}/hour_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/day_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/month_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/year_1")) == SIMPLE_NUM_VARS


def compare_timestr(hf_collection, ts_paths, timestep, time_format):
    with Dataset(list(hf_collection)[0], 'r') as hf_ds:
        units = hf_ds["time"].units
        calendar = hf_ds["time"].calendar
        start_time = hf_ds["time"][:][0]

    start_date = num2date(start_time, units=units, calendar=calendar)
    end_date = num2date(start_time+timestep*(len(hf_collection)-1), units=units, calendar=calendar)

    for path in ts_paths:
        time_str = path.split(".")[-2]
        assert "-" in time_str
        assert len(time_str.split("-")) == 2
        assert time_str == f"{start_date.strftime(time_format)}-{end_date.strftime(time_format)}"


def test_simple_3hourly_case_timestr(simple_3hourly_case):
    """3-hourly TS filenames use the ``%Y%m%d%H`` timestamp format."""
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 3/24, "%Y%m%d%H")


def test_simple_6hourly_case_timestr(simple_6hourly_case):
    """6-hourly TS filenames use the ``%Y%m%d%H`` timestamp format."""
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 6/24, "%Y%m%d%H")


def test_simple_daily_case_timestr(simple_daily_case):
    """Daily TS filenames use the ``%Y%m%d`` timestamp format."""
    input_head_dir, output_head_dir = simple_daily_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 1, "%Y%m%d")


def test_simple_monthly_case_timestr(simple_monthly_case):
    """Monthly TS filenames use the ``%Y%m`` timestamp format."""
    input_head_dir, output_head_dir = simple_monthly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 30, "%Y%m")


def test_simple_yearly_case_timestr(simple_yearly_case):
    """Yearly TS filenames use the ``%Y`` timestamp format."""
    input_head_dir, output_head_dir = simple_yearly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 365, "%Y")


def test_simple_6hourly_case_timestr_dir(simple_6hourly_case):
    """append_timestep_dirs() creates an ``hour_6`` subdirectory for 6-hourly inputs."""
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_6" in listdir(output_head_dir)


def test_simple_3hourly_case_timestr_dir(simple_3hourly_case):
    """append_timestep_dirs() creates an ``hour_3`` subdirectory for 3-hourly inputs."""
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_3" in listdir(output_head_dir)


def test_chunking(large_file_for_chunking_case):
    """Large variables are stored with time-axis chunking rather than contiguously."""
    input_head_dir, output_head_dir = large_file_for_chunking_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    for path in ts_paths:
        assert check_timeseries_conform(path)
        with Dataset(path, 'r') as ts_ds:
            assert list(ts_ds["VAR0"].chunking()) != list(ts_ds["VAR0"].shape)

def test_dask_deprecation_warning(simple_case):
    """Passing dask_client=True to TSCollection raises a DeprecationWarning."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)

    with pytest.warns(DeprecationWarning):
        ts_collection = TSCollection(hf_collection, output_head_dir, dask_client=True)