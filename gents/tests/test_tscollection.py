from gents.tests.test_cases import *
from gents.hfcollection import HFCollection, find_files
from gents.timeseries import *
from os.path import isfile, getsize, isdir
from os import listdir, remove, makedirs
from netCDF4 import Dataset
from shutil import rmtree
from cftime import num2date


def clear_output_dir(output_dir):
    """Helper function to clear the output directory after testing"""
    for name in listdir(output_dir):
        if isfile(f"{output_dir}/{name}"):
            remove(f"{output_dir}/{name}")
        else:
            rmtree(f"{output_dir}/{name}")


def test_clear_output_dir(tmp_path_factory):
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
    """This does not test TSCollection, but the primary function it relies on."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    var_name = "VAR1"
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", var_name, ["time", "time_bounds"], "%Y%m%d", complevel=0, compression=None, overwrite=False)
    
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
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", var_name, ["time", "time_bounds"], "%Y%m%d", complevel=9, compression="zlib", overwrite=True)

    assert getsize(ts_path) < original_size
    assert len(listdir(output_head_dir)) == 1
    assert check_timeseries_integrity(ts_path)


def test_integrity_check(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", "VAR1", ["time", "time_bounds"], "%Y%m%d")
    assert check_timeseries_integrity(ts_path)
    for path in hf_paths:
        assert not check_timeseries_integrity(path)


def test_tscollection_copy(simple_case):
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
    """Assumes default compression is 0."""
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
    """
    To test the overwrite function, we write uncompressed and then overwrite with compressed.

    Since this entangles overwrite with compression, check if `test_tscollection_compression`
    passes, as it purely tests compression. 
    """
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
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 3/24, "%Y%m%d%H")


def test_simple_6hourly_case_timestr(simple_6hourly_case):
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 6/24, "%Y%m%d%H")


def test_simple_daily_case_timestr(simple_daily_case):
    input_head_dir, output_head_dir = simple_daily_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 1, "%Y%m%d")


def test_simple_monthly_case_timestr(simple_monthly_case):
    input_head_dir, output_head_dir = simple_monthly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 30, "%Y%m")


def test_simple_monthly_case_timestr(simple_yearly_case):
    input_head_dir, output_head_dir = simple_yearly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 365, "%Y")


def test_simple_6hourly_case_timestr_dir(simple_6hourly_case):
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_6" in listdir(output_head_dir)


def test_simple_3hourly_case_timestr_dir(simple_3hourly_case):
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_3" in listdir(output_head_dir)