from test_cases import *
from gents.hfcollection import HFCollection
from gents.timeseries import *
from os.path import isfile, getsize
from os import listdir
from netCDF4 import Dataset


def test_generate_time_series_func(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    var_name = "VAR1"
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", var_name, ["time", "time_bounds"], complevel=0, compression=None, overwrite=False)
    
    assert isfile(ts_path)
    assert check_timeseries_integrity(ts_path)

    ts_ds = Dataset(ts_path, 'r')
    assert ts_ds["time"][:].size == len(hf_paths)
    
    for index in range(len(hf_paths)):
        hf_ds = Dataset(hf_paths[index], 'r')

        assert (ts_ds[var_name][:][index] == hf_ds[var_name][:]).all()
        hf_ds.close()

    ts_ds.close()

    original_size = getsize(ts_path)
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", var_name, ["time", "time_bounds"], complevel=9, compression="zlib", overwrite=True)

    assert getsize(ts_path) < original_size
    assert len(listdir(output_head_dir)) == 1
    assert check_timeseries_integrity(ts_path)


def test_integrity_check(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", "VAR1", ["time", "time_bounds"])
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
    