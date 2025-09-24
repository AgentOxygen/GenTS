from test_cases import *
from gents.hfcollection import HFCollection, find_files
from gents.timeseries import *
from os.path import isfile, getsize
from os import listdir, remove, makedirs
from netCDF4 import Dataset
from shutil import rmtree


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
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", var_name, ["time", "time_bounds"], complevel=0, compression=None, overwrite=False)
    
    assert isfile(ts_path)
    assert check_timeseries_integrity(ts_path)

    ts_ds = Dataset(ts_path, 'r')
    assert ts_ds["time"][:].size == len(hf_paths)
    
    for index in range(len(hf_paths)):
        hf_ds = Dataset(hf_paths[index], 'r')

        assert (ts_ds[var_name][:][index] == hf_ds[var_name][:]).all()
        hf_ds.close()

    assert "time" in ts_ds.variables
    assert "time_bounds" in ts_ds.variables

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

    ts_collection.include("*/2/*").execute()