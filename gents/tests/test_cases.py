from gents.utils import get_time_stamp, get_version
from netCDF4 import Dataset
from os import listdir, makedirs
import pytest
import numpy as np
import random

CASE_START_YEAR = 1850
SIMPLE_NUM_TEST_HIST_FILES = 120
SIMPLE_NUM_VARS = 6
SCRAMBLED_NUM_TEST_HIST_FILES = 36
SCRAMBLED_NUM_VARS = 2
STRUCTURED_NUM_TEST_HIST_FILES = 2
STRUCTURED_NUM_VARS = 2
STRUCTURED_NUM_DIRS = 3
STRUCTURED_NUM_SUBDIRS = 2

def generate_history_file(path, time_val, time_bounds_val, num_vars=SIMPLE_NUM_VARS, nc_format="NETCDF4_CLASSIC"):
    ds = Dataset(path, "w", format=nc_format)

    dim_shapes = {
        "time": None,
        "bnds": 2,
        "lat": 3,
        "lon": 4,
        "lev": 5
    }
    
    for dim in dim_shapes:
        ds.createDimension(dim, dim_shapes[dim])

    for index in range(num_vars):
        var_data = ds.createVariable(f"VAR{index}", float, ("time", "lat", "lon"))
        var_data[:] = np.random.random((1, dim_shapes["lat"], dim_shapes["lon"])).astype(float)
        var_data.setncatts({
            "units": "kg/g/m^2/K",
            "standard_name": f"VAR{index}",
            "long_name": f"variable_{index}"
        })

    time_data = ds.createVariable(f"time", np.double, "time")
    time_data[:] = time_val
    time_data.setncatts({
        "calendar": "360_day",
        "units": f"days since {CASE_START_YEAR}-01-01",
        "standard_name": "time",
        "long_name": "time"
    })

    if time_bounds_val is not None:
        time_bnds_data = ds.createVariable(f"time_bounds", np.double, ("time", "bnds"))
        time_bnds_data[:] = time_bounds_val
        time_bnds_data.setncatts({
            "calendar": "360_day",
            "units": "days since 1850-01-01",
            "standard_name": "time_bounds",
            "long_name": "time_bounds"
        })
        
    ds.setncatts({
        "source": "GenTS testing suite",
        "description": "Synthetic data used for testing with the GenTS package.",
        "frequency": "month",
    })
    
    ds.close()


@pytest.fixture(scope="function")
def simple_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("simple_history_files")
    head_ts_dir = tmp_path_factory.mktemp("simple_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_NUM_TEST_HIST_FILES)]
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]])

    return head_hf_dir, head_ts_dir


@pytest.fixture(scope="function")
def scrambled_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("scrambled_history_files")
    head_ts_dir = tmp_path_factory.mktemp("scrambled_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SCRAMBLED_NUM_TEST_HIST_FILES)]

    random.seed(0)
    random.shuffle(hf_paths)
    
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]], num_vars=SCRAMBLED_NUM_VARS)
    return head_hf_dir, head_ts_dir
    

@pytest.fixture(scope="function")
def structured_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("structured_history_files")
    head_ts_dir = tmp_path_factory.mktemp("structured_timeseries_files")

    for top_index in range(STRUCTURED_NUM_DIRS):
        for sub_index in range(STRUCTURED_NUM_SUBDIRS):
            dir_path = f"{head_hf_dir}/{top_index}_dir/{sub_index}_subdir/"
            makedirs(dir_path, exist_ok=True)
            
            for file_index in range(STRUCTURED_NUM_TEST_HIST_FILES):
                path = f"{dir_path}/testing.hf.{str(file_index).zfill(5)}.nc" 
                generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]], num_vars=SCRAMBLED_NUM_VARS)
    return head_hf_dir, head_ts_dir


@pytest.fixture(scope="function")
def no_time_bounds_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("no_tb_history_files")
    head_ts_dir = tmp_path_factory.mktemp("no_tb_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_NUM_TEST_HIST_FILES)]
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], None)

    return head_hf_dir, head_ts_dir