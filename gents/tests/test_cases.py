from netCDF4 import Dataset
from os import makedirs
import numpy as np
import pytest
import random

CASE_START_YEAR = 1850
SIMPLE_NUM_TEST_HIST_FILES = 120
SIMPLE_NUM_VARS = 6
TIME_NUM_TEST_HIST_FILES = 2
TIME_NUM_VARS = 1
SCRAMBLED_NUM_TEST_HIST_FILES = 36
SCRAMBLED_NUM_VARS = 2
STRUCTURED_NUM_TEST_HIST_FILES = 2
STRUCTURED_NUM_VARS = 2
STRUCTURED_NUM_DIRS = 3
STRUCTURED_NUM_SUBDIRS = 2
FRAGMENTED_NUM_LAT_FILES = 3
FRAGMENTED_NUM_LON_FILES = 2
FRAGMENTED_NUM_LAT_PTS_PER_HF = 2
FRAGMENTED_NUM_LON_PTS_PER_HF = 1
FRAGMENTED_NUM_TIMESTEPS = 20


def generate_history_file(
        path,
        time_val,
        time_bounds_val,
        num_vars=SIMPLE_NUM_VARS,
        nc_format="NETCDF4_CLASSIC",
        time_bounds_attrs=True,
        time_name="time",
        time_bounds_name="time_bounds",
        auxiliary=False,
        dim_shapes=None,
        dim_vals={}
    ):
    if dim_shapes is None: 
        dim_shapes = {
            time_name: None,
            "bnds": 2,
            "lat": 3,
            "lon": 4,
            "lev": 1
        }
    
    with Dataset(path, "w", format=nc_format) as ds:
        for dim in dim_shapes:
            ds.createDimension(dim, dim_shapes[dim])
            if dim in dim_vals:
                dim_data = ds.createVariable(dim, float, (dim))
                dim_data[:] = dim_vals[dim]

        for index in range(num_vars):
            if auxiliary:
                var_data = ds.createVariable(f"VAR{index}", float, (time_name))

                var_data[:] = np.random.random((len(time_val))).astype(float)
                var_data.setncatts({
                    "units": "kg/g/m^2/K",
                    "standard_name": f"VAR{index}",
                    "long_name": f"variable_{index}"
                })
            else:
                var_data = ds.createVariable(f"VAR{index}", float, (time_name, "lat", "lon"))

                var_data[:] = index*np.ones((len(time_val), dim_shapes["lat"], dim_shapes["lon"])).astype(float)
                var_data.setncatts({
                    "units": "kg/g/m^2/K",
                    "standard_name": f"VAR{index}",
                    "long_name": f"variable_{index}"
                })

        time_data = ds.createVariable(time_name, np.double, time_name)
        time_data[:] = time_val
        time_data.setncatts({
            "calendar": "360_day",
            "units": f"days since {CASE_START_YEAR}-01-01",
            "standard_name": time_name,
            "long_name": time_name
        })

        if time_bounds_val is not None:
            time_bnds_data = ds.createVariable(time_bounds_name, np.double, (time_name, "bnds"))
            time_bnds_data[:] = time_bounds_val
            if time_bounds_attrs:
                time_bnds_data.setncatts({
                    "calendar": "360_day",
                    "units": "days since 1850-01-01",
                    "standard_name": time_bounds_name,
                    "long_name": time_bounds_name
                })
            
        ds.setncatts({
            "source": "GenTS testing suite",
            "description": "Synthetic data used for testing with the GenTS package.",
            "frequency": "month",
        })


@pytest.fixture(scope="function")
def time_bounds_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("time_history_files")
    head_ts_dir = tmp_path_factory.mktemp("time_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(TIME_NUM_TEST_HIST_FILES)]
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]], num_vars=TIME_NUM_VARS, time_bounds_name="Time_Bounds", time_name="Time")

    return head_hf_dir, head_ts_dir


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


@pytest.fixture(scope="function")
def simple_case_missing_attrs(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("simple_history_files")
    head_ts_dir = tmp_path_factory.mktemp("simple_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_NUM_TEST_HIST_FILES)]
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]], time_bounds_attrs=False)

    return head_hf_dir, head_ts_dir


@pytest.fixture(scope="function")
def multistep_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("multistep_history_files")
    head_ts_dir = tmp_path_factory.mktemp("multistep_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_NUM_TEST_HIST_FILES)]
    index = 0
    for  path in hf_paths:
        generate_history_file(path, [(index)*30, (index+1)*30, (index+2)*30], [[(index)*30, (index+1)*30], [(index+1)*30, (index+2)*30], [(index+2)*30, (index+3)*30]])
        index += 3

    return head_hf_dir, head_ts_dir


@pytest.fixture(scope="function")
def auxiliary_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("auxiliary_history_files")
    head_ts_dir = tmp_path_factory.mktemp("auxiliary_timeseries_files")
    
    hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(SIMPLE_NUM_TEST_HIST_FILES)]
    for file_index, path in enumerate(hf_paths):
        generate_history_file(path, [(file_index+1)*30], [[file_index*30, (file_index+1)*30]], auxiliary=True)

    return head_hf_dir, head_ts_dir


@pytest.fixture(scope="function")
def spatial_fragment_case(tmp_path_factory):
    head_hf_dir = tmp_path_factory.mktemp("fragmented_history_files")
    head_ts_dir = tmp_path_factory.mktemp("fragmented_timeseries_files")
    
    base_hf_paths = [f"{head_hf_dir}/testing.hf.{str(index).zfill(5)}.nc" for index in range(FRAGMENTED_NUM_TIMESTEPS)]

    lat_range = np.linspace(-90, 90, FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF)
    lon_range = np.linspace(-180, 180, FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF)

    dim_shapes = {
        "time": None,
        "bnds": 2,
        "lat": FRAGMENTED_NUM_LAT_PTS_PER_HF,
        "lon": FRAGMENTED_NUM_LON_PTS_PER_HF
    }

    for file_index, path in enumerate(base_hf_paths):
        tile_index = 0
        for lat_index in range(0, lat_range.size, FRAGMENTED_NUM_LAT_PTS_PER_HF):
            for lon_index in range(0, lon_range.size, FRAGMENTED_NUM_LON_PTS_PER_HF):
                dim_vals = {
                    "lat": lat_range[lat_index:lat_index+FRAGMENTED_NUM_LAT_PTS_PER_HF],
                    "lon": lon_range[lon_index:lon_index+FRAGMENTED_NUM_LON_PTS_PER_HF]
                }
                generate_history_file(f"{path}.{tile_index}", [(file_index+1)*180], [[file_index*180, (file_index+1)*180]], dim_shapes=dim_shapes, dim_vals=dim_vals)
                tile_index += 1

    return head_hf_dir, head_ts_dir