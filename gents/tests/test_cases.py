from gents.utils import generate_history_file
from netCDF4 import Dataset
from os import makedirs
import pytest
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
