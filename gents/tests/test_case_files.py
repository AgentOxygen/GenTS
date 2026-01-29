from os import listdir
from netCDF4 import Dataset
import numpy as np
from gents.tests.test_cases import *


def test_simple_case(simple_case):
    input_head_dir, output_head_dir = simple_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES


def test_unstructured_grid_case(unstructured_grid_case):
    input_head_dir, output_head_dir = unstructured_grid_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES

    with Dataset(f"{input_head_dir}/{listdir(input_head_dir)[0]}", 'r') as hf_ds:
        assert hf_ds["VAR_AUX_0"].size == UNSTRUCT_GRID_NUM_NCOLS
        assert hf_ds["VAR_AUX_0"].dimensions == ("ncol",)
        assert hf_ds["VAR0"].dimensions == ("time", "ncol",)


def test_time_bounds_case(time_bounds_case):
    input_head_dir, output_head_dir = time_bounds_case
    assert len(listdir(input_head_dir)) == TIME_NUM_TEST_HIST_FILES
    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'Time_Bounds' in hf_ds.variables
            assert 'Time' in hf_ds.variables


def test_scrambled_case(scrambled_case):
    input_head_dir, output_head_dir = scrambled_case
    assert len(listdir(input_head_dir)) == SCRAMBLED_NUM_TEST_HIST_FILES


def test_structured_case(structured_case):
    input_head_dir, output_head_dir = structured_case
    assert len(listdir(input_head_dir)) == STRUCTURED_NUM_DIRS
    for top_dir in listdir(input_head_dir):
        assert len(listdir(f"{input_head_dir}/{top_dir}")) == STRUCTURED_NUM_SUBDIRS
        for sub_dir in listdir(f"{input_head_dir}/{top_dir}/"):
            assert len(listdir(f"{input_head_dir}/{top_dir}/{sub_dir}")) == STRUCTURED_NUM_TEST_HIST_FILES


def test_time_bounds_missing_attrs_case(simple_case_missing_attrs):
    input_head_dir, output_head_dir = simple_case_missing_attrs
    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'units' not in hf_ds['time_bounds'].ncattrs()
            assert 'calendar' not in hf_ds['time_bounds'].ncattrs()


def test_no_time_case(no_time_case):
    input_head_dir, output_head_dir = no_time_case
    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert 'time' not in hf_ds.variables
            assert 'time_bounds' not in hf_ds.variables


def test_multistep_case(multistep_case):
    input_head_dir, output_head_dir = multistep_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES
    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            assert len(hf_ds['time'].dimensions) == 1


def test_with_auxiliary_case(with_auxiliary_case):
    input_head_dir, output_head_dir = with_auxiliary_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES

    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            for var_index in range(SIMPLE_NUM_VARS):
                assert hf_ds[f"VAR_AUX_{var_index}"].dimensions == ('time',)


def test_fragmented_case(spatial_fragment_case):
    input_head_dir, output_head_dir = spatial_fragment_case
    assert len(listdir(input_head_dir)) == FRAGMENTED_NUM_TIMESTEPS*FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LON_FILES
    
    dim_hashes = []

    for file_name in listdir(input_head_dir):
        with Dataset(f"{input_head_dir}/{file_name}", 'r') as hf_ds:
            for var_index in range(SIMPLE_NUM_VARS):
                assert hf_ds["lat"].size == FRAGMENTED_NUM_LAT_PTS_PER_HF
                assert hf_ds["lon"].size == FRAGMENTED_NUM_LON_PTS_PER_HF
                assert np.unique(hf_ds["lat"][:]).size == hf_ds["lat"].size
                assert np.unique(hf_ds["lon"][:]).size == hf_ds["lon"].size
            
            dim_hash = str([hf_ds["lat"][:], hf_ds["lon"][:]])
            if dim_hash not in dim_hashes:
                dim_hashes.append(dim_hash)
    
    assert len(dim_hashes) == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LON_FILES
