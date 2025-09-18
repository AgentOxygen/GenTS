from test_cases import *
from gents.hfcollection import *
from gents.meta import netCDFMeta
from pathlib import PosixPath
from os.path import isfile
import numpy as np


def test_find_files(structured_case):
    input_head_dir, output_head_dir = structured_case
    num_files = STRUCTURED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS
    assert len(find_files(input_head_dir, "*.nc")) == num_files

    with open(f'{input_head_dir}/empty_file.txt', 'w') as f: pass
    assert len(find_files(input_head_dir, "*.nc")) == num_files
    assert len(find_files(input_head_dir, "*.txt")) == 1
    assert len(find_files(input_head_dir, "*")) == 1 + num_files


def test_calculate_year_slices():
    assert calculate_year_slices(10, 0, 30) == [(0, 9), (10, 19), (20, 29)]
    assert calculate_year_slices(1, 0, 3) == [(0, 0), (1, 1), (2, 2)]
    assert calculate_year_slices(5, 3, 12) == [(0, 4), (5, 9), (10, 14)]


def test_hf_sorting(structured_case):
    input_head_dir, output_head_dir = structured_case
    hf_paths = find_files(input_head_dir, "*.nc")
    groups = sort_hf_groups(hf_paths)

    num_files = 0
    for group in groups:
        num_files += len(groups[group])

    assert len(groups) == STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS
    assert num_files == len(hf_paths)

    new_paths = hf_paths + [PosixPath(f"{path.parent}/{path.name.replace('testing', 'other_testing')}") for path in hf_paths]
    groups = sort_hf_groups(new_paths)

    num_files = 0
    for group in groups:
        num_files += len(groups[group])

    assert len(groups) == 2*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS
    assert num_files == 2*len(hf_paths)


def test_get_year_bounds(simple_case, scrambled_case, structured_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    assert get_year_bounds(hf_collection) == (CASE_START_YEAR, int(CASE_START_YEAR+((SIMPLE_NUM_TEST_HIST_FILES-1)/12)))

    input_head_dir, output_head_dir = scrambled_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    
    assert get_year_bounds(hf_collection) == (CASE_START_YEAR, int(CASE_START_YEAR+((SCRAMBLED_NUM_TEST_HIST_FILES-1)/12)))

    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    assert get_year_bounds(hf_collection) == (CASE_START_YEAR, int(CASE_START_YEAR+((STRUCTURED_NUM_TEST_HIST_FILES-1)/12)))


def test_simple_hfcollection(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    
    assert hf_collection.get_input_dir() == input_head_dir
    assert hasattr(hf_collection, '__iter__')
    assert len(hf_collection) == SIMPLE_NUM_TEST_HIST_FILES
    for path in hf_collection:
        assert isfile(path)


    excluded = hf_collection.exclude_patterns(["*.00001.nc"])
    included = hf_collection.include_patterns(["*.00001.nc"])
    
    assert len(excluded) == SIMPLE_NUM_TEST_HIST_FILES - 1
    assert len(included) == 1
    assert hf_collection is not excluded
    assert hf_collection is not included
    
    hf_collection.pull_metadata()
    
    valid_checks = hf_collection.check_validity()
    assert type(valid_checks) is dict
    assert len(valid_checks) == 0
    
    path_sample = list(hf_collection)[0]
    meta_sample = hf_collection[path_sample]
    assert meta_sample.get_path() == str(path_sample)
    assert type(meta_sample) == netCDFMeta
    
    groups = hf_collection.get_groups()
    assert type(groups) == dict
    assert len(groups) == 1
    assert len(groups[list(groups)[0]]) == len(hf_collection)
    
    sliced_groups = hf_collection.slice_groups(slice_size_years=1).get_groups()
    assert len(sliced_groups) == int(np.ceil(SIMPLE_NUM_TEST_HIST_FILES / 12))


def test_scrambled_hfcollection(scrambled_case):
    input_head_dir, output_head_dir = scrambled_case
    hf_collection = HFCollection(input_head_dir)

    assert len(hf_collection) == SCRAMBLED_NUM_TEST_HIST_FILES

    hf_collection.pull_metadata()
    assert len(hf_collection.check_validity()) == 0


def test_structured_hfcollection(structured_case):
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)

    assert len(hf_collection) == STRUCTURED_NUM_TEST_HIST_FILES*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS

    hf_collection.pull_metadata()
    assert len(hf_collection.check_validity()) == 0


def test_hfcollection_copy(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)

    hf_copy = hf_collection.copy()
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection

    hf_copy = hf_collection.include_patterns("*.nc")
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection

    hf_copy = hf_collection.exclude_patterns("*.txt")
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection
    
    hf_copy = hf_collection.include_years(0, 99999)
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection
    
    hf_copy = hf_collection.slice_groups(1)
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection
    