from os import listdir
from test_cases import *


def test_simple_case(simple_case):
    input_head_dir, output_head_dir = simple_case
    assert len(listdir(input_head_dir)) == SIMPLE_NUM_TEST_HIST_FILES


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
