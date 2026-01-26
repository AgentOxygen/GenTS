from gents.tests.test_cases import *
from gents.mhfdataset import *
from gents.hfcollection import HFCollection
from netCDF4 import Dataset
import numpy as np


def test_MHFDataset_simple(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            assert len(agg_hf_ds) == len(hf_groups[group])
            for index in range(len(agg_hf_ds)):
                assert agg_hf_ds[index].filepath() == str(hf_groups[group][index])

            assert np.array_equal(agg_hf_ds.get_var_data_shape("VAR0"), (SIMPLE_NUM_TEST_HIST_FILES, 3, 4))

            var1_t0_output = agg_hf_ds.get_var_vals("VAR1", time_index_start=0, time_index_end=1)
            
            assert np.array_equal(var1_t0_output, np.ones(var1_t0_output.shape))


def test_MHFDataset_fragmented(spatial_fragment_case):
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            assert len(agg_hf_ds) == len(hf_groups[group])
            for index in range(len(agg_hf_ds)):
                assert agg_hf_ds[index].filepath() == str(hf_groups[group][index])

            assert np.array_equal(
                agg_hf_ds.get_var_data_shape("VAR0"),
                (
                    FRAGMENTED_NUM_TIMESTEPS,
                    FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF,
                    FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
                )
            )


def test_get_concat_coords_simple(simple_case):
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            coords = get_concat_coords(agg_hf_ds)
            for dim in coords:
                if dim == "time" or coords[dim] is None:
                    continue
                if dim in agg_hf_ds[0].variables:
                    assert np.array_equal(agg_hf_ds[0][dim][:], coords[dim])
                else:
                    assert agg_hf_ds[0].dimensions[dim].size == len(coords[dim])
            assert len(coords["time"]) == SIMPLE_NUM_TEST_HIST_FILES


def test_get_concat_coords_fragmented(spatial_fragment_case):
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            coords = get_concat_coords(agg_hf_ds)
            assert len(coords["lat"]) == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF
            assert len(coords["lon"]) == FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
            assert len(coords["time"]) == FRAGMENTED_NUM_TIMESTEPS