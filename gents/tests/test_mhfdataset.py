from gents.tests.test_cases import *
from gents.mhfdataset import *
from gents.hfcollection import HFCollection
from gents.datastore import GenTSDataStore
from unittest.mock import patch
import numpy as np


def test_MHFDataset_open_skips_date_decoding(simple_case):
    """open() never triggers cftime decoding -- MHFDataset only ever uses raw float times."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()
    group = next(iter(hf_groups))

    with patch("gents.meta.num2date") as mock_num2date:
        with MHFDataset(hf_groups[group]):
            pass
        mock_num2date.assert_not_called()


def test_MHFDataset_open_skips_redundant_netCDFMeta_work(simple_case):
    """open() opts every file out of time-bounds loading and dim-bounds computation (both duplicate work MHFDataset does itself), and opts out of variable-attribute loading for every file but the first (the only one get_var_attrs() ever reads from)."""
    from gents.meta import netCDFMeta as real_netCDFMeta
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()
    group = next(iter(hf_groups))

    with patch("gents.mhfdataset.netCDFMeta", wraps=real_netCDFMeta) as mock_meta:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            # get_var_attrs() must still work end-to-end off file 0's loaded attrs.
            assert agg_hf_ds.get_var_attrs("VAR0").get("standard_name") == "VAR0"

    calls = mock_meta.call_args_list
    assert len(calls) == len(hf_groups[group])
    for index, call in enumerate(calls):
        assert call.kwargs["decode_dates"] is False
        assert call.kwargs["load_time_bounds"] is False
        assert call.kwargs["compute_dim_bounds"] is False
        assert call.kwargs["load_variable_attrs"] is (index == 0)


def test_MHFDataset_simple(simple_case):
    """MHFDataset opens a non-fragmented group, exposes correct file handles, and returns the expected variable shape and values."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            assert len(agg_hf_ds) == len(hf_groups[group])
            for index in range(len(agg_hf_ds)):
                assert str(agg_hf_ds[index]) == str(hf_groups[group][index])

            assert np.array_equal(agg_hf_ds.get_var_data_shape("VAR0"), (SIMPLE_NUM_TEST_HIST_FILES, 3, 4))

            var1_t0_output = agg_hf_ds.get_var_vals("VAR1", time_index_start=0, time_index_end=1)
            
            assert np.array_equal(var1_t0_output, np.ones(var1_t0_output.shape))


def test_MHFDataset_fragmented(spatial_fragment_case):
    """MHFDataset over a fragmented group reports the full combined spatial shape across all tiles."""
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        with MHFDataset(hf_groups[group]) as agg_hf_ds:
            assert len(agg_hf_ds) == len(hf_groups[group])
            for index in range(len(agg_hf_ds)):
                assert str(agg_hf_ds[index]) == str(hf_groups[group][index])

            assert np.array_equal(
                agg_hf_ds.get_var_data_shape("VAR0"),
                (
                    FRAGMENTED_NUM_TIMESTEPS,
                    FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF,
                    FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
                )
            )


def test_extend_coords_simple(simple_case):
    """extend_coords(), accumulated across a non-fragmented group's files, returns coordinate values matching the first file and the correct time count."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        paths = hf_groups[group]
        coords = {}
        for path in paths:
            with GenTSDataStore(path, 'r') as ds:
                coords = extend_coords(ds, coords)

        with GenTSDataStore(paths[0], 'r') as ds0:
            for dim in coords:
                if dim == "time" or coords[dim] is None:
                    continue
                if dim in ds0.variables:
                    assert np.array_equal(ds0[dim][:], coords[dim])
                else:
                    assert ds0.dimensions[dim].size == len(coords[dim])
        assert len(coords["time"]) == SIMPLE_NUM_TEST_HIST_FILES


def test_extend_coords_fragmented(spatial_fragment_case):
    """extend_coords(), accumulated across a fragmented group's files, merges lat/lon coordinates to the full combined extent."""
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_groups = hf_collection.get_groups()

    for group in hf_groups:
        paths = hf_groups[group]
        coords = {}
        for path in paths:
            with GenTSDataStore(path, 'r') as ds:
                coords = extend_coords(ds, coords)

        assert len(coords["lat"]) == FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LAT_PTS_PER_HF
        assert len(coords["lon"]) == FRAGMENTED_NUM_LON_FILES*FRAGMENTED_NUM_LON_PTS_PER_HF
        assert len(coords["time"]) == FRAGMENTED_NUM_TIMESTEPS


def test_MHFDataset_fragmented_values(tmp_path):
    """get_var_vals() on a fragmented group places each tile's data at its own grid location, not just the right overall shape."""
    tile_defs = [
        {"lat": [-45.0], "lon": [-90.0], "value": 10.0},
        {"lat": [-45.0], "lon": [90.0], "value": 20.0},
        {"lat": [45.0], "lon": [-90.0], "value": 30.0},
        {"lat": [45.0], "lon": [90.0], "value": 40.0},
    ]
    dim_shapes = {"time": None, "bnds": 2, "lat": 1, "lon": 1}
    hf_paths = []
    for tile_index, tile in enumerate(tile_defs):
        path = f"{tmp_path}/tile{tile_index}.nc"
        generate_history_file(
            path, [180.0], [[0.0, 180.0]], num_vars=1,
            dim_shapes=dim_shapes,
            dim_vals={"lat": tile["lat"], "lon": tile["lon"]},
        )
        with GenTSDataStore(path, "a") as ds:
            ds["VAR0"][:] = tile["value"]
        hf_paths.append(path)

    with MHFDataset(hf_paths) as agg_hf_ds:
        var_vals = agg_hf_ds.get_var_vals("VAR0")
        lat_vals = agg_hf_ds.get_var_vals("lat")
        lon_vals = agg_hf_ds.get_var_vals("lon")

    for tile in tile_defs:
        lat_index = int(np.where(lat_vals == tile["lat"][0])[0][0])
        lon_index = int(np.where(lon_vals == tile["lon"][0])[0][0])
        assert var_vals[0, lat_index, lon_index] == tile["value"]