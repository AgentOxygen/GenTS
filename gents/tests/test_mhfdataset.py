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


def test_MHFDataset_tight_memory_limit_does_not_crash_on_secondary_vars(tmp_path):
    """
    A memory_limit_bytes too small to cache every file's secondary-variable
    (time_bounds/lat/lon) data must not leave a variable PARTIALLY cached --
    __get_hf_data has no bounds-checked fallback for
    self.__data_secondary_var_cache, so a partial cache list crashes with
    IndexError the first time a file beyond the cached prefix is read.
    """
    n_files = 10
    dim_shapes = {"time": None, "bnds": 2, "lat": 2, "lon": 2}
    hf_paths = []
    for i in range(n_files):
        path = f"{tmp_path}/hf{i:02d}.nc"
        generate_history_file(path, [float(i)], [[float(i), float(i) + 1]], num_vars=1, dim_shapes=dim_shapes)
        hf_paths.append(path)

    with MHFDataset(hf_paths, preload_var_list=["VAR0"]) as full_ds:
        secondary_total = sum(
            full_ds.get_var_dsize(name) for name in full_ds._MHFDataset__data_secondary_var_cache
        )

    # Enough for some, but not all, files' worth of secondary data.
    limit = int(secondary_total * 0.5)

    ds = MHFDataset(hf_paths, preload_var_list=["VAR0"], memory_limit_bytes=limit)
    ds.open()
    try:
        for i in range(n_files):
            bounds = ds.get_var_vals("time_bounds", time_index_start=i, time_index_end=i + 1)
            assert np.array(bounds).flatten().tolist() == [float(i), float(i) + 1]
    finally:
        ds.close()


def test_MHFDataset_tight_memory_limit_does_not_corrupt_primary_vars(tmp_path):
    """
    Reading two primary variables in sequence -- exactly what
    TSCollection.execute() does for every group, one output file per
    variable off a shared MHFDataset -- under a memory_limit_bytes too small
    to fully cache both must not silently return one variable's data tagged
    with a different file's values. (Regression for the partial-cache
    index-alignment bug: once a variable's cache list is a partial prefix,
    __cache_variable's reactive fallback appends a fresh full re-read on top
    of it instead of replacing it, shifting every later index.)
    """
    n_files = 10
    hf_paths = []
    for i in range(n_files):
        path = f"{tmp_path}/hf{i:02d}.nc"
        generate_history_file(path, [(i + 0.5) * 30], [[i * 30, (i + 1) * 30]], num_vars=2)
        with GenTSDataStore(path, "a") as ds:
            ds["VAR0"][:] = float(i) * 10
            ds["VAR1"][:] = float(i) * 10 + 1
        hf_paths.append(path)

    with MHFDataset(hf_paths, preload_var_list=["VAR0", "VAR1"]) as full_ds:
        full0 = full_ds.get_var_dsize("VAR0")
        full1 = full_ds.get_var_dsize("VAR1")

    # Enough for VAR0 in full plus most, but not all, of VAR1.
    limit = full0 + int(full1 * 0.6)

    ds = MHFDataset(hf_paths, preload_var_list=["VAR0", "VAR1"], memory_limit_bytes=limit)
    ds.open()
    try:
        for var_name, tag in [("VAR0", 0.0), ("VAR1", 1.0)]:
            vals = np.array(ds.get_var_vals(var_name)).reshape(n_files, -1)[:, 0]
            expected = np.array([float(i) * 10 + tag for i in range(n_files)])
            assert np.array_equal(vals, expected), f"{var_name}: expected {expected.tolist()}, got {vals.tolist()}"
    finally:
        ds.close()


def test_MHFDataset_repeated_reads_of_same_file_do_not_return_none(tmp_path):
    """
    write_timeseries_file() writes a primary variable in fixed byte-sized
    chunks (chunksizes[0] from compute_chunksizes), independent of how many
    time steps live in each source history file. When a single file holds
    more time steps than fit in one write chunk (e.g. a high-frequency
    stream like hourly output, vs. the one-step-per-file fixtures every
    other test here uses), that file's cached data is requested across two
    separate get_var_vals() calls. __get_hf_data() nulls a file's cache
    entry the moment it's read once ("free up that memory" -- assuming
    each file is only ever read once per variable); the second call for the
    same file then gets back None, and get_var_vals()'s
    var_data[sub_t_index:sub_t_index + run_len] slice crashes with
    TypeError: 'NoneType' object is not subscriptable.
    """
    hf_paths = []
    for f in range(2):
        path = f"{tmp_path}/hf{f}.nc"
        time_vals = [f * 4 + t for t in range(4)]
        time_bnds = [[tv, tv + 1] for tv in time_vals]
        generate_history_file(path, time_vals, time_bnds, num_vars=1)
        with GenTSDataStore(path, "a") as ds:
            for t in range(4):
                ds["VAR0"][t, :, :] = f * 4 + t
        hf_paths.append(path)

    ds = MHFDataset(hf_paths, preload_var_list=["VAR0"])
    ds.open()
    try:
        # Mimic write_timeseries_file's chunked writes splitting file 0's 4
        # time steps across a write-chunk boundary (steps 0-1, then 2-3).
        chunk1 = np.array(ds.get_var_vals("VAR0", time_index_start=0, time_index_end=2))[:, 0, 0]
        chunk2 = np.array(ds.get_var_vals("VAR0", time_index_start=2, time_index_end=4))[:, 0, 0]
        assert chunk1.tolist() == [0.0, 1.0]
        assert chunk2.tolist() == [2.0, 3.0]
    finally:
        ds.close()


def test_MHFDataset_open_skips_secondary_var_missing_from_a_later_file(tmp_path):
    """
    open() decides once, from file 0's metadata, which secondary variables to
    cache (see __plan_cacheable_vars), then reuses that same fixed list for
    every file in the group. If a secondary/coordinate variable (e.g. CLM's
    time-invariant ZSOI soil-depth field) is present in file 0 but absent
    from a later file in the same group -- real CESM/CTSM history streams
    can vary their variable set across a run -- open() must skip it for that
    file rather than crash. The primary-variable loop already guards this
    (`if var_name not in hf_meta.get_primary_variables(): continue`); the
    secondary-variable loop is missing the equivalent guard, so it blindly
    indexes hf_ds["ZSOI"] on a file that doesn't have it and netCDF4 raises
    IndexError: ZSOI not found in /.
    """
    hf_paths = []
    for f in range(2):
        path = f"{tmp_path}/hf{f}.nc"
        generate_history_file(path, [(f + 0.5) * 30], [[f * 30, (f + 1) * 30]], num_vars=1)
        hf_paths.append(path)

    with GenTSDataStore(hf_paths[0], "a") as ds:
        ds.createDimension("levgrnd", 3)
        zsoi = ds.createVariable("ZSOI", float, ("levgrnd",))
        zsoi[:] = [0.1, 0.2, 0.3]
    # file 1 (and every later file) deliberately does NOT have ZSOI.

    with MHFDataset(hf_paths, preload_var_list=["VAR0"]) as agg_hf_ds:
        # ZSOI has no time dimension, so get_var_vals only ever reads it from
        # file 0 -- caching it there alone (and skipping the file that lacks
        # it) is correct, not partial.
        assert agg_hf_ds.get_var_vals("ZSOI").tolist() == [0.1, 0.2, 0.3]
        var_vals = agg_hf_ds.get_var_vals("VAR0")
        assert var_vals.shape[0] == 2