from gents.tests.test_cases import *
from gents.hfcollection import HFCollection, find_files
from gents.datastore import GenTSDataStore
from gents.timeseries import *
from os.path import isfile, getsize, isdir
from os import listdir, remove, makedirs
from pathlib import Path
from shutil import rmtree
from cftime import num2date
import logging
import warnings


def clear_output_dir(output_dir):
    """Helper function to clear the output directory after testing"""
    for name in listdir(output_dir):
        if isfile(f"{output_dir}/{name}"):
            remove(f"{output_dir}/{name}")
        else:
            rmtree(f"{output_dir}/{name}")


def test_clear_output_dir(tmp_path_factory):
    """Validates the clear_output_dir() helper deletes all files and subdirectories."""
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
    """generate_time_series() produces a complete TS file with correct time size, variable values, and secondary vars; re-running with compression produces a smaller file."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    var_name = "VAR1"
    ts_args = {f"{var_name}": {"ts_string": "test", "complevel": 0, "compression": None, "overwrite": False}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    
    assert isfile(ts_path)
    assert check_timeseries_integrity(ts_path)

    with GenTSDataStore(ts_path, 'r') as ts_ds:
        assert ts_ds["time"][:].size == len(hf_paths)
        
        for index in range(len(hf_paths)):
            with GenTSDataStore(hf_paths[index], 'r') as hf_ds:
                assert (ts_ds[var_name][:][index] == hf_ds[var_name][:]).all()
        assert "time" in ts_ds.variables
        assert "time_bounds" in ts_ds.variables

    original_size = getsize(ts_path)

    ts_args[var_name]["complevel"] = 9
    ts_args[var_name]["compression"] = "zlib"
    ts_args[var_name]["overwrite"] = True
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]

    assert getsize(ts_path) < original_size
    assert len(listdir(output_head_dir)) == 1
    assert check_timeseries_integrity(ts_path)


def test_integrity_check(simple_case):
    """check_timeseries_integrity() returns True for GenTS-generated TS files and False for raw history files."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_args = {"VAR1": {"ts_string": "test"}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    assert check_timeseries_integrity(ts_path)
    for path in hf_paths:
        assert not check_timeseries_integrity(path)


def test_conform_check(simple_case):
    """check_timeseries_conform() returns True for a freshly generated TS file."""
    input_head_dir, output_head_dir = simple_case
    hf_paths = [f"{input_head_dir}/{name}" for name in listdir(input_head_dir)]

    ts_args = {"VAR1": {"ts_string": "test"}}
    ts_path = generate_time_series(hf_paths, f"{output_head_dir}/test_ts.", ["time", "time_bounds"], ts_args)[0]
    assert check_timeseries_conform(ts_path)


def test_tscollection_copy(simple_case):
    """All TSCollection modifier operations return new instances distinct from the original."""
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

    ts_copy = ts_collection.skip_existing()
    assert type(ts_copy) == TSCollection
    assert list(ts_copy) == list(ts_collection)
    assert ts_copy is not ts_collection
    

def test_tscollection_compression(simple_case):
    """Applying zlib compression at level 9 produces smaller output files than the uncompressed default."""
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
    """Overwriting existing files with compressed settings produces smaller files than the originals."""
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
    """TSCollection exclude() reduces output count; include() reduces it further; both are less than unfiltered."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.execute()
    unfiltered_num_files = len(find_files(output_head_dir, "*"))
    clear_output_dir(output_head_dir)

    ts_collection.exclude("*/1_dir/*").execute()
    exclude_num_files = len(find_files(output_head_dir, "*"))
    clear_output_dir(output_head_dir)

    ts_collection.include("*/1_dir/*").execute()
    include_num_files = len(find_files(output_head_dir, "*"))

    assert unfiltered_num_files > exclude_num_files
    assert exclude_num_files > include_num_files
    assert include_num_files > 0


def test_ts_collection_path_swapping(structured_case):
    """apply_path_swap() redirects output files to the substituted directory path."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.apply_path_swap("/0_subdir/", "/proc/tseries/").execute()
    assert isdir(f"{output_head_dir}/0_dir/proc/tseries/")
    assert len(listdir(f"{output_head_dir}/0_dir/proc/tseries/")) == STRUCTURED_NUM_TEST_HIST_FILES

    clear_output_dir(output_head_dir)

    ts_collection.apply_path_swap("dir", "folder").execute()
    assert isdir(f"{output_head_dir}/0_folder/0_subfolder/")
    assert len(listdir(output_head_dir)) == STRUCTURED_NUM_DIRS
    for top_dir in listdir(output_head_dir):
        assert len(listdir(f"{output_head_dir}/{top_dir}")) == STRUCTURED_NUM_SUBDIRS
        for sub_dir in listdir(f"{output_head_dir}/{top_dir}/"):
            assert len(listdir(f"{output_head_dir}/{top_dir}/{sub_dir}")) == STRUCTURED_NUM_VARS


def test_ts_collection_append_timestep_dirs(mixed_timestep_case):
    """append_timestep_dirs() creates hour_1, day_1, month_1, and year_1 subdirectories for mixed-frequency inputs."""
    input_head_dir, output_head_dir = mixed_timestep_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection.append_timestep_dirs().execute()
    
    assert isdir(f"{output_head_dir}/hour_1")
    assert isdir(f"{output_head_dir}/day_1")
    assert isdir(f"{output_head_dir}/month_1")
    assert isdir(f"{output_head_dir}/year_1")

    assert len(listdir(f"{output_head_dir}/hour_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/day_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/month_1")) == SIMPLE_NUM_VARS
    assert len(listdir(f"{output_head_dir}/year_1")) == SIMPLE_NUM_VARS


def compare_timestr(hf_collection, ts_paths, timestep, time_format):
    with GenTSDataStore(list(hf_collection)[0], 'r') as hf_ds:
        units = hf_ds["time"].units
        calendar = hf_ds["time"].calendar
        start_time = hf_ds["time"][:][0]

    start_date = num2date(start_time, units=units, calendar=calendar)
    end_date = num2date(start_time+timestep*(len(hf_collection)-1), units=units, calendar=calendar)

    for path in ts_paths:
        time_str = path.split(".")[-2]
        assert "-" in time_str
        assert len(time_str.split("-")) == 2
        assert time_str == f"{start_date.strftime(time_format)}-{end_date.strftime(time_format)}"


def test_simple_3hourly_case_timestr(simple_3hourly_case):
    """3-hourly TS filenames use the ``%Y%m%d%H`` timestamp format."""
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 3/24, "%Y%m%d%H")


def test_simple_6hourly_case_timestr(simple_6hourly_case):
    """6-hourly TS filenames use the ``%Y%m%d%H`` timestamp format."""
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 6/24, "%Y%m%d%H")


def test_simple_daily_case_timestr(simple_daily_case):
    """Daily TS filenames use the ``%Y%m%d`` timestamp format."""
    input_head_dir, output_head_dir = simple_daily_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 1, "%Y%m%d")


def test_simple_monthly_case_timestr(simple_monthly_case):
    """Monthly TS filenames use the ``%Y%m`` timestamp format."""
    input_head_dir, output_head_dir = simple_monthly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 30, "%Y%m")


def test_simple_yearly_case_timestr(simple_yearly_case):
    """Yearly TS filenames use the ``%Y`` timestamp format."""
    input_head_dir, output_head_dir = simple_yearly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()
    compare_timestr(hf_collection, ts_paths, 365, "%Y")


def test_simple_6hourly_case_timestr_dir(simple_6hourly_case):
    """append_timestep_dirs() creates an ``hour_6`` subdirectory for 6-hourly inputs."""
    input_head_dir, output_head_dir = simple_6hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_6" in listdir(output_head_dir)


def test_simple_3hourly_case_timestr_dir(simple_3hourly_case):
    """append_timestep_dirs() creates an ``hour_3`` subdirectory for 3-hourly inputs."""
    input_head_dir, output_head_dir = simple_3hourly_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir).append_timestep_dirs()
    ts_paths = ts_collection.execute()
    assert "hour_3" in listdir(output_head_dir)


def test_chunking(large_file_for_chunking_case):
    """Large variables are stored with time-axis chunking rather than contiguously."""
    input_head_dir, output_head_dir = large_file_for_chunking_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)
    ts_paths = ts_collection.execute()

    for path in ts_paths:
        assert check_timeseries_conform(path)
        with GenTSDataStore(path, 'r') as ts_ds:
            assert list(ts_ds["VAR0"].chunking()) != list(ts_ds["VAR0"].shape)


def test_dask_deprecation_warning(simple_case):
    """Passing dask_client=True to TSCollection raises a DeprecationWarning."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)

    with pytest.warns(DeprecationWarning):
        ts_collection = TSCollection(hf_collection, output_head_dir, dask_client=True)


def test_strfrmt_kwargs(simple_case):
    """Test various inputs to TSCollection ``strfrmt_kwargs``."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    for order in ts_collection:
        assert order["ts_string"] == "185001-185401"
    
    ts_collection = ts_collection.update_ts_orders(
        strfrmt_kwargs={"monthly_format": "%Y%m%d%H"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "1850011600-1854011600"

    ts_collection = ts_collection.update_ts_orders(
        strfrmt_kwargs={"monthly_format": "%Y"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "1850-1854"

    ts_collection = ts_collection.update_ts_orders(
        strfrmt_kwargs={"daily_format": "%Y%m%d%H"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "185001-185401"

    ts_collection = ts_collection.update_ts_orders(
        strfrmt_kwargs={"yearly_format": "%Y%m%d%H"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "185001-185401"


def test_time_alignment_kwargs(simple_case):
    """Test various inputs to TSCollection ``time_alignment_method``."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    with pytest.raises(ValueError):
        ts_collection = ts_collection.update_ts_orders(
            time_alignment_method=None
        )

    ts_collection = ts_collection.update_ts_orders(
        time_alignment_method="direct_time",
        strfrmt_kwargs={"monthly_format": "%Y%m%d"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "18500116-18540116"

    ts_collection = ts_collection.update_ts_orders(
        time_alignment_method="midpoint",
        strfrmt_kwargs={"monthly_format": "%Y%m%d"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "18500116-18540116"

    ts_collection = ts_collection.update_ts_orders(
        time_alignment_method="start_bound",
        strfrmt_kwargs={"monthly_format": "%Y%m%d"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "18500101-18540101"

    ts_collection = ts_collection.update_ts_orders(
        time_alignment_method="end_bound",
        strfrmt_kwargs={"monthly_format": "%Y%m%d"}
    )
    for order in ts_collection:
        assert order["ts_string"] == "18500201-18540201"


def test_tscollection_add_attrs(simple_case):
    """Adding attributes works as expected and is not an immutable function."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    ts_collection = ts_collection.add_attrs({"gents_test_key": "gents_test_val", "gents_test_key2": "gents_test_val2"})
    ts_paths = ts_collection.execute()

    assert len(ts_paths) > 0

    for path in ts_paths:
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds.getncattr("gents_test_key") == "gents_test_val"
            assert ts_ds.getncattr("gents_test_key2") == "gents_test_val2"


def test_get_timestep_label_unknown_delta():
    """An unknown timestep duration is labelled 'unsorted' rather than guessed at."""
    assert get_timestep_label(None) == "unsorted"


def test_update_ts_orders_no_full_time_decoding(multistep_large_case):
    """TSCollection construction (sorting + order building) decodes at most the
    per-file endpoint candidates, never a full time array."""
    from unittest.mock import patch
    from cftime import num2date as real_num2date
    import numpy as np
    input_head_dir, output_head_dir = multistep_large_case

    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata(show_progress=False)
    with patch("gents.meta.num2date", wraps=real_num2date) as mock_num2date:
        ts_collection = TSCollection(hf_collection, str(output_head_dir))
        for call in mock_num2date.call_args_list:
            values = np.atleast_1d(np.asarray(call.args[0]))
            assert values.size <= 2
    assert len(ts_collection) > 0


def test_execute_defaults_memory_limit(simple_case):
    """execute() forwards DEFAULT_MEMORY_LIMIT_BYTES to every MHFDataset it opens
    unless the caller overrides it."""
    from unittest.mock import patch
    from gents.mhfdataset import MHFDataset as real_MHFDataset
    input_head_dir, output_head_dir = simple_case

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, str(output_head_dir))
    with patch("gents.timeseries.MHFDataset", wraps=real_MHFDataset) as mock_ds:
        ts_collection.execute(show_progress=False)
    assert mock_ds.call_count > 0
    for call in mock_ds.call_args_list:
        assert call.kwargs["memory_limit_bytes"] == DEFAULT_MEMORY_LIMIT_BYTES
    clear_output_dir(output_head_dir)


def test_execute_no_data_skips_primary_preload(simple_case):
    """execute(no_data=True) opens its MHFDatasets with primary preloading
    disabled -- primary data is never read for structure-only output."""
    from unittest.mock import patch
    from gents.mhfdataset import MHFDataset as real_MHFDataset
    input_head_dir, output_head_dir = simple_case

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, str(output_head_dir))
    with patch("gents.timeseries.MHFDataset", wraps=real_MHFDataset) as mock_ds:
        ts_paths = ts_collection.execute(no_data=True, show_progress=False)
    assert mock_ds.call_count > 0
    for call in mock_ds.call_args_list:
        assert call.kwargs["preload_primaries"] is False
    assert len(ts_paths) > 0
    clear_output_dir(output_head_dir)


def test_execute_no_data_never_reads_primary_data(simple_case):
    """End-to-end: no primary variable data is read from any source file during a
    no_data run."""
    from unittest.mock import patch
    from gents.datastore import GenTSDataStore
    input_head_dir, output_head_dir = simple_case

    from gents.tests.test_mhfdataset import _RecordingVariable

    class RecordingDataStore(GenTSDataStore):
        accessed = []

        def __getitem__(self, key):
            return _RecordingVariable(super().__getitem__(key), str(key), RecordingDataStore.accessed)

    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, str(output_head_dir))
    with patch("gents.mhfdataset.GenTSDataStore", RecordingDataStore):
        ts_collection.execute(no_data=True, show_progress=False)
    assert not any(name.startswith("VAR") for name in RecordingDataStore.accessed)
    clear_output_dir(output_head_dir)


def test_exclude_requires_both_filters_to_match(structured_case):
    """exclude() drops an order only when the path AND the variable match, per its docstring."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))

    def labels(tsc):
        return {(("0_dir" in order["ts_path_template"]), order["primary_var"]) for order in tsc}

    everything = labels(ts_collection)
    target_var = sorted(var for _, var in everything)[0]
    assert len(everything) > 1

    # Both filters together remove only the intersection, not the union.
    narrowed = labels(ts_collection.exclude("*/0_dir/*", target_var))
    assert (True, target_var) not in narrowed
    assert (False, target_var) in narrowed
    assert any(in_dir for in_dir, _ in narrowed)

    # A variable filter with a catch-all path glob must not empty the collection.
    var_only = labels(ts_collection.exclude("*", target_var))
    assert var_only == {entry for entry in everything if entry[1] != target_var}
    assert len(var_only) > 0


def test_exclude_single_argument_drops_whole_paths(structured_case):
    """The one-argument form still drops every order under the matched path."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))

    remaining = ts_collection.exclude("*/0_dir/*")

    assert len(remaining) < len(ts_collection)
    for order in remaining:
        assert "/0_dir/" not in order["ts_path_template"]


def test_include_filters_on_variable(structured_case):
    """include() keeps only orders matching both the path and variable globs."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    target_var = sorted({order["primary_var"] for order in ts_collection})[0]

    narrowed = ts_collection.include("*/0_dir/*", target_var)

    assert len(narrowed) > 0
    for order in narrowed:
        assert order["primary_var"] == target_var
        assert "/0_dir/" in order["ts_path_template"]


def test_filters_accept_glob_lists(structured_case):
    """include/exclude take lists for both path and variable globs."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    all_vars = sorted({order["primary_var"] for order in ts_collection})
    kept_vars, dropped_var = all_vars[:1], all_vars[-1]
    assert kept_vars[0] != dropped_var

    included = ts_collection.include(["*/0_dir/*", "*/1_dir/*"], kept_vars)
    assert len(included) > 0
    for order in included:
        assert order["primary_var"] in kept_vars
        assert "/0_dir/" in order["ts_path_template"] or "/1_dir/" in order["ts_path_template"]

    excluded = ts_collection.exclude(["*/0_dir/*", "*/1_dir/*"], [dropped_var])
    for order in excluded:
        in_listed_dirs = "/0_dir/" in order["ts_path_template"] or "/1_dir/" in order["ts_path_template"]
        assert not (in_listed_dirs and order["primary_var"] == dropped_var)
    # 2_dir keeps every variable, including the one named in the exclude.
    assert any(order["primary_var"] == dropped_var for order in excluded)


def test_glob_list_equals_repeated_single_glob(structured_case):
    """A two-element list matches the union of the two globs applied separately."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))

    as_list = ts_collection.include(["*/0_dir/*", "*/1_dir/*"])
    union = {order["ts_path_template"] + order["primary_var"] for order in
             list(ts_collection.include("*/0_dir/*")) + list(ts_collection.include("*/1_dir/*"))}

    assert {order["ts_path_template"] + order["primary_var"] for order in as_list} == union


def test_add_args_accepts_glob_lists(structured_case):
    """The apply_*/add_args family takes lists too, so the whole API stays consistent."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    target_vars = sorted({order["primary_var"] for order in ts_collection})[:1]

    applied = ts_collection.apply_compression(4, "zlib", ["*/0_dir/*", "*/1_dir/*"], target_vars)

    compressed = [order for order in applied if order.get("complevel") == 4]
    assert len(compressed) > 0
    for order in compressed:
        assert order["primary_var"] in target_vars
        assert "/2_dir/" not in order["ts_path_template"]


def test_apply_path_swap_honors_var_glob(structured_case):
    """apply_path_swap swaps only orders whose variable matches var_glob."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    target_var = sorted({order["primary_var"] for order in ts_collection})[0]

    swapped = ts_collection.apply_path_swap("/0_dir/", "/SWAPPED/", var_glob=target_var)

    hits = [order for order in swapped if "/SWAPPED/" in order["ts_path_template"]]
    assert len(hits) > 0
    for order in hits:
        assert order["primary_var"] == target_var
    # Orders for other variables under the same path keep the original template.
    others = [order for order in swapped
              if order["primary_var"] != target_var and "/0_dir/" in order["ts_path_template"]]
    assert len(others) > 0


def test_apply_path_swap_default_var_glob_swaps_every_variable(structured_case):
    """The default var_glob='*' leaves the path-only behaviour of existing callers intact."""
    input_head_dir, output_head_dir = structured_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))

    swapped = ts_collection.apply_path_swap("/0_dir/", "/SWAPPED/")

    expected = sum("/0_dir/" in order["ts_path_template"] for order in ts_collection)
    assert sum("/SWAPPED/" in order["ts_path_template"] for order in swapped) == expected


def test_skip_existing_no_prior_output(continued_case):
    """With no existing output, skip_existing() leaves every order unchanged."""
    input_head_dir, output_head_dir, extend = continued_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))

    resumed = ts_collection.skip_existing()

    assert len(resumed) == len(ts_collection)
    for order, original in zip(resumed, ts_collection):
        assert order["hf_paths"] == original["hf_paths"]
        assert order["ts_start_index"] == original["ts_start_index"]
        assert order["ts_string"] == original["ts_string"]


def test_skip_existing_full_coverage_drops_order(continued_case):
    """Once every order's output already exists, skip_existing() drops every order."""
    input_head_dir, output_head_dir, extend = continued_case
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).execute()

    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    resumed = ts_collection.skip_existing()

    assert len(resumed) == 0


def test_skip_existing_partial_coverage_trims_and_prunes(continued_case):
    """A continuation run only regenerates the new timesteps, leaving prior output untouched."""
    input_head_dir, output_head_dir, extend = continued_case
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).execute()

    original_files = {name: getsize(f"{output_head_dir}/{name}") for name in listdir(output_head_dir)}
    assert len(original_files) == CONTINUED_NUM_VARS

    extend()

    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    resumed = ts_collection.skip_existing()

    assert len(resumed) == CONTINUED_NUM_VARS
    for order in resumed:
        # Only the new files remain -- the fully-covered ones were pruned, not
        # just skipped by index.
        assert len(order["hf_paths"]) == CONTINUED_EXTEND_NUM_HIST_FILES
        for path in order["hf_paths"]:
            file_index = int(Path(path).name.split(".")[2])
            assert file_index >= CONTINUED_INITIAL_NUM_HIST_FILES

    resumed.execute()

    new_files = {name: getsize(f"{output_head_dir}/{name}") for name in listdir(output_head_dir)}
    for name, size in original_files.items():
        assert name in new_files
        assert new_files[name] == size
    assert len(new_files) == 2 * CONTINUED_NUM_VARS


def test_skip_existing_duplicate_boundary_timestep(tmp_path_factory):
    """A continuation's first timestep exactly duplicating the prior run's last one is dropped."""
    head_hf_dir = tmp_path_factory.mktemp("dup_boundary_hf")
    head_ts_dir = tmp_path_factory.mktemp("dup_boundary_ts")

    generate_history_file(f"{head_hf_dir}/testing.hf.00000.nc", [15], [[0, 30]], num_vars=1)
    generate_history_file(f"{head_hf_dir}/testing.hf.00001.nc", [45], [[30, 60]], num_vars=1)
    TSCollection(HFCollection(head_hf_dir), str(head_ts_dir)).execute()

    # The continuation's first file duplicates the old run's last timestep
    # exactly (a restart-boundary duplicate); the second is genuinely new.
    generate_history_file(f"{head_hf_dir}/testing.hf.00002.nc", [45], [[30, 60]], num_vars=1)
    generate_history_file(f"{head_hf_dir}/testing.hf.00003.nc", [75], [[60, 90]], num_vars=1)

    resumed = TSCollection(HFCollection(head_hf_dir), str(head_ts_dir)).skip_existing()

    assert len(resumed) == 1
    order = resumed[0]
    assert len(order["hf_paths"]) == 1
    assert Path(order["hf_paths"][0]).name == "testing.hf.00003.nc"


def test_skip_existing_ignores_corrupt_existing_file(continued_case):
    """A file matching the naming pattern but failing the integrity check counts as no coverage."""
    input_head_dir, output_head_dir, extend = continued_case
    ts_collection = TSCollection(HFCollection(input_head_dir), str(output_head_dir))
    original_order = ts_collection[0]
    template, var = original_order["ts_path_template"], original_order["primary_var"]

    makedirs(Path(template).parent, exist_ok=True)
    with open(f"{template}.{var}.18500101-18501231.nc", "w") as corrupt_file:
        corrupt_file.write("not a real netCDF file")

    resumed = ts_collection.skip_existing()

    matched = [order for order in resumed if order["primary_var"] == var][0]
    assert matched["hf_paths"] == original_order["hf_paths"]


def test_skip_existing_multiple_variables_independent_coverage(continued_case):
    """Each order's coverage is evaluated independently, even within the same group."""
    input_head_dir, output_head_dir, extend = continued_case
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).execute()
    extend()

    # Only VAR0 gets regenerated for the new range; VAR1 is left behind.
    partial = TSCollection(HFCollection(input_head_dir), str(output_head_dir)).include("*", "VAR0").skip_existing()
    assert len(partial) == 1
    partial.execute()

    resumed = TSCollection(HFCollection(input_head_dir), str(output_head_dir)).skip_existing()

    var0_orders = [order for order in resumed if order["primary_var"] == "VAR0"]
    var1_orders = [order for order in resumed if order["primary_var"] == "VAR1"]
    assert len(var0_orders) == 0
    assert len(var1_orders) == 1
    assert len(var1_orders[0]["hf_paths"]) == CONTINUED_EXTEND_NUM_HIST_FILES


def test_skip_existing_zero_covered_time_not_mistaken_for_no_output(tmp_path_factory):
    """A latest-covered raw time of exactly 0.0 must not read as 'no existing output'."""
    head_hf_dir = tmp_path_factory.mktemp("zero_time_hf")
    head_ts_dir = tmp_path_factory.mktemp("zero_time_ts")

    # Second initial file's raw time lands exactly on 0.0 ("days since 1850-01-01").
    generate_history_file(f"{head_hf_dir}/testing.hf.00000.nc", [-30], [[-45, -15]], num_vars=1)
    generate_history_file(f"{head_hf_dir}/testing.hf.00001.nc", [0], [[-15, 15]], num_vars=1)
    TSCollection(HFCollection(head_hf_dir), str(head_ts_dir)).execute()

    generate_history_file(f"{head_hf_dir}/testing.hf.00002.nc", [30], [[15, 45]], num_vars=1)

    resumed = TSCollection(HFCollection(head_hf_dir), str(head_ts_dir)).skip_existing()

    assert len(resumed) == 1
    order = resumed[0]
    assert len(order["hf_paths"]) == 1
    assert Path(order["hf_paths"][0]).name == "testing.hf.00002.nc"


def test_skip_existing_ts_string_reflects_trimmed_range(continued_case):
    """A trimmed order's output filename describes the steps it actually covers."""
    input_head_dir, output_head_dir, extend = continued_case
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).execute()
    extend()

    resumed = TSCollection(HFCollection(input_head_dir), str(output_head_dir)).skip_existing()

    for order in resumed:
        assert order["ts_string"] == "185101-185112"


def test_skip_existing_recomputes_cut_indices_for_trimmed_order(straddling_case):
    """Trimming leading files from an order whose files straddle slice boundaries
    shifts its time indices onto the trimmed group."""
    input_head_dir, output_head_dir, write = straddling_case

    def build():
        hf_collection = HFCollection(input_head_dir)
        hf_collection.pull_metadata()
        return TSCollection(hf_collection.slice_groups(slice_size_years=1, start_year=CASE_START_YEAR), str(output_head_dir))

    write(range(5))    # Apr 1850 .. Sep 1852
    build().execute(raise_errors=True, show_progress=False)
    write(range(5, 7))  # Oct 1852 .. Sep 1853

    resumed = build().skip_existing()

    # 1852's order was files 3-5 (steps 3..15); files 3-4 are covered, leaving
    # file 5's first three steps (Oct-Dec 1852).
    trimmed = [order for order in resumed if order["ts_string"].startswith("1852")][0]
    assert [Path(path).name for path in trimmed["hf_paths"]] == ["testing.hf.00005.nc"]
    assert (trimmed["ts_start_index"], trimmed["ts_end_index"]) == (0, 3)

    resumed.execute(raise_errors=True, show_progress=False)
    with GenTSDataStore(f"{output_head_dir}/testing.hf.VAR0.185210-185212.nc", "r") as ds:
        assert ds["VAR0"][:, 0, 0].tolist() == [33, 34, 35]


def test_trailing_single_file_slice_keeps_last_step(straddling_case):
    """A slice made of one multi-step file that starts before the window and ends
    inside it keeps every in-window step, including the file's last."""
    input_head_dir, output_head_dir, write = straddling_case
    write(range(4))  # Apr 1850 .. Mar 1852; 1852's slice is file 3's Jan-Mar alone

    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    ts_collection = TSCollection(hf_collection.slice_groups(slice_size_years=1, start_year=CASE_START_YEAR), str(output_head_dir))
    ts_collection.execute(raise_errors=True, show_progress=False)

    with GenTSDataStore(f"{output_head_dir}/testing.hf.VAR0.185201-185203.nc", "r") as ds:
        assert ds["VAR0"][:, 0, 0].tolist() == [24, 25, 26]
