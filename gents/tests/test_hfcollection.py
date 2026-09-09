from gents.tests.test_cases import *
from gents.hfcollection import *
from gents.meta import netCDFMeta
from gents.utils import enable_logging
from pathlib import PosixPath
from os.path import isfile
import logging
import numpy as np
import fnmatch


def test_find_files(structured_case):
    """find_files() matches only the specified glob pattern, ignoring non-matching file types."""
    input_head_dir, output_head_dir = structured_case
    num_files = STRUCTURED_NUM_VARS*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS
    assert len(find_files(input_head_dir, "*.nc")) == num_files

    with open(f'{input_head_dir}/empty_file.txt', 'w') as f: pass
    assert len(find_files(input_head_dir, "*.nc")) == num_files
    assert len(find_files(input_head_dir, "*.txt")) == 1
    assert len(find_files(input_head_dir, "*")) == 1 + num_files


def test_calculate_year_slices():
    """Spot-checks that year slices have correct widths, alignment, and non-overlapping bounds."""
    assert calculate_year_slices(10, 0, 30) == [(0, 9), (10, 19), (20, 29), (30, 39)]
    assert calculate_year_slices(10, 1, 30) == [(1, 10), (11, 20), (21, 30)]
    assert calculate_year_slices(1, 0, 3) == [(0, 0), (1, 1), (2, 2), (3, 3)]
    assert calculate_year_slices(5, 3, 12) == [(3, 7), (8, 12)]


def test_hf_sorting(structured_case):
    """sort_hf_groups() groups files by parent directory and filename prefix; distinct prefixes produce distinct groups."""
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


def test_hf_sorting_semantics():
    """sort_hf_groups() pins the group key, prefix derivation, and ordering contract."""
    paths = [
        PosixPath("/case/atm/hist/model.cam.h0.0001-01.nc"),
        PosixPath("/case/atm/hist/model.cam.h0.0001-02.nc"),
        PosixPath("/case/atm/hist/model.cam.h1.0001-01.nc"),
        PosixPath("/case/ocn/hist/model.pop.h.0001-01.nc"),
    ]
    groups = sort_hf_groups(paths)

    assert groups == {
        "/case/atm/hist/model.cam.h0*": paths[0:2],
        "/case/atm/hist/model.cam.h1*": [paths[2]],
        "/case/ocn/hist/model.pop.h*": [paths[3]],
    }
    # Keys are ordered by parent directory (first appearance), then prefix.
    assert list(groups) == [
        "/case/atm/hist/model.cam.h0*",
        "/case/atm/hist/model.cam.h1*",
        "/case/ocn/hist/model.pop.h*",
    ]
    # Identical names in different directories never share a group.
    split_dirs = sort_hf_groups([PosixPath("/d1/f.h0.001.nc"), PosixPath("/d2/f.h0.001.nc")])
    assert list(split_dirs) == ["/d1/f.h0*", "/d2/f.h0*"]

    # substring_index controls how many trailing tokens are dropped.
    deep = [PosixPath("/d/a.b.c.d.e.nc")]
    assert list(sort_hf_groups(deep, substring_index=1)) == ["/d/a.b.c.d.e*"]
    assert list(sort_hf_groups(deep, substring_index=3)) == ["/d/a.b.c*"]
    assert list(sort_hf_groups(deep, delimiter="_")) == ["/d/a.b.c.d.e.nc*"]

    # Fewer delimiters than substring_index: strip what is there, no more.
    assert list(sort_hf_groups([PosixPath("/d/single.nc")])) == ["/d/single*"]
    assert list(sort_hf_groups([PosixPath("/d/nodelimiter")])) == ["/d/nodelimiter*"]
    assert list(sort_hf_groups([PosixPath("/d/.nc")])) == ["/d/*"]

    assert sort_hf_groups([]) == {}


def test_hf_sorting_preserves_path_order(structured_case):
    """Every input path lands in exactly one group, in its original relative order."""
    input_head_dir, output_head_dir = structured_case
    hf_paths = find_files(input_head_dir, "*.nc")
    groups = sort_hf_groups(hf_paths)

    regrouped = [path for group_paths in groups.values() for path in group_paths]
    assert sorted(regrouped) == sorted(hf_paths)
    assert len(regrouped) == len(set(regrouped))
    for group_paths in groups.values():
        assert group_paths == sorted(group_paths, key=hf_paths.index)


def test_get_year_bounds(simple_case, scrambled_case, structured_case, multistep_large_case):
    """get_year_bounds() returns correct min/max years for simple, scrambled, and structured cases."""
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

    input_head_dir, output_head_dir = multistep_large_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    assert get_year_bounds(hf_collection) == (CASE_START_YEAR, np.ceil(CASE_START_YEAR+(((MS_LARGE_NUM_TEST_HIST_FILES-1)*MS_LARGE_NUM_TIMESTEPS)/12)))


def test_simple_hfcollection(simple_case, caplog):
    """Comprehensive test of HFCollection: file count, include/exclude, pull_metadata, check_validity, get_groups, and slice_groups."""
    caplog.set_level(logging.WARNING, logger="gents")
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    
    assert hf_collection.get_input_dir() == input_head_dir
    assert hasattr(hf_collection, '__iter__')
    assert len(hf_collection) == SIMPLE_NUM_TEST_HIST_FILES
    for path in hf_collection:
        assert isfile(path)


    excluded = hf_collection.exclude(["*.00001.nc"])
    included = hf_collection.include(["*.00001.nc"])
    
    assert len(excluded) == SIMPLE_NUM_TEST_HIST_FILES - 1
    assert len(included) == 1
    assert hf_collection is not excluded
    assert hf_collection is not included

    assert not hf_collection.is_pulled()

    hf_collection.pull_metadata()

    assert hf_collection.is_pulled()

    meta_data_pulled = False
    for path in hf_collection:
        if hf_collection[path] is not None:
            meta_data_pulled = True
            break

    valid_checks = hf_collection.check_validity()
    assert type(valid_checks) is dict
    assert len(valid_checks) == 0
    
    path_sample = list(hf_collection)[0]
    meta_sample = hf_collection[path_sample]
    assert meta_sample.get_path() == path_sample
    assert type(meta_sample) == netCDFMeta
    
    groups = hf_collection.get_groups()
    assert type(groups) == dict
    assert len(groups) == 1
    assert len(groups[list(groups)[0]]) == len(hf_collection)

    group_meta_map = {path: hf_collection[path] for path in hf_collection}
    
    min_year, max_year = get_year_bounds(group_meta_map)
    assert (min_year, max_year) == (CASE_START_YEAR, CASE_START_YEAR + int(np.floor(SIMPLE_NUM_TEST_HIST_FILES / 12)))
    
    for slice_size in range(1, 10):
        repeat_years = []
        year_slices = calculate_year_slices(slice_size, min_year, max_year)
        for lower, upper in year_slices:
            assert upper - lower <= slice_size
            assert lower <= upper
            assert lower not in repeat_years
            assert upper not in repeat_years
            repeat_years.append(lower)
            repeat_years.append(upper)
        assert year_slices[0][0] <= min_year
        assert year_slices[-1][1] >= max_year

    sliced_groups = hf_collection.slice_groups(slice_size_years=1).get_groups()
    assert len(sliced_groups) == int(np.ceil(SIMPLE_NUM_TEST_HIST_FILES / 12))
    
    for group in list(sliced_groups)[:-1]:
        assert len(sliced_groups[group]) == 12

    assert len(caplog.text) == 0


def test_time_bounds_case(time_bounds_case):
    """HFCollection loads files with non-default time variable names without error."""
    input_head_dir, output_head_dir = time_bounds_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()

    assert len(hf_collection) == TIME_NUM_TEST_HIST_FILES


def test_no_times_case(no_time_case):
    """pull_metadata() raises ValueError when history files have no recognised time variable."""
    input_head_dir, output_head_dir = no_time_case
    hf_collection = HFCollection(input_head_dir)
    with pytest.raises(ValueError, match=".nc"):
        hf_collection.pull_metadata(raise_errors=True)


def test_scrambled_hfcollection(scrambled_case):
    """HFCollection loads scrambled-order files and all pass validity checks."""
    input_head_dir, output_head_dir = scrambled_case
    hf_collection = HFCollection(input_head_dir)

    assert len(hf_collection) == SCRAMBLED_NUM_TEST_HIST_FILES

    hf_collection.pull_metadata()
    assert len(hf_collection.check_validity()) == 0


def test_structured_hfcollection(structured_case):
    """HFCollection discovers all files across a multi-directory structure and they all pass validity checks."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)

    assert len(hf_collection) == STRUCTURED_NUM_TEST_HIST_FILES*STRUCTURED_NUM_DIRS*STRUCTURED_NUM_SUBDIRS

    hf_collection.pull_metadata()
    assert len(hf_collection.check_validity()) == 0


def test_hfcollection_copy(simple_case):
    """All filter/transform operations return new HFCollection instances distinct from the original."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)

    hf_copy = hf_collection.copy()
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection

    hf_copy = hf_collection.include("*.nc")
    assert type(hf_copy) == HFCollection
    assert list(hf_copy) == list(hf_collection)
    assert hf_copy is not hf_collection

    hf_copy = hf_collection.exclude("*.txt")
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


def test_missing_time_bounds_attrs(simple_case_missing_attrs):
    """Files with time_bounds missing units/calendar attributes are still considered valid."""
    input_head_dir, output_head_dir = simple_case_missing_attrs
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()

    for entry in hf_collection:
        assert hf_collection[entry].is_valid()


def test_spatially_fragmented_hf(spatial_fragment_case):
    """Spatially fragmented tile files are merged into a single group by get_groups()."""
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()

    assert len(hf_collection) == FRAGMENTED_NUM_TIMESTEPS*FRAGMENTED_NUM_LAT_FILES*FRAGMENTED_NUM_LON_FILES
    groups = hf_collection.get_groups(check_fragmented=True)
    assert len(groups) == 1


def test_long_hf_slicing(long_case):
    """slice_groups() produces the expected per-group file counts for 10-year, 5-year, and offset slicing."""
    input_head_dir, output_head_dir = long_case
    hf_collection = HFCollection(input_head_dir)
    assert len(hf_collection) == LONG_TEST_NUM_HIST_FILES

    hf_coll1 = hf_collection.slice_groups(slice_size_years=10, start_year=0)
    groups = hf_coll1.get_groups()
    for group in groups:
        assert len(groups[group]) == 120

    hf_coll1 = hf_collection.slice_groups(slice_size_years=5, start_year=0)
    groups = hf_coll1.get_groups()
    for group in groups:
        assert len(groups[group]) == 60

    hf_coll1 = hf_collection.slice_groups(slice_size_years=5, start_year=1)
    groups = hf_coll1.get_groups()
    assert len(groups[list(groups)[0]]) == 12
    for index in range(1, len(groups)-1):
        assert len(groups[list(groups)[index]]) == 60
    assert len(groups[list(groups)[-1]]) == 48

    offset = 2
    hf_coll1 = hf_collection.include_years(CASE_START_YEAR+offset, CASE_START_YEAR+offset+9)
    assert len(hf_coll1) == 120
    hf_coll1 = hf_coll1.slice_groups(slice_size_years=10, start_year=None)
    groups = hf_coll1.get_groups()
    assert len(groups) == 1
    assert len(groups[list(groups)[0]]) == 120

    hf_coll1 = hf_coll1.slice_groups(slice_size_years=10, start_year=CASE_START_YEAR)
    groups = hf_coll1.get_groups()
    assert len(groups) == 2
    assert len(groups[list(groups)[0]]) == 120 - (offset*12)


def test_multistep_hf_slicing(multistep_large_case):
    """slice_groups() produces the expected per-group file counts for history files with a large number of time steps."""
    input_head_dir, output_head_dir = multistep_large_case
    hf_collection = HFCollection(input_head_dir)
    assert len(hf_collection) == MS_LARGE_NUM_TEST_HIST_FILES

    with pytest.raises(KeyError) as exc:
        hf_collection.get_multistep_slices("non-existent history file.nc")
    assert hf_collection.get_multistep_slices(list(hf_collection)[0]) is None

    hf_coll1 = hf_collection.slice_groups(slice_size_years=1, start_year=None)
    assert len(hf_coll1.get_groups()) == int(np.ceil(MS_LARGE_NUM_TEST_HIST_FILES*MS_LARGE_NUM_TIMESTEPS / 12))
    for hf_path in hf_coll1:
        assert hf_coll1.get_multistep_slices(hf_path) is not None

    hf_coll1 = hf_collection.slice_groups(slice_size_years=2, start_year=None)
    assert len(hf_coll1.get_groups()) == int(np.ceil(MS_LARGE_NUM_TEST_HIST_FILES*MS_LARGE_NUM_TIMESTEPS / 24))
    for hf_path in hf_coll1:
        assert hf_coll1.get_multistep_slices(hf_path) is not None


def test_slice_groups_time_alignment(simple_case):
    """slice_groups() accepts each valid time_alignment_method, conserving all files, and rejects invalid ones."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)

    for method in ["direct_time", "midpoint", "start_bound", "end_bound"]:
        groups = hf_collection.slice_groups(slice_size_years=1, time_alignment_method=method).get_groups()
        assert sum(len(paths) for paths in groups.values()) == SIMPLE_NUM_TEST_HIST_FILES

    with pytest.raises(ValueError, match="invalid time-alignment method"):
        hf_collection.slice_groups(slice_size_years=1, time_alignment_method="not_a_method")


def test_include_years(long_case):
    """include_years() returns the correct number of files for single- and multi-year ranges."""
    input_head_dir, output_head_dir = long_case
    hf_collection = HFCollection(input_head_dir)
    assert len(hf_collection.include_years(CASE_START_YEAR, CASE_START_YEAR)) == 12
    assert len(hf_collection.include_years(CASE_START_YEAR, CASE_START_YEAR+1)) == 24


def test_include_filter(structured_case):
    """include() and exclude() with single and multi-pattern globs correctly retain or remove matching paths."""
    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)

    glob1 = "*/0_dir/0_subdir/*"
    glob2 = "*/1_dir/0_subdir/*"
    glob1_match = False
    glob2_match = False
    for path in hf_collection:
        if fnmatch.fnmatch(str(path), glob1):
            glob1_match = True
        if fnmatch.fnmatch(str(path), glob2):
            glob2_match = True
    assert glob1_match and glob2_match

    glob1_match = False
    glob2_match = False
    for path in hf_collection.include(["*/0_dir/*"]):
        if fnmatch.fnmatch(str(path), glob1):
            glob1_match = True
        if fnmatch.fnmatch(str(path), glob2):
            glob2_match = True
    assert glob1_match and not glob2_match

    glob1_match = False
    glob2_match = False
    for path in hf_collection.include(["*/0_dir/*", "*/1_dir/*"]):
        if fnmatch.fnmatch(str(path), glob1):
            glob1_match = True
        if fnmatch.fnmatch(str(path), glob2):
            glob2_match = True
    assert glob1_match and glob2_match

    glob1_match = False
    glob2_match = False
    for path in hf_collection.exclude(["*/0_dir/*"]):
        if fnmatch.fnmatch(str(path), glob1):
            glob1_match = True
        if fnmatch.fnmatch(str(path), glob2):
            glob2_match = True
    assert not glob1_match and glob2_match

    glob1_match = False
    glob2_match = False
    for path in hf_collection.exclude(["*/0_dir/*", "*/1_dir/*"]):
        if fnmatch.fnmatch(str(path), glob1):
            glob1_match = True
        if fnmatch.fnmatch(str(path), glob1):
            glob2_match = True
    assert not glob1_match and not glob2_match


def test_dask_deprecation_warning(simple_case):
    """Passing dask_client=True to HFCollection raises a DeprecationWarning."""
    input_head_dir, output_head_dir = simple_case

    with pytest.warns(DeprecationWarning):
        hf_collection = HFCollection(input_head_dir, dask_client=True)


def test_slicing_auxiliary(auxiliary_only_case):
    """Auxiliary history files should not raise an error when slicing."""
    input_head_dir, output_head_dir = auxiliary_only_case
    hf_collection = HFCollection(input_head_dir).slice_groups(slice_size_years=1)


def test_spatially_fragmented_handling(spatial_fragment_case):
    """Spatial tile files are properly handled when slicing."""
    input_head_dir, output_head_dir = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection = hf_collection.slice_groups()
    assert len(hf_collection.get_groups(check_fragmented=True)) == 1


def _reference_timestep_delta_map(hf_collection):
    """Original (pre-optimization) timestep-delta computation, kept as a
    behavioral oracle: for each group, sort every cftime value in the group and
    take the gap between the two latest steps."""
    groups = hf_collection.get_groups()
    reference = {}
    for group in groups:
        times = []
        for path in groups[group]:
            cftimes = hf_collection[path].get_cftimes()
            if isinstance(cftimes, (list, np.ndarray)):
                for ts in cftimes:
                    times.append(ts)
            else:
                times.append(cftimes)
        times = np.sort(times)
        for path in groups[group]:
            reference[path] = times[-1] - times[-2]
    return reference


def test_get_timestep_delta_matches_reference(simple_case, multistep_large_case,
                                              spatial_fragment_case, mixed_timestep_case):
    """The optimized timestep-delta computation reproduces the original full-sort
    result exactly across single-step, multi-step, spatially-fragmented (duplicate
    timesteps), and mixed-frequency groups."""
    from datetime import timedelta

    for case in (simple_case, multistep_large_case, spatial_fragment_case, mixed_timestep_case):
        input_head_dir, _ = case
        hf_collection = HFCollection(input_head_dir)
        hf_collection.pull_metadata()

        reference = _reference_timestep_delta_map(hf_collection)
        assert len(reference) > 0
        for path in reference:
            assert hf_collection.get_timestep_delta(path) == reference[path]

    # Concrete anchor: 49 consecutive monthly (30-day) files -> 30-day step.
    input_head_dir, _ = simple_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    for path in hf_collection:
        assert hf_collection.get_timestep_delta(path) == timedelta(days=30)

    # Spatially fragmented tiles share every timestep, so the two latest values
    # are identical -> a zero-length step (a property the optimization preserves).
    input_head_dir, _ = spatial_fragment_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata()
    for path in hf_collection:
        assert hf_collection.get_timestep_delta(path) == timedelta(0)


def test_no_history_files():
    """No history files found should raise an error."""
    with pytest.raises(FileNotFoundError) as exc:
        empty_hfcollection = HFCollection("")


def test_extraneous_hfcollection(extraneous_file_case):
    """An extraneous history file should raise a ValueError naming the offending file."""
    input_head_dir, output_head_dir = extraneous_file_case
    hf_collection = HFCollection(input_head_dir)
    with pytest.raises(ValueError, match="extraneous.nc"):
        hf_collection.slice_groups()

def test_pull_metadata_decodes_no_full_time_arrays(multistep_large_case):
    """pull_metadata() (including the group timestep-delta computation) never decodes a
    full per-file time array -- at most the two endpoint candidates per file."""
    from unittest.mock import patch
    from cftime import num2date as real_num2date
    input_head_dir, _ = multistep_large_case

    with patch("gents.meta.num2date", wraps=real_num2date) as mock_num2date:
        hf_collection = HFCollection(input_head_dir)
        hf_collection.pull_metadata(show_progress=False)
        for call in mock_num2date.call_args_list:
            values = np.atleast_1d(np.asarray(call.args[0]))
            assert values.size <= 2


def test_get_year_bounds_multistep(multistep_large_case):
    """get_year_bounds() reports the midpoint-year span of a multi-step group."""
    input_head_dir, _ = multistep_large_case
    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata(show_progress=False)
    meta_map = {path: hf_collection[path] for path in hf_collection}
    min_year, max_year = get_year_bounds(meta_map)
    # 4 files x 15 monthly steps = 60 months = 5 years starting at CASE_START_YEAR.
    assert min_year == CASE_START_YEAR
    assert max_year == CASE_START_YEAR + 4


def test_slice_groups_no_full_time_decoding(multistep_large_case):
    """slice_groups() works in the raw float time domain -- no per-step cftime decode."""
    from unittest.mock import patch
    from cftime import num2date as real_num2date
    input_head_dir, _ = multistep_large_case

    hf_collection = HFCollection(input_head_dir)
    hf_collection.pull_metadata(show_progress=False)
    with patch("gents.meta.num2date", wraps=real_num2date) as mock_num2date:
        sliced = hf_collection.slice_groups(slice_size_years=2, start_year=None)
        for call in mock_num2date.call_args_list:
            values = np.atleast_1d(np.asarray(call.args[0]))
            assert values.size <= 2
    # The slicing itself must still produce multiple year windows.
    sliced_keys = [key for key in sliced.get_groups() if "[sorting_pivot]" in key]
    assert len(sliced_keys) >= 2


def test_pull_metadata_returns_self(simple_case):
    """pull_metadata() is fluent, not immutable: it returns the receiver itself, mutated in place."""
    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    assert not hf_collection.is_pulled()

    returned = hf_collection.pull_metadata(show_progress=False)

    assert returned is hf_collection
    assert hf_collection.is_pulled()
    # Contrast: the copy-returning transforms leave the receiver alone.
    assert hf_collection.exclude(["*.00001.nc"]) is not hf_collection


def test_include_years_glob_patterns_are_selective(long_case):
    """Files matching none of the glob patterns bypass the year filter entirely."""
    input_head_dir, output_head_dir = long_case
    hf_collection = HFCollection(input_head_dir)

    # The first 6 monthly files are the only ones the year filter may touch.
    selected = "*.0000[0-5].nc"
    filtered = hf_collection.include_years(CASE_START_YEAR, CASE_START_YEAR, glob_patterns=[selected])

    untouched = [path for path in hf_collection if not fnmatch.fnmatch(str(path), selected)]
    assert len(untouched) > 0
    for path in untouched:
        assert path in filtered

    # Every selected file here also falls in the kept year, so nothing is dropped.
    assert len(filtered) == len(hf_collection)

    # A year range excluding the selected files drops only those.
    dropped = hf_collection.include_years(CASE_START_YEAR + 10, CASE_START_YEAR + 10, glob_patterns=[selected])
    assert len(dropped) == len(hf_collection) - 6
    for path in untouched:
        assert path in dropped


def test_include_years_default_applies_to_every_file(long_case):
    """The default ['*'] leaves the year filter applying to the whole collection."""
    input_head_dir, output_head_dir = long_case
    hf_collection = HFCollection(input_head_dir)

    assert len(hf_collection.include_years(CASE_START_YEAR, CASE_START_YEAR)) == 12
