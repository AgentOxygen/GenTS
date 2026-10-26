from gents.tests.test_cases import *
from gents.meta import netCDFMeta, is_var_secondary, get_attributes, get_time_variables_names, get_meta_from_path, read_meta_map, split_valid
from gents.datastore import GenTSDataStore
from gents.hfcollection import HFCollection
import numpy as np
import pytest


def test_get_attributes(tmp_path):
    """get_attributes() returns a dict containing the global attributes written to the file."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    with GenTSDataStore(path, "r") as ds:
        attrs = get_attributes(ds)
    assert isinstance(attrs, dict)
    assert attrs.get("source") == "GenTS testing suite"


def test_get_time_variables_names_standard(tmp_path):
    """Standard 'time' and 'time_bounds' names are detected correctly."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]], time_name="time", time_bounds_name="time_bounds")
    with GenTSDataStore(path, "r") as ds:
        time_name, bnds_name = get_time_variables_names(ds)
    assert time_name == "time"
    assert bnds_name == "time_bounds"


def test_get_time_variables_names_case_insensitive(tmp_path):
    """Time variable names are matched case-insensitively."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]], time_name="Time", time_bounds_name="Time_Bounds")
    with GenTSDataStore(path, "r") as ds:
        time_name, bnds_name = get_time_variables_names(ds)
    assert time_name == "Time"
    assert bnds_name == "Time_Bounds"


def test_get_time_variables_names_no_bounds(tmp_path):
    """Returns None for the bounds name when no time-bounds variable is present."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None)
    with GenTSDataStore(path, "r") as ds:
        time_name, bnds_name = get_time_variables_names(ds)
    assert time_name == "time"
    assert bnds_name is None


def test_get_time_variables_names_no_time(tmp_path):
    """Returns None for both names when no recognisable time variable exists."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None, time_name="nottime")
    with GenTSDataStore(path, "r") as ds:
        time_name, bnds_name = get_time_variables_names(ds)
    assert time_name is None
    assert bnds_name is None


@pytest.fixture
def simple_meta(tmp_path):
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    with GenTSDataStore(path, "r") as ds:
        return netCDFMeta(ds, path), path


@pytest.fixture
def no_bounds_meta(tmp_path):
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None)
    with GenTSDataStore(path, "r") as ds:
        return netCDFMeta(ds, path), path

def test_netcdfmeta_get_path(simple_meta):
    """get_path() returns the path passed to the constructor."""
    meta, path = simple_meta
    assert meta.get_path() == path


def test_netcdfmeta_get_float_times(simple_meta):
    """get_float_times() returns a 1-D array with the correct time value."""
    meta, _ = simple_meta
    times = meta.get_float_times()
    assert len(times) == 1
    assert float(times[0]) == pytest.approx(15.0)


def test_netcdfmeta_get_cftimes(simple_meta):
    """get_cftimes() returns an array of CFTime objects of the same length as the time axis."""
    meta, _ = simple_meta
    cftimes = meta.get_cftimes()
    assert len(cftimes) == 1


def test_netcdfmeta_get_float_time_bounds(simple_meta):
    """get_float_time_bounds() returns a (1, 2) array with the correct boundary values."""
    meta, _ = simple_meta
    bounds = meta.get_float_time_bounds()
    assert bounds is not None
    assert bounds.shape == (1, 2)
    assert float(bounds[0, 0]) == pytest.approx(0.0)
    assert float(bounds[0, 1]) == pytest.approx(30.0)


def test_netcdfmeta_get_float_time_bounds_none(no_bounds_meta):
    """get_float_time_bounds() returns None when the file has no time-bounds variable."""
    meta, _ = no_bounds_meta
    assert meta.get_float_time_bounds() is None


def test_netcdfmeta_get_cftime_bounds(simple_meta):
    """get_cftime_bounds() returns a (1, 2) array of CFTime objects."""
    meta, _ = simple_meta
    bounds = meta.get_cftime_bounds()
    assert bounds is not None
    assert bounds.shape == (1, 2)


def test_netcdfmeta_get_cftime_bounds_none(no_bounds_meta):
    """get_cftime_bounds() returns None when the file has no time-bounds variable."""
    meta, _ = no_bounds_meta
    assert meta.get_cftime_bounds() is None


def test_is_var_secondary_case_insensitive(tmp_path):
    """Capitalised time dim / bounds names are classified like their lowercase forms."""
    path = str(tmp_path / "mom.nc")
    with GenTSDataStore(path, "w", format="NETCDF4") as ds:
        ds.createDimension("Time", None)
        ds.createDimension("Layer", 3)
        ds.createDimension("lath", 4)
        ds.createDimension("lonh", 5)
        ds.createDimension("nbnd", 2)
        ds.createVariable("Temp", np.double, ("Time", "Layer", "lath", "lonh"))
        ds.createVariable("lath", np.double, ("lath",))
        ds.createVariable("Time_Bounds", np.double, ("Time", "nbnd"))
    with GenTSDataStore(path, "r") as ds:
        assert is_var_secondary(ds["Temp"]) is False        # primary despite 'Time'
        assert is_var_secondary(ds["lath"]) is True          # 1-D coordinate
        assert is_var_secondary(ds["Time_Bounds"]) is True   # bounds name, capitalised


def test_netcdfmeta_get_variables(simple_meta):
    """get_variables() lists all variable names including time, bounds, and primary fields."""
    meta, _ = simple_meta
    variables = meta.get_variables()
    assert isinstance(variables, list)
    assert "time" in variables
    assert "time_bounds" in variables
    for i in range(SIMPLE_NUM_VARS):
        assert f"VAR{i}" in variables


def test_netcdfmeta_get_primary_variables(simple_meta):
    """get_primary_variables() contains VAR* fields and excludes coordinate variables."""
    meta, _ = simple_meta
    primary = meta.get_primary_variables()
    assert isinstance(primary, list)
    for i in range(SIMPLE_NUM_VARS):
        assert f"VAR{i}" in primary
    assert "time" not in primary
    assert "time_bounds" not in primary


def test_netcdfmeta_get_secondary_variables(simple_meta):
    """get_secondary_variables() contains time/time_bounds and excludes primary fields."""
    meta, _ = simple_meta
    secondary = meta.get_secondary_variables()
    assert isinstance(secondary, list)
    assert "time" in secondary
    assert "time_bounds" in secondary
    for i in range(SIMPLE_NUM_VARS):
        assert f"VAR{i}" not in secondary


def test_netcdfmeta_variable_partition_covers_all(simple_meta):
    """Primary and secondary variable lists together cover exactly the full variable list."""
    meta, _ = simple_meta
    all_vars = set(meta.get_variables())
    partitioned = set(meta.get_primary_variables()) | set(meta.get_secondary_variables())
    assert all_vars == partitioned


def test_netcdfmeta_get_variable_dims(simple_meta):
    """get_variable_dims() returns the correct dimension names for a primary variable."""
    meta, _ = simple_meta
    dims = meta.get_variable_dims("VAR0")
    assert "time" in dims
    assert "lat" in dims
    assert "lon" in dims


def test_netcdfmeta_get_variable_shapes(simple_meta):
    """get_variable_shapes() returns the correct (time, lat, lon) shape for a primary variable."""
    meta, _ = simple_meta
    shape = meta.get_variable_shapes("VAR0")
    assert shape == (1, 3, 4)


def test_netcdfmeta_get_variable_dtype(simple_meta):
    """get_variable_dtype() returns the correct data type for a primary variable."""
    meta, _ = simple_meta
    dtype = meta.get_variable_dtype("VAR0")
    assert dtype == float


def test_netcdfmeta_get_attributes(simple_meta):
    """get_attributes() returns the global attributes dict including the 'source' key."""
    meta, _ = simple_meta
    attrs = meta.get_attributes()
    assert isinstance(attrs, dict)
    assert "source" in attrs


def test_netcdfmeta_is_valid(simple_meta):
    """is_valid() returns True for a well-formed history file."""
    meta, _ = simple_meta
    assert meta.is_valid() is True


def test_netcdfmeta_get_dim_bounds(tmp_path):
    """get_dim_bounds() maps each coordinate dimension to its [min, max] values."""
    path = str(tmp_path / "test.nc")
    lat_vals = np.linspace(-90, 90, 3)
    lon_vals = np.linspace(-180, 180, 4)
    generate_history_file(path, [15.0], [[0.0, 30.0]], dim_vals={"lat": lat_vals, "lon": lon_vals})
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path)
    bounds = meta.get_dim_bounds()
    assert "lat" in bounds
    assert "lon" in bounds
    assert bounds["lat"] == pytest.approx([-90.0, 90.0])
    assert bounds["lon"] == pytest.approx([-180.0, 180.0])


def test_netcdfmeta_raises_on_no_time(tmp_path):
    """netCDFMeta raises ValueError when no recognisable time variable is present."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None, time_name="nottime")
    with GenTSDataStore(path, "r") as ds:
        with pytest.raises(ValueError, match="No equivalent time variable"):
            netCDFMeta(ds, path)


@pytest.fixture
def lazy_meta(tmp_path):
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    with GenTSDataStore(path, "r") as ds:
        return netCDFMeta(ds, path, decode_dates=False), path


def test_netcdfmeta_decode_dates_false_defers_and_still_decodes(lazy_meta):
    """decode_dates=False defers CFTime conversion, but get_cftimes()/get_cftime_bounds() still decode correctly on demand, after the source file has closed."""
    meta, _ = lazy_meta
    cftimes = meta.get_cftimes()
    assert len(cftimes) == 1
    bounds = meta.get_cftime_bounds()
    assert bounds is not None
    assert bounds.shape == (1, 2)


def test_netcdfmeta_decode_dates_false_no_bounds(tmp_path):
    """decode_dates=False with no time-bounds variable: get_cftime_bounds() still returns None rather than raising."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None)
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, decode_dates=False)
    assert meta.get_cftime_bounds() is None


def test_netcdfmeta_decode_dates_false_is_valid(lazy_meta):
    """is_valid() still works correctly (triggering the lazy decode internally) when constructed with decode_dates=False."""
    meta, _ = lazy_meta
    assert meta.is_valid() is True


def test_netcdfmeta_load_time_bounds_false_raises(tmp_path):
    """load_time_bounds=False raises on get_float_time_bounds()/get_cftime_bounds() if the file actually has a time-bounds variable."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, load_time_bounds=False)
    with pytest.raises(RuntimeError, match="load_time_bounds"):
        meta.get_float_time_bounds()
    with pytest.raises(RuntimeError, match="load_time_bounds"):
        meta.get_cftime_bounds()


def test_netcdfmeta_load_time_bounds_false_no_bounds_in_file(tmp_path):
    """load_time_bounds=False on a file with no time-bounds variable at all still returns None, not a raise -- nothing was actually skipped."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], None)
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, load_time_bounds=False)
    assert meta.get_float_time_bounds() is None
    assert meta.get_cftime_bounds() is None


def test_netcdfmeta_load_variable_attrs_false_raises(simple_meta):
    """load_variable_attrs=False raises on get_variable_attrs()."""
    path = str(simple_meta[1])
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, load_variable_attrs=False)
    with pytest.raises(RuntimeError, match="load_variable_attrs"):
        meta.get_variable_attrs("VAR0")


def test_netcdfmeta_load_variable_attrs_true_default(simple_meta):
    """load_variable_attrs defaults to True, matching prior behavior."""
    meta, _ = simple_meta
    attrs = meta.get_variable_attrs("VAR0")
    assert attrs.get("standard_name") == "VAR0"


def test_netcdfmeta_compute_dim_bounds_false_raises(tmp_path):
    """compute_dim_bounds=False raises on get_dim_bounds()."""
    path = str(tmp_path / "test.nc")
    lat_vals = np.linspace(-90, 90, 3)
    lon_vals = np.linspace(-180, 180, 4)
    generate_history_file(path, [15.0], [[0.0, 30.0]], dim_vals={"lat": lat_vals, "lon": lon_vals})
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, compute_dim_bounds=False)
    with pytest.raises(RuntimeError, match="compute_dim_bounds"):
        meta.get_dim_bounds()


def test_get_meta_from_path(tmp_path):
    """get_meta_from_path() returns a populated netCDFMeta with the correct path."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    meta = get_meta_from_path(path)
    assert isinstance(meta, netCDFMeta)
    assert meta.get_path() == path


def test_get_meta_from_path_raises_with_path(tmp_path):
    """get_meta_from_path() re-raises exceptions with the file path appended to the message."""
    path = str(tmp_path / "bad.nc")
    generate_history_file(path, [15.0], None, time_name="nottime")
    with pytest.raises(ValueError, match=str(path)):
        get_meta_from_path(path)


def test_netcdfmeta_get_time_units_and_calendar(simple_meta):
    """get_time_units()/get_time_calendar() return the time variable's reference attributes."""
    meta, _ = simple_meta
    assert meta.get_time_units() == f"days since {CASE_START_YEAR}-01-01"
    assert meta.get_time_calendar() == "360_day"


def test_netcdfmeta_decode_time_values(simple_meta):
    """decode_time_values() decodes raw floats identically to get_cftimes()."""
    meta, _ = simple_meta
    decoded = meta.decode_time_values(np.array([15.0]))
    assert decoded[0] == meta.get_cftimes()[0]


def test_netcdfmeta_decode_time_values_lazy(lazy_meta):
    """decode_time_values() works on a decode_dates=False meta without triggering the full decode."""
    from unittest.mock import patch
    from cftime import num2date as real_num2date
    meta, _ = lazy_meta
    with patch("gents.meta.num2date", wraps=real_num2date) as mock_num2date:
        decoded = meta.decode_time_values(np.array([15.0, 45.0]))
        # One call for the two requested values -- not a full-array decode.
        assert mock_num2date.call_count == 1
    assert decoded[0].year == CASE_START_YEAR
    assert decoded[1].month == 2


def test_netcdfmeta_decode_time_bounds_values(simple_meta):
    """decode_time_bounds_values() decodes raw floats identically to get_cftime_bounds()."""
    meta, _ = simple_meta
    decoded = meta.decode_time_bounds_values(np.array([0.0, 30.0]))
    bounds = meta.get_cftime_bounds()
    assert decoded[0] == bounds[0][0]
    assert decoded[1] == bounds[0][1]


def test_netcdfmeta_decode_time_bounds_values_no_bounds_raises(no_bounds_meta):
    """decode_time_bounds_values() raises when the file has no time-bounds variable to take a reference from."""
    meta, _ = no_bounds_meta
    with pytest.raises(RuntimeError, match="time-bounds"):
        meta.decode_time_bounds_values(np.array([0.0, 30.0]))


def test_get_meta_from_path_defers_decoding(tmp_path):
    """get_meta_from_path() builds the meta without decoding any dates; decoding still works on demand afterwards."""
    from unittest.mock import patch
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0, 45.0], [[0.0, 30.0], [30.0, 60.0]])
    with patch("gents.meta.num2date") as mock_num2date:
        meta = get_meta_from_path(path)
        mock_num2date.assert_not_called()
    assert len(meta.get_cftimes()) == 2


def test_get_meta_from_path_skips_variable_attrs(tmp_path):
    """get_meta_from_path() leaves per-variable attrs unread: nothing reads them from
    collection metadata, and holding them for every file dominates the parent's memory."""
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    meta = get_meta_from_path(path)
    with pytest.raises(RuntimeError, match="load_variable_attrs"):
        meta.get_variable_attrs("VAR0")


def test_netcdfmeta_is_valid_does_not_decode(tmp_path):
    """is_valid() decides from raw values alone, never triggering a cftime decode."""
    from unittest.mock import patch
    path = str(tmp_path / "test.nc")
    generate_history_file(path, [15.0], [[0.0, 30.0]])
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, decode_dates=False)
    with patch("gents.meta.num2date") as mock_num2date:
        assert meta.is_valid() is True
        mock_num2date.assert_not_called()


@pytest.mark.parametrize("num_processes", [1, 2])
def test_read_meta_map(tmp_path, num_processes):
    """read_meta_map() returns metadata for loadable files and omits (not raises on) failures, serially and in a pool."""
    good = tmp_path / "good.nc"
    generate_history_file(str(good), [15.0], [[0.0, 30.0]])
    missing = tmp_path / "missing.nc"

    loaded = read_meta_map([good, missing], get_meta_from_path, num_processes=num_processes, show_progress=False)
    assert list(loaded) == [good]
    assert isinstance(loaded[good], netCDFMeta)

    with pytest.raises(Exception):
        read_meta_map([missing], get_meta_from_path, num_processes=num_processes, raise_errors=True, show_progress=False)


def test_split_valid():
    """split_valid() keeps entries the predicate accepts and removes both rejected and None entries."""
    meta_map = {"a": 1, "b": 2, "c": None}
    kept, removed = split_valid(meta_map, lambda meta: meta == 1)
    assert kept == {"a": 1}
    assert removed == {"b": 2, "c": None}
    assert meta_map == {"a": 1, "b": 2, "c": None}


@pytest.mark.parametrize("bounds", [[[0.0, 30.0], [30.0, 60.0]], None])
@pytest.mark.parametrize("flags", [{}, {"load_variable_attrs": False}, {"compute_dim_bounds": False},
                                   {"load_time_bounds": False}])
def test_netcdfmeta_dict_round_trip(tmp_path, bounds, flags):
    """from_dict(json(to_dict())) matches the original in every field, so a new attribute can't be forgotten."""
    import json
    path = str(tmp_path / "test.nc")
    dim_vals = {"lat": np.linspace(-90, 90, 3), "lon": np.linspace(-180, 180, 4)}
    generate_history_file(path, [15.0, 45.0], bounds, dim_vals=dim_vals)
    with GenTSDataStore(path, "r") as ds:
        meta = netCDFMeta(ds, path, decode_dates=False, **flags)

    rebuilt = netCDFMeta.from_dict(json.loads(json.dumps(meta.to_dict())))

    assert rebuilt.to_dict() == meta.to_dict()
    assert rebuilt.get_path() == path
    assert rebuilt.get_variables() == meta.get_variables()
    assert rebuilt.get_primary_variables() == meta.get_primary_variables()
    assert rebuilt.get_secondary_variables() == meta.get_secondary_variables()
    assert rebuilt.get_attributes() == meta.get_attributes()
    assert rebuilt.get_float_times().dtype == meta.get_float_times().dtype == np.float64
    assert not np.ma.isMaskedArray(meta.get_float_times())
    np.testing.assert_array_equal(rebuilt.get_float_times(), meta.get_float_times())
    assert list(rebuilt.get_cftimes()) == list(meta.get_cftimes())
    if "load_time_bounds" not in flags or bounds is None:  # is_valid() reads bounds, which raises when not loaded
        assert rebuilt.is_valid() == meta.is_valid()
    for var in meta.get_variables():
        assert rebuilt.get_variable_shapes(var) == meta.get_variable_shapes(var)
        assert rebuilt.get_variable_dims(var) == meta.get_variable_dims(var)
        assert rebuilt.get_variable_dtype(var) == meta.get_variable_dtype(var)
    # Flag-gated getters must behave identically: values, or the same RuntimeError.
    for getter in ("get_float_time_bounds", "get_dim_bounds"):
        try:
            expected = getattr(meta, getter)()
        except RuntimeError:
            with pytest.raises(RuntimeError):
                getattr(rebuilt, getter)()
        else:
            assert str(getattr(rebuilt, getter)()) == str(expected)
    if flags.get("load_variable_attrs", True):
        for var in meta.get_variables():
            assert rebuilt.get_variable_attrs(var).keys() == meta.get_variable_attrs(var).keys()
    else:
        with pytest.raises(RuntimeError):
            rebuilt.get_variable_attrs(meta.get_variables()[0])


def test_encode_decode_numpy_values():
    """Numpy scalars, arrays and str dtype survive the codec with dtype intact."""
    from gents.meta import _encode, _decode
    scalar = _decode(_encode(np.float32(1.5)))
    assert scalar == np.float32(1.5) and scalar.dtype == np.float32
    f32 = _decode(_encode(np.array([0.1], dtype="f4")))
    assert f32.dtype == np.float32 and str(f32) == str(np.array([0.1], dtype="f4"))
    assert _decode(_encode("text")) == "text"


def test_manifest_round_trip_hfcollection(simple_case):
    """An HFCollection's metas survive save/load and match the originals field for field."""
    from gents.meta import save_meta_manifest, load_meta_manifest
    hf_dir, ts_dir = simple_case
    hfc = HFCollection(str(hf_dir)).pull_metadata()
    manifest = ts_dir / "manifest.json"
    save_meta_manifest(dict(hfc.items()), manifest, "hf")

    loaded = load_meta_manifest(manifest, "hf")
    assert set(loaded) == set(hfc.keys())
    for path, meta in hfc.items():
        assert loaded[path].to_dict() == meta.to_dict()


def test_manifest_drops_changed_and_deleted_files(simple_case):
    """Entries whose size/mtime changed, or whose file is gone, are not returned."""
    import os
    from gents.meta import save_meta_manifest, load_meta_manifest
    hf_dir, ts_dir = simple_case
    hfc = HFCollection(str(hf_dir)).pull_metadata()
    manifest = ts_dir / "manifest.json"
    save_meta_manifest(dict(hfc.items()), manifest, "hf")

    paths = sorted(hfc.keys())
    st = os.stat(paths[0])
    os.utime(paths[0], ns=(st.st_atime_ns, st.st_mtime_ns + 1))
    paths[1].unlink()

    assert set(load_meta_manifest(manifest, "hf")) == set(paths[2:])


def test_manifest_unusable_gives_empty(tmp_path):
    """Missing, corrupt and wrong-version manifests give {} instead of raising."""
    from gents.meta import load_meta_manifest
    assert load_meta_manifest(tmp_path / "absent.json", "hf") == {}
    (tmp_path / "bad.json").write_text("{not json")
    assert load_meta_manifest(tmp_path / "bad.json", "hf") == {}
    (tmp_path / "old.json").write_text('{"version": 0, "entries": {}}')
    assert load_meta_manifest(tmp_path / "old.json", "hf") == {}


def test_manifest_skips_unpulled_and_leaves_no_temp_file(simple_case):
    """None metas are skipped, and the temp file is renamed away."""
    from gents.meta import save_meta_manifest, load_meta_manifest
    hf_dir, ts_dir = simple_case
    hfc = HFCollection(str(hf_dir)).pull_metadata()
    manifest = ts_dir / "manifest.json"
    save_meta_manifest({**dict(hfc.items()), hf_dir / "unpulled.nc": None}, manifest, "hf")
    assert len(load_meta_manifest(manifest, "hf")) == len(hfc)
    assert not (ts_dir / "manifest.json.tmp").exists()


def test_netcdfmeta_stat_taken_at_read_time(tmp_path):
    """The stat is recorded when the metadata is read, so a later edit to the file can't be mistaken for current."""
    import os
    from gents.meta import save_meta_manifest, load_meta_manifest
    path = tmp_path / "test.nc"
    generate_history_file(str(path), [15.0, 45.0], [[0.0, 30.0], [30.0, 60.0]])
    with GenTSDataStore(str(path), "r") as ds:
        meta = netCDFMeta(ds, str(path))
    assert meta.get_stat() == {"size": os.stat(path).st_size, "mtime_ns": os.stat(path).st_mtime_ns}

    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1))  # file changes between read and save
    save_meta_manifest({path: meta}, tmp_path / "manifest.json", "hf")
    assert load_meta_manifest(tmp_path / "manifest.json", "hf") == {}


def test_manifest_kind_mismatch_raises(simple_case):
    """A manifest of another kind (or none) raises instead of being ignored or overwritten."""
    from gents.meta import save_meta_manifest, load_meta_manifest
    hf_dir, ts_dir = simple_case
    hfc = HFCollection(str(hf_dir)).pull_metadata()
    manifest = ts_dir / "manifest.json"
    save_meta_manifest(dict(hfc.items()), manifest, "hf")
    before = manifest.read_text()

    with pytest.raises(ValueError, match="'hf' metadata but 'ts' was requested"):
        load_meta_manifest(manifest, "ts")
    assert manifest.read_text() == before  # load never modifies the file

    unlabelled = ts_dir / "unlabelled.json"
    unlabelled.write_text('{"version": 1, "entries": {}}')
    with pytest.raises(ValueError):
        load_meta_manifest(unlabelled, "hf")
