from gents.tests.test_cases import *
from gents.meta import netCDFMeta, is_var_secondary, get_attributes, get_time_variables_names, get_meta_from_path
from gents.datastore import GenTSDataStore
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
