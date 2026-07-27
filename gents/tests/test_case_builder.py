from gents.tests.test_cases import generate_history_file
from gents.validation.case_builder import clone_netcdf_with_missing
from netCDF4 import Dataset
import numpy as np
import pytest


def _clone(tmp_path, **gen_kwargs):
    """Generate a synthetic history file, clone it with missing values, return both paths."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "sub" / "dst.nc")
    generate_history_file(src, [15.0], [[0.0, 30.0]], **gen_kwargs)
    clone_netcdf_with_missing(src, dst)
    return src, dst


def test_creates_missing_parent_directories(tmp_path):
    """The clone is written even when its parent directory does not yet exist."""
    src, dst = _clone(tmp_path)
    assert (tmp_path / "sub" / "dst.nc").is_file()


def test_structure_matches_source(tmp_path):
    """Dimensions, variables, dtypes and unlimited flags are identical to the source."""
    src, dst = _clone(tmp_path)
    with Dataset(src) as s, Dataset(dst) as d:
        assert d.dimensions.keys() == s.dimensions.keys()
        for name, sdim in s.dimensions.items():
            ddim = d.dimensions[name]
            assert ddim.isunlimited() == sdim.isunlimited()
            assert len(ddim) == len(sdim)

        assert d.variables.keys() == s.variables.keys()
        for name, svar in s.variables.items():
            dvar = d.variables[name]
            assert dvar.dtype == svar.dtype
            assert dvar.dimensions == svar.dimensions


def test_primary_variables_filled_with_nan(tmp_path):
    """Every primary (time-varying, multi-dim) field is entirely NaN in the clone."""
    src, dst = _clone(tmp_path)
    with Dataset(dst) as d:
        for name, var in d.variables.items():
            if name.startswith("VAR"):
                assert np.all(np.isnan(np.asarray(var[:])))


def test_secondary_variables_copied_verbatim(tmp_path):
    """Coordinate/time/bounds variables retain their exact source values."""
    src, dst = _clone(tmp_path)
    with Dataset(src) as s, Dataset(dst) as d:
        for name in ("time", "time_bounds"):
            assert np.array_equal(np.asarray(s[name][:]), np.asarray(d[name][:]))


def test_global_attributes_preserved(tmp_path):
    """Global attributes are copied onto the clone."""
    src, dst = _clone(tmp_path)
    with Dataset(dst) as d:
        assert d.getncattr("source") == "GenTS testing suite"


def test_variable_attributes_preserved(tmp_path):
    """Per-variable attributes are copied onto the clone."""
    src, dst = _clone(tmp_path)
    with Dataset(dst) as d:
        var = d.variables["VAR0"]
        assert var.getncattr("units") == "kg/g/m^2/K"
        assert var.getncattr("long_name") == "variable_0"


def test_integer_primary_filled_with_fill_value(tmp_path):
    """Integer primary variables are filled with their ``_FillValue``."""
    src = str(tmp_path / "src_int.nc")
    dst = str(tmp_path / "dst_int.nc")
    fill = np.int32(-999)
    with Dataset(src, "w", format="NETCDF4_CLASSIC") as ds:
        ds.createDimension("time", None)
        ds.createDimension("lat", 3)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [15.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        ivar = ds.createVariable("COUNTS", np.int32, ("time", "lat"), fill_value=fill)
        ivar[:] = np.arange(3, dtype=np.int32)

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        counts = d.variables["COUNTS"]
        assert counts.dtype == np.int32
        assert counts._FillValue == fill
        assert np.all(np.asarray(counts[:]) == fill)


def test_compression_and_chunking_preserved(tmp_path):
    """Compression settings and chunk sizes survive the clone."""
    src = str(tmp_path / "src_zip.nc")
    dst = str(tmp_path / "dst_zip.nc")
    with Dataset(src, "w", format="NETCDF4") as ds:
        ds.createDimension("time", None)
        ds.createDimension("x", 8)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [15.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable(
            "FIELD", np.float32, ("time", "x"),
            zlib=True, complevel=4, shuffle=True, chunksizes=(1, 8),
        )
        var[:] = np.ones((1, 8), dtype=np.float32)

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        field = d.variables["FIELD"]
        filters = field.filters()
        assert filters["zlib"] is True
        assert filters["complevel"] == 4
        assert filters["shuffle"] is True
        assert field.chunking() == [1, 8]
        assert np.all(np.isnan(np.asarray(field[:])))


def test_empty_variable_is_skipped(tmp_path):
    """A variable with zero elements is created but left empty without error."""
    src = str(tmp_path / "src_empty.nc")
    dst = str(tmp_path / "dst_empty.nc")
    with Dataset(src, "w", format="NETCDF4_CLASSIC") as ds:
        ds.createDimension("time", None)  # unlimited, length 0
        ds.createDimension("lat", 3)
        ds.createVariable("EMPTY", np.float32, ("time", "lat"))

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        assert d.variables["EMPTY"].size == 0
