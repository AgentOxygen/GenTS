from gents.tests.test_cases import generate_history_file
from gents.validation.case_builder import clone_netcdf_with_missing
from netCDF4 import Dataset
from os.path import getsize
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


def _write_netcdf4_source(path, zlib=False, complevel=0):
    """Write a small NETCDF4 file with one primary field, optionally compressed."""
    with Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension("time", None)
        ds.createDimension("x", 8)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [15.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable(
            "FIELD", np.float32, ("time", "x"),
            zlib=zlib, complevel=complevel, chunksizes=(1, 8),
        )
        var[:] = np.ones((1, 8), dtype=np.float32)


def test_forces_high_compression_by_default(tmp_path):
    """An uncompressed source is written with high zlib+shuffle compression."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "dst.nc")
    _write_netcdf4_source(src, zlib=False)

    clone_netcdf_with_missing(src, dst, complevel=9)

    with Dataset(dst) as d:
        for name in ("FIELD", "time"):
            filters = d.variables[name].filters()
            assert filters["zlib"] is True
            assert filters["complevel"] == 9
            assert filters["shuffle"] is True
        assert d.variables["FIELD"].chunking() == [1, 8]
        assert np.all(np.isnan(np.asarray(d.variables["FIELD"][:])))


def test_no_compress_mirrors_source_filters(tmp_path):
    """With compress=False the clone mirrors the source's own filter settings."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "dst.nc")
    _write_netcdf4_source(src, zlib=True, complevel=4)

    clone_netcdf_with_missing(src, dst, compress=False)

    with Dataset(dst) as d:
        filters = d.variables["FIELD"].filters()
        assert filters["zlib"] is True
        assert filters["complevel"] == 4  # mirrored, not overridden to 9


def test_netcdf3_source_is_not_compressed(tmp_path):
    """netCDF3 formats do not support zlib, so the clone stays uncompressed."""
    src = str(tmp_path / "src3.nc")
    dst = str(tmp_path / "dst3.nc")
    with Dataset(src, "w", format="NETCDF3_64BIT_OFFSET") as ds:
        ds.createDimension("time", None)
        ds.createDimension("x", 4)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [15.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable("FIELD", np.float32, ("time", "x"))
        var[:] = np.ones((1, 4), dtype=np.float32)

    clone_netcdf_with_missing(src, dst)  # must not raise

    with Dataset(dst) as d:
        assert d.file_format.startswith("NETCDF3")
        # netCDF3 variables carry no filters at all.
        assert d.variables["FIELD"].filters() is None
        assert np.all(np.isnan(np.asarray(d.variables["FIELD"][:])))


@pytest.mark.parametrize("src_zlib", [False, True], ids=["uncompressed", "compressed"])
def test_clone_is_much_smaller_than_source(tmp_path, src_zlib):
    """
    The clone is a fraction of the source size whether or not the source is
    itself compressed.

    The whole point of the tool is to shrink the on-disk data burden while
    keeping structure. Because high compression is forced on the clone, even an
    uncompressed source shrinks: the constant NaN-filled primaries compress away
    almost entirely.
    """
    src = str(tmp_path / "src_big.nc")
    dst = str(tmp_path / "dst_big.nc")

    # Random data resists compression, so the source stays large regardless.
    shape = (40, 90, 90)
    with Dataset(src, "w", format="NETCDF4") as ds:
        ds.createDimension("time", None)
        ds.createDimension("lat", shape[1])
        ds.createDimension("lon", shape[2])
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = np.arange(shape[0], dtype=np.double)
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable(
            "FIELD", np.float32, ("time", "lat", "lon"),
            zlib=src_zlib, complevel=4 if src_zlib else 0,
        )
        var[:] = np.random.random(shape).astype(np.float32)

    clone_netcdf_with_missing(src, dst)

    src_size = getsize(src)
    dst_size = getsize(dst)
    assert dst_size < src_size / 10, (
        f"clone ({dst_size} B) is not much smaller than source ({src_size} B)"
    )


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
