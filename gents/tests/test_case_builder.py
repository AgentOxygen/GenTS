from gents.tests.test_cases import generate_history_file
from gents.conformity.case_builder import (
    clone_netcdf_with_missing,
    record_clone_command,
    CLONE_COMMAND_FILENAME,
    _is_valid_netcdf,
    _resolve_clone_jobs,
)
from netCDF4 import Dataset
from os.path import getsize
from pathlib import Path
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


def test_uncompressed_source_stays_uncompressed(tmp_path):
    """No compression is forced: an uncompressed source clones uncompressed."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "dst.nc")
    _write_netcdf4_source(src, zlib=False)

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        for name in ("FIELD", "time"):
            assert d.variables[name].filters()["zlib"] is False
        assert d.variables["FIELD"].chunking() == [1, 8]  # source chunking mirrored
        assert np.all(np.isnan(np.asarray(d.variables["FIELD"][:])))


def test_source_filters_are_mirrored(tmp_path):
    """A compressed source's own filter settings are mirrored exactly."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "dst.nc")
    _write_netcdf4_source(src, zlib=True, complevel=4)

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        filters = d.variables["FIELD"].filters()
        assert filters["zlib"] is True
        assert filters["complevel"] == 4  # mirrored, not overridden


def _write_cdf5_source(path):
    """Write a CDF-5 (NETCDF3_64BIT_DATA) file with a float and an int64 field."""
    with Dataset(path, "w", format="NETCDF3_64BIT_DATA") as ds:
        ds.createDimension("time", None)
        ds.createDimension("x", 4)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [15.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable("FIELD", np.float32, ("time", "x"))
        var[:] = np.ones((1, 4), dtype=np.float32)
        # int64 is a CDF-5 extended type the classic data model cannot hold.
        big = ds.createVariable("BIG", np.int64, ("time", "x"))
        big[:] = np.arange(4, dtype=np.int64)


def test_cdf5_source_upgraded_to_netcdf4(tmp_path):
    """A CDF-5 source is upgraded to NETCDF4 so its clone can shrink (no compression)."""
    src = str(tmp_path / "src5.nc")
    dst = str(tmp_path / "dst5.nc")
    _write_cdf5_source(src)

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        assert d.file_format == "NETCDF4"
        # No compression is forced; the upgrade is purely to get HDF5 lazy
        # allocation of the unwritten primaries.
        assert d.variables["FIELD"].filters()["zlib"] is False
        assert np.all(np.isnan(np.asarray(d.variables["FIELD"][:])))
        # int64 extended type survives the upgrade to NETCDF4.
        assert d.variables["BIG"].dtype == np.int64


def test_preserve_format_keeps_netcdf3(tmp_path):
    """With upgrade_netcdf3=False a netCDF3-model source keeps its original format."""
    src = str(tmp_path / "src5.nc")
    dst = str(tmp_path / "dst5.nc")
    _write_cdf5_source(src)

    clone_netcdf_with_missing(src, dst, upgrade_netcdf3=False)

    with Dataset(dst) as d:
        assert d.file_format == "NETCDF3_64BIT_DATA"
        assert d.variables["FIELD"].filters() is None  # no filters in netCDF3
        assert np.all(np.isnan(np.asarray(d.variables["FIELD"][:])))


@pytest.mark.parametrize("src_zlib", [False, True], ids=["uncompressed", "compressed"])
def test_clone_is_much_smaller_than_source(tmp_path, src_zlib):
    """
    The clone is a fraction of the source size whether or not the source is
    itself compressed.

    The whole point of the tool is to shrink the on-disk data burden while
    keeping structure. The primary field is left unwritten, so HDF5 allocates no
    storage for it regardless of the source's compression.
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


def _clone_size_with_levels(tmp_path, nlev):
    """Clone a source whose primary field has ``nlev`` levels; return clone bytes."""
    src = str(tmp_path / f"src_{nlev}.nc")
    dst = str(tmp_path / f"dst_{nlev}.nc")
    with Dataset(src, "w", format="NETCDF4") as ds:
        ds.createDimension("time", None)
        ds.createDimension("lev", nlev)
        ds.createDimension("ncol", 5000)
        tvar = ds.createVariable("time", np.double, ("time",))
        tvar[:] = [0.0]
        tvar.setncatts({"units": "days since 1850-01-01", "calendar": "360_day"})
        var = ds.createVariable("FIELD", np.float64, ("time", "lev", "ncol"))
        var[:] = np.random.random((1, nlev, 5000))
    clone_netcdf_with_missing(src, dst)
    return getsize(dst)


def test_primary_data_is_not_materialized(tmp_path):
    """
    Growing a primary field 90x barely changes the clone size, proving the
    missing values are stored via the fill value rather than written to disk.
    """
    small = _clone_size_with_levels(tmp_path, 1)
    large = _clone_size_with_levels(tmp_path, 90)
    # A materialized 90-level field would dwarf a 1-level one; unwritten it must
    # not. Allow generous slack for metadata/chunk-index differences.
    assert large < small * 2, f"clone grew with primary size: {small} -> {large}"


def test_float_primary_gets_nan_fillvalue(tmp_path):
    """A float primary without a source _FillValue reads back as NaN in the clone."""
    src = str(tmp_path / "src.nc")
    dst = str(tmp_path / "dst.nc")
    generate_history_file(src, [15.0], [[0.0, 30.0]])  # VAR* have no _FillValue

    clone_netcdf_with_missing(src, dst)

    with Dataset(dst) as d:
        var = d.variables["VAR0"]
        assert np.isnan(var._FillValue)
        var.set_auto_mask(False)
        assert np.all(np.isnan(np.asarray(var[:])))


def _write_guard_source(path):
    """Write a source with a large non-time 2-D field and a large 1-D coord."""
    with Dataset(str(path), "w", format="NETCDF4") as ds:
        ds.createDimension("a", 400)
        ds.createDimension("b", 400)
        ds.createDimension("n", 200_000)
        # 2-D, no time dim -> is_var_secondary() calls it secondary; 400*400*8
        # = 1.28 MiB exceeds the default guard threshold.
        ds.createVariable("BIG2D", np.float64, ("a", "b"))[:] = np.ones((400, 400))
        # 1-D coordinate, 1.6 MiB -> above threshold but exempt (guard skips 1-D).
        ds.createVariable("COORD", np.float64, ("n",))[:] = np.arange(200_000)


def test_size_guard_fills_large_non_time_field(tmp_path):
    """A large multi-dim field with no time dim is filled by the size guard."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _write_guard_source(src)

    clone_netcdf_with_missing(str(src), str(dst))  # default 1 MiB guard

    with Dataset(str(dst)) as d:
        v = d.variables["BIG2D"]
        v.set_auto_mask(False)
        assert np.all(np.isnan(np.asarray(v[:])))


def test_size_guard_fills_cice_grid_sized_field(tmp_path):
    """A ~1012 KiB 2-D grid array (CICE geometry) is filled by the default guard."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    with Dataset(str(src), "w", format="NETCDF4") as ds:
        ds.createDimension("nj", 480)
        ds.createDimension("ni", 540)
        # 480*540*4 = 1012.5 KiB: under 1 MiB (old default missed it), over 0.5.
        ds.createVariable("TLON", np.float32, ("nj", "ni"))[:] = np.ones((480, 540))

    clone_netcdf_with_missing(str(src), str(dst))  # default 0.5 MiB guard

    with Dataset(str(dst)) as d:
        v = d.variables["TLON"]
        v.set_auto_mask(False)
        assert np.all(np.isnan(np.asarray(v[:])))


def test_size_guard_exempts_1d_coordinate(tmp_path):
    """A large 1-D coordinate is copied verbatim regardless of the guard."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _write_guard_source(src)

    clone_netcdf_with_missing(str(src), str(dst))

    with Dataset(str(dst)) as d:
        assert np.array_equal(np.asarray(d.variables["COORD"][:]), np.arange(200_000))


def test_size_guard_disabled_copies_verbatim(tmp_path):
    """With the guard disabled, the large non-time field is copied verbatim."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _write_guard_source(src)

    clone_netcdf_with_missing(str(src), str(dst), max_copy_bytes=0)

    with Dataset(str(dst)) as d:
        assert np.array_equal(np.asarray(d.variables["BIG2D"][:]), np.ones((400, 400)))


def _make_valid_netcdf(path):
    """Write a minimal valid netCDF file at ``path``."""
    with Dataset(str(path), "w", format="NETCDF4") as ds:
        ds.createDimension("x", 2)
        ds.createVariable("V", np.float32, ("x",))[:] = [1.0, 2.0]


def test_is_valid_netcdf(tmp_path):
    """_is_valid_netcdf distinguishes readable files from corrupt/missing ones."""
    good = tmp_path / "good.nc"
    _make_valid_netcdf(good)
    assert _is_valid_netcdf(good) is True

    corrupt = tmp_path / "corrupt.nc"
    corrupt.write_bytes(b"not a netcdf file")
    assert _is_valid_netcdf(corrupt) is False

    assert _is_valid_netcdf(tmp_path / "missing.nc") is False


def test_resolve_clone_jobs_skips_existing_valid(tmp_path):
    """An existing valid clone is dropped from the job list and left untouched."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _make_valid_netcdf(dst)  # pretend a prior run already produced it

    pending = _resolve_clone_jobs([(src, dst)], overwrite=False)

    assert pending == []
    assert dst.exists()  # not deleted


def test_resolve_clone_jobs_rebuilds_corrupt(tmp_path):
    """An existing corrupt clone is deleted and its job retained for rebuild."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    dst.write_bytes(b"garbage")

    pending = _resolve_clone_jobs([(src, dst)], overwrite=False)

    assert pending == [(src, dst)]
    assert not dst.exists()  # corrupt file removed


def test_resolve_clone_jobs_overwrite_deletes_valid(tmp_path):
    """With overwrite, even a valid existing clone is deleted and rebuilt."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _make_valid_netcdf(dst)

    pending = _resolve_clone_jobs([(src, dst)], overwrite=True)

    assert pending == [(src, dst)]
    assert not dst.exists()


def test_resolve_clone_jobs_keeps_missing(tmp_path):
    """A job whose destination does not yet exist is always kept."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"

    assert _resolve_clone_jobs([(src, dst)], overwrite=False) == [(src, dst)]


def _recorded_entries(out_dir):
    """Return the command file split into (provenance_comment, command) pairs."""
    lines = (out_dir / CLONE_COMMAND_FILENAME).read_text().splitlines()
    return list(zip(lines[::2], lines[1::2]))


def test_records_clone_command(tmp_path, monkeypatch):
    """The invocation is written to the command file at the top of the clone."""
    monkeypatch.setattr(
        "sys.argv", ["gents_conform_build", "/case", "-o", str(tmp_path)]
    )

    record_clone_command(tmp_path)

    (provenance, command), = _recorded_entries(tmp_path)
    assert command == f"gents_conform_build /case -o {tmp_path}"
    assert provenance.startswith("#")


def test_records_clone_command_provenance(tmp_path, monkeypatch):
    """Each entry is preceded by a comment carrying date, host and GenTS version."""
    monkeypatch.setattr("sys.argv", ["gents_conform_build", "/case"])
    monkeypatch.setattr("socket.gethostname", lambda: "testhost01")
    monkeypatch.setattr(
        "gents.conformity.case_builder.get_time_stamp", lambda: "2026-07-30 14:55"
    )
    monkeypatch.setattr(
        "gents.conformity.case_builder.get_version", lambda: "9.9.9"
    )

    record_clone_command(tmp_path)

    (provenance, _), = _recorded_entries(tmp_path)
    assert provenance == "# 2026-07-30 14:55 | host: testhost01 | GenTS 9.9.9"


def test_records_clone_command_creates_directory(tmp_path, monkeypatch):
    """The clone directory is created if it does not exist yet."""
    out_dir = tmp_path / "not_yet_there"
    monkeypatch.setattr("sys.argv", ["gents_conform_build", "/case"])

    record_clone_command(out_dir)

    assert (out_dir / CLONE_COMMAND_FILENAME).is_file()


def test_records_clone_command_quotes_globs(tmp_path, monkeypatch):
    """Glob arguments are re-quoted so the recorded line can be pasted back into a shell."""
    monkeypatch.setattr(
        "sys.argv",
        ["gents_conform_build", "/case", "--include", "*.0001-*.nc"],
    )

    record_clone_command(tmp_path)

    recorded = (tmp_path / CLONE_COMMAND_FILENAME).read_text()
    assert "'*.0001-*.nc'" in recorded


def test_records_clone_command_uses_entry_point_name(tmp_path, monkeypatch):
    """Only the entry point's name is recorded, not its full installed path."""
    monkeypatch.setattr(
        "sys.argv", ["/usr/local/bin/gents_conform_build", "/case"]
    )

    record_clone_command(tmp_path)

    (_, command), = _recorded_entries(tmp_path)
    assert command.startswith("gents_conform_build /case")


def test_records_clone_command_appends(tmp_path, monkeypatch):
    """A clone built over several runs keeps every command that contributed files."""
    monkeypatch.setattr("sys.argv", ["gents_conform_build", "/case", "--include", "a*"])
    record_clone_command(tmp_path)

    monkeypatch.setattr("sys.argv", ["gents_conform_build", "/case", "--include", "b*"])
    record_clone_command(tmp_path)

    entries = _recorded_entries(tmp_path)
    assert len(entries) == 2
    assert entries[0][1].endswith("'a*'")
    assert entries[1][1].endswith("'b*'")


def test_recorded_command_file_is_valid_shell(tmp_path, monkeypatch):
    """Only the command lines are executable; provenance is commented out."""
    monkeypatch.setattr("sys.argv", ["gents_conform_build", "/case", "--include", "a*"])
    record_clone_command(tmp_path)

    lines = (tmp_path / CLONE_COMMAND_FILENAME).read_text().splitlines()
    for line in lines:
        assert line.startswith("#") or line.startswith("gents_conform_build")


def test_resolve_clone_jobs_dryrun_keeps_corrupt_file(tmp_path):
    """A dry run reports a corrupt clone as pending without deleting it."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    dst.write_text("not a netcdf file")

    pending = _resolve_clone_jobs([(src, dst)], overwrite=False, dryrun=True)

    assert pending == [(src, dst)]
    assert dst.exists()


def test_resolve_clone_jobs_dryrun_keeps_overwritten_file(tmp_path):
    """A dry run with overwrite leaves even a valid existing clone on disk."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _make_valid_netcdf(dst)

    pending = _resolve_clone_jobs([(src, dst)], overwrite=True, dryrun=True)

    assert pending == [(src, dst)]
    assert dst.exists()


def test_resolve_clone_jobs_dryrun_still_skips_valid(tmp_path):
    """A dry run reports the same skips a real run would: valid clones are not rebuilt."""
    src = tmp_path / "src.nc"
    dst = tmp_path / "dst.nc"
    _make_valid_netcdf(dst)

    assert _resolve_clone_jobs([(src, dst)], overwrite=False, dryrun=True) == []
