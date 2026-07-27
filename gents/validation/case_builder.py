from gents.hfcollection import find_files
from gents.meta import is_var_secondary
from gents.utils import enable_logging, ProgressBar
from pathlib import Path
from netCDF4 import Dataset
from fnmatch import fnmatch
import numpy as np
import argparse
import logging

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="GenTS Validation Case Builder Tool"
    )
    parser.add_argument(
        "hf_head_dir",
        type=str,
        help="Path to the head directory of the case to mirror."
    )
    parser.add_argument(
        "-o", "--outputdir",
        type=str,
        help="Path to write the mirrored directory structure and clone files to."
    )
    parser.add_argument(
        "-p", "--pattern",
        type=str,
        default="*.nc*",
        help="Glob matched against file names to discover files to clone. "
             "(Default '*.nc*', matching GenTS's own discovery glob.)"
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Pattern to exclude (can be specified multiple times)."
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Pattern to include (can be specified multiple times)."
    )
    parser.add_argument(
        "--no-compress",
        dest="compress",
        action="store_false",
        help="Mirror the source's compression instead of forcing high "
             "compression on the clones (clones will not be shrunk)."
    )
    parser.add_argument(
        "--complevel",
        type=int,
        default=9,
        help="zlib compression level (0-9) to use when compressing. (Default 9)"
    )
    return parser.parse_args()


def _copy_variable_creation_kwargs(src_var, force_compression: bool, complevel: int) -> dict:
    """
    Collect the ``createVariable`` keyword arguments used to write a variable
    into the clone.

    When ``force_compression`` is set, high ``zlib`` + ``shuffle`` compression
    is applied to every dimensioned variable regardless of the source's own
    settings, which is what shrinks the clone (the missing-value primaries
    become constant arrays that compress to almost nothing). GenTS reads only
    variable values, dtypes, dimensions and attributes and never inspects
    on-disk layout, so overriding compression does not affect what the clones
    validate. When compression is off (or unsupported by the file format) the
    source's own filters and chunking are mirrored instead.

    :param src_var: Source netCDF4 variable to mirror.
    :type src_var: netCDF4._netCDF4.Variable
    :param force_compression: Apply high zlib+shuffle compression instead of
        mirroring the source's filters.
    :type force_compression: bool
    :param complevel: zlib compression level (0-9) to use when compressing.
    :type complevel: int
    :returns: Keyword arguments to pass to ``Dataset.createVariable``.
    :rtype: dict
    """
    kwargs = {}

    # zlib compression requires chunked storage, which netCDF4 cannot apply to
    # scalar (dimensionless) variables, so only force it on dimensioned ones.
    if force_compression and src_var.dimensions:
        kwargs["zlib"] = True
        kwargs["complevel"] = complevel
        kwargs["shuffle"] = True
        # Preserve explicit source chunking; otherwise let netCDF4 auto-chunk
        # (forcing a contiguous layout is incompatible with compression).
        try:
            chunking = src_var.chunking()
            if chunking != "contiguous":
                kwargs["chunksizes"] = chunking
        except Exception:
            pass
    else:
        # Mirror the source's compression / checksum filters. ``filters()``
        # returns None for variable types that cannot carry filters (e.g. VLEN).
        filters = src_var.filters()
        if filters:
            for key in ("zlib", "complevel", "shuffle", "fletcher32"):
                if key in filters:
                    kwargs[key] = filters[key]

        # Chunking is either the string "contiguous" or a list of chunk sizes.
        try:
            chunking = src_var.chunking()
            if chunking != "contiguous":
                kwargs["chunksizes"] = chunking
        except Exception:
            pass

    try:
        kwargs["endian"] = src_var.endian()
    except Exception:
        pass

    # ``_FillValue`` must be set at creation time; it cannot be assigned as a
    # regular attribute afterwards, so route it through the kwarg here and skip
    # it when copying the remaining attributes.
    fill_value = getattr(src_var, "_FillValue", None)
    if fill_value is not None:
        kwargs["fill_value"] = fill_value

    return kwargs


def clone_netcdf_with_missing(src_path: str, dst_path: str, compress: bool = True,
                              complevel: int = 9):
    """
    Create a structurally identical netCDF file with the primary (scientific)
    variables replaced by missing values so the clone is cheap to store.

    Primary variables (multi-dimensional, time-varying fields, as classified by
    :func:`gents.meta.is_var_secondary`) are filled with ``NaN`` for floating
    point types, the integer fill value for integer types, and empty strings for
    character types. Secondary variables (coordinates, time, bounds) are copied
    verbatim so the clone remains a valid, self-describing history file.

    By default high ``zlib`` + ``shuffle`` compression is forced on every
    dimensioned variable, regardless of the source's own settings. This is what
    shrinks the clone: the constant missing-value primaries compress to almost
    nothing. GenTS reads only variable values, dtypes, dimensions and attributes
    and never inspects on-disk layout, so this does not affect what the clones
    validate. Compression is skipped for netCDF3 file formats, which do not
    support it; there the source's own filters are mirrored instead and the
    original file format is always preserved.

    Preserves dimensions (including unlimited), global and per-variable
    attributes, endianness, fill values, groups and the file format.

    :param src_path: Path to the source netCDF file to clone.
    :type src_path: str
    :param dst_path: Path to write the missing-value clone to. Parent
        directories are created as needed.
    :type dst_path: str
    :param compress: Force high compression on the clone. Defaults to ``True``.
    :type compress: bool
    :param complevel: zlib compression level (0-9) to use when compressing.
        Defaults to ``9``.
    :type complevel: int
    """
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)

    with Dataset(src_path, "r") as src, Dataset(dst_path, "w", format=src.file_format) as dst:
        # zlib is only available on the HDF5-backed netCDF4 formats.
        force_compression = compress and src.file_format.startswith("NETCDF4")
        _copy_group(src, dst, force_compression, complevel)


def _copy_group(src_grp, dst_grp, force_compression: bool, complevel: int):
    """
    Recursively copy a netCDF group's structure into ``dst_grp``, filling
    primary variables with missing values. See :func:`clone_netcdf_with_missing`.
    """
    dst_grp.setncatts({attr: src_grp.getncattr(attr) for attr in src_grp.ncattrs()})

    for name, dim in src_grp.dimensions.items():
        dst_grp.createDimension(name, None if dim.isunlimited() else len(dim))

    for name, src_var in src_grp.variables.items():
        dst_var = dst_grp.createVariable(
            name,
            src_var.dtype,
            src_var.dimensions,
            **_copy_variable_creation_kwargs(src_var, force_compression, complevel),
        )
        dst_var.setncatts(
            {attr: src_var.getncattr(attr)
             for attr in src_var.ncattrs() if attr != "_FillValue"}
        )

        if src_var.size == 0:
            continue

        # Secondary variables (coordinates, time, bounds) are copied unchanged
        # so the clone stays self-describing; primary fields become missing.
        if is_var_secondary(src_var):
            dst_var[:] = src_var[:]
        else:
            _fill_missing(src_var, dst_var)

    for name, subgroup in src_grp.groups.items():
        _copy_group(subgroup, dst_grp.createGroup(name), force_compression, complevel)


def _fill_missing(src_var, dst_var):
    """
    Write missing values over the whole extent of ``dst_var``, matching the
    dtype of ``src_var``.
    """
    if np.issubdtype(src_var.dtype, np.floating):
        dst_var[:] = np.full(src_var.shape, np.nan, dtype=src_var.dtype)
    elif np.issubdtype(src_var.dtype, np.integer):
        fill = getattr(src_var, "_FillValue", None)
        if fill is None:
            fill = np.iinfo(src_var.dtype).min
        dst_var[:] = np.full(src_var.shape, fill, dtype=src_var.dtype)
    elif src_var.dtype.kind in ("S", "U"):
        dst_var[:] = ""
    else:
        # Unknown/compound dtype: fall back to copying the real values.
        dst_var[:] = src_var[:]


def _passes_filters(path: str, include: list, exclude: list) -> bool:
    """
    Apply GenTS-style ``fnmatch`` include/exclude globs to an absolute path.

    :param path: Absolute path string to test.
    :param include: Globs; if non-empty, the path must match at least one.
    :param exclude: Globs; the path must match none of them.
    :returns: ``True`` if the path should be cloned.
    :rtype: bool
    """
    if include and not any(fnmatch(path, glob) for glob in include):
        return False
    if any(fnmatch(path, glob) for glob in exclude):
        return False
    return True


def main():
    enable_logging(verbose=True)
    args = parse_arguments()

    if args.outputdir is None:
        raise ValueError("No output directory specified.")

    # Mirror the raw case directory rather than a viable HFCollection: we want
    # every file matching the discovery glob, including ones that GenTS's
    # metadata filters are supposed to ignore, so the clones can exercise them.
    head_dir = Path(args.hf_head_dir).resolve()
    src_paths = [
        path for path in find_files(head_dir, args.pattern)
        if _passes_filters(str(path), args.include, args.exclude)
    ]
    logger.info("Found %d file(s) under %s to mirror.", len(src_paths), head_dir)

    out_dir = Path(args.outputdir)
    prog_bar = ProgressBar(total=len(src_paths), label="Creating NaN-Filled Clone")
    for src_path in src_paths:
        dst_path = out_dir / src_path.relative_to(head_dir)
        clone_netcdf_with_missing(
            str(src_path), str(dst_path),
            compress=args.compress, complevel=args.complevel,
        )
        prog_bar.step()

    print("GenTS done!")