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
    return parser.parse_args()


def _copy_variable_creation_kwargs(src_var: "Dataset.variables") -> dict:
    """
    Collect the ``createVariable`` keyword arguments needed to reproduce a
    variable's on-disk layout (compression, checksumming, chunking, endianness
    and fill value).

    :param src_var: Source netCDF4 variable to mirror.
    :type src_var: netCDF4._netCDF4.Variable
    :returns: Keyword arguments to pass to ``Dataset.createVariable``.
    :rtype: dict
    """
    kwargs = {}

    # Compression / checksum filters. ``filters()`` returns None for variable
    # types that cannot carry filters (e.g. VLEN), so guard against that.
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


def clone_netcdf_with_missing(src_path: str, dst_path: str):
    """
    Create a structurally identical netCDF file with the primary (scientific)
    variables replaced by missing values so the clone is cheap to store.

    Primary variables (multi-dimensional, time-varying fields, as classified by
    :func:`gents.meta.is_var_secondary`) are filled with ``NaN`` for floating
    point types, the integer fill value for integer types, and empty strings for
    character types. Because these arrays become constant, they compress to a
    fraction of their original size whenever the source variable uses
    compression. Secondary variables (coordinates, time, bounds) are copied
    verbatim so the clone remains a valid, self-describing history file.

    Preserves dimensions (including unlimited), global and per-variable
    attributes, compression, chunking, endianness, fill values and groups.

    :param src_path: Path to the source netCDF file to clone.
    :type src_path: str
    :param dst_path: Path to write the missing-value clone to. Parent
        directories are created as needed.
    :type dst_path: str
    """
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)

    with Dataset(src_path, "r") as src, Dataset(dst_path, "w", format=src.file_format) as dst:
        _copy_group(src, dst)


def _copy_group(src_grp, dst_grp):
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
            **_copy_variable_creation_kwargs(src_var),
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
        _copy_group(subgroup, dst_grp.createGroup(name))


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
        clone_netcdf_with_missing(str(src_path), str(dst_path))
        prog_bar.step()

    print("GenTS done!")