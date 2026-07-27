from gents.hfcollection import find_files
from gents.meta import is_var_secondary
from gents.utils import enable_logging, ProgressBar
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from netCDF4 import Dataset
from fnmatch import fnmatch
import numpy as np
import argparse
import logging

logger = logging.getLogger(__name__)

# Multi-dimensional variables whose logical (uncompressed) size exceeds this are
# never copied verbatim, even if classified as secondary. See the size-guard
# note in ``clone_netcdf_with_missing``.
DEFAULT_MAX_COPY_MIB = 1.0


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
        "-n", "--num-processes",
        type=int,
        default=1,
        help="Number of worker processes used to clone files in parallel. "
             "(Default 1)"
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
        "--preserve-format",
        dest="upgrade_netcdf3",
        action="store_false",
        help="Preserve the source file format exactly. netCDF3-model sources "
             "(incl. CDF-5) are otherwise rewritten as NETCDF4 so their clones "
             "can shrink; with this flag they keep their format and full size."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing clones. By default an existing, valid clone is "
             "left in place (corrupt ones are deleted and rebuilt)."
    )
    parser.add_argument(
        "--max-copy-mib",
        type=float,
        default=DEFAULT_MAX_COPY_MIB,
        help="Multi-dimensional variables larger than this (in MiB) are filled "
             "instead of copied verbatim, even if classified as secondary. "
             f"0 disables the guard. (Default {DEFAULT_MAX_COPY_MIB})"
    )
    return parser.parse_args()


def _copy_variable_creation_kwargs(src_var) -> dict:
    """
    Collect the ``createVariable`` keyword arguments that mirror a source
    variable's on-disk layout — its compression / checksum filters, chunking,
    endianness and fill value.

    The clone applies no compression of its own: the size savings come from
    leaving primary variables unwritten (see :func:`clone_netcdf_with_missing`),
    not from compressing them. Mirroring the source exactly keeps the clone's
    metadata and structure as faithful to the original as possible — a
    compressed source stays compressed, an uncompressed source stays
    uncompressed.

    :param src_var: Source netCDF4 variable to mirror.
    :type src_var: netCDF4._netCDF4.Variable
    :returns: Keyword arguments to pass to ``Dataset.createVariable``.
    :rtype: dict
    """
    kwargs = {}

    # Mirror the source's compression / checksum filters. ``filters()`` returns
    # None for variable types that cannot carry filters (e.g. VLEN).
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


def clone_netcdf_with_missing(src_path: str, dst_path: str,
                              max_copy_bytes: int = int(DEFAULT_MAX_COPY_MIB * 1024**2),
                              upgrade_netcdf3: bool = True):
    """
    Create a structurally identical netCDF file with the primary (scientific)
    variables replaced by missing values so the clone is cheap to store.

    Primary variables (multi-dimensional, time-varying fields, as classified by
    :func:`gents.meta.is_var_secondary`) are never written: they are created with
    a fill value (``NaN`` for floating point types, the source ``_FillValue`` or
    the dtype minimum for integer types) and left empty. HDF5 (the netCDF4
    backend) allocates no storage for an unwritten variable and returns the fill
    value on read, so the primaries occupy essentially nothing on disk while
    still reading back at full shape as missing data. A consequence is that a
    floating point primary always carries ``_FillValue = NaN`` in the clone, even
    if the source used a different fill (or none). Secondary variables
    (coordinates, time, bounds) are copied verbatim so the clone remains a valid,
    self-describing history file.

    No compression is applied: the size savings come entirely from leaving the
    primaries unwritten, not from compressing them. Each variable's compression
    filters, chunking, endianness and fill value are mirrored from the source, so
    the clone's metadata and structure stay as faithful to the original as
    possible (a compressed source stays compressed; an uncompressed one does
    not).

    **Size guard:** as a backstop against variables the classifier misses (for
    example a large field on an unrecognised record dimension), any *multi-
    dimensional* variable whose logical size exceeds ``max_copy_bytes`` is filled
    rather than copied, even if classified secondary. One-dimensional variables
    (coordinates) are always copied verbatim regardless of size. Note this can
    fill large multi-dimensional coordinate variables (e.g. 2-D curvilinear
    lat/lon); raise the threshold or pass ``0`` to disable the guard if that
    matters for a given case.

    **File format:** the "unwritten variable costs nothing" behaviour relies on
    HDF5's lazy allocation, which only the netCDF4 formats provide. A
    netCDF3-model source (classic, 64-bit offset, or CDF-5 / 64-bit data) stores
    every variable at full size regardless of whether it is written, so by
    default such a source is written as ``NETCDF4`` so the clone can shrink.
    ``NETCDF4`` (not ``NETCDF4_CLASSIC``) is used because CDF-5 permits extended
    integer types the classic data model cannot hold. Set ``upgrade_netcdf3`` to
    ``False`` to preserve the original format exactly, at the cost of the clone
    not shrinking for netCDF3 sources.

    Preserves dimensions (including unlimited), global and per-variable
    attributes, endianness, fill values and groups.

    :param src_path: Path to the source netCDF file to clone.
    :type src_path: str
    :param dst_path: Path to write the missing-value clone to. Parent
        directories are created as needed.
    :type dst_path: str
    :param max_copy_bytes: Logical-size threshold, in bytes, above which a
        multi-dimensional variable is filled instead of copied verbatim. ``0``
        disables the guard. Defaults to ``DEFAULT_MAX_COPY_MIB`` MiB.
    :type max_copy_bytes: int
    :param upgrade_netcdf3: Rewrite netCDF3-model sources as ``NETCDF4`` so they
        can shrink. When ``False`` the source format is preserved exactly.
        Defaults to ``True``.
    :type upgrade_netcdf3: bool
    """
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)

    with Dataset(src_path, "r") as src:
        dst_format = src.file_format
        if not dst_format.startswith("NETCDF4"):
            if upgrade_netcdf3:
                dst_format = "NETCDF4"
            else:
                logger.warning(
                    "Preserving netCDF3 format for '%s'; unwritten variables "
                    "still occupy full space, so this clone will not shrink.",
                    src_path,
                )

        with Dataset(dst_path, "w", format=dst_format) as dst:
            _copy_group(src, dst, max_copy_bytes)


def _copy_group(src_grp, dst_grp, max_copy_bytes: int):
    """
    Recursively copy a netCDF group's structure into ``dst_grp``, filling
    primary variables with missing values. See :func:`clone_netcdf_with_missing`.
    """
    dst_grp.setncatts({attr: src_grp.getncattr(attr) for attr in src_grp.ncattrs()})

    for name, dim in src_grp.dimensions.items():
        dst_grp.createDimension(name, None if dim.isunlimited() else len(dim))

    for name, src_var in src_grp.variables.items():
        secondary = is_var_secondary(src_var)

        # Size guard: never copy a large multi-dimensional field verbatim, even
        # if the classifier calls it secondary (e.g. an unrecognised record-
        # dimension name). Such a field is filled like a primary instead.
        if secondary and max_copy_bytes and len(src_var.dimensions) > 1:
            itemsize = getattr(src_var.dtype, "itemsize", 0)
            logical_bytes = int(np.prod(src_var.shape, dtype=np.int64)) * itemsize
            if logical_bytes > max_copy_bytes:
                secondary = False

        kwargs = _copy_variable_creation_kwargs(src_var)

        # For primary fields the missing value is written implicitly via the
        # variable's fill value (see below), which overrides any source fill.
        missing = None if secondary else _missing_value(src_var)
        if missing is not None:
            kwargs["fill_value"] = missing

        dst_var = dst_grp.createVariable(
            name, src_var.dtype, src_var.dimensions, **kwargs,
        )
        dst_var.setncatts(
            {attr: src_var.getncattr(attr)
             for attr in src_var.ncattrs() if attr != "_FillValue"}
        )

        if src_var.size == 0:
            continue

        if secondary:
            # Coordinates, time and bounds are copied unchanged so the clone
            # stays self-describing.
            dst_var[:] = src_var[:]
        elif missing is None:
            # Char/compound dtype we cannot express as a fill value: copy it.
            dst_var[:] = src_var[:]
        # Otherwise leave the primary field unwritten: HDF5 allocates no storage
        # for it and returns ``missing`` (the fill value) on read, which is what
        # keeps the clone small.

    for name, subgroup in src_grp.groups.items():
        _copy_group(subgroup, dst_grp.createGroup(name), max_copy_bytes)


def _missing_value(src_var):
    """
    Return the value an unwritten primary variable should read back as, or
    ``None`` if the dtype cannot be represented by a fill value (in which case
    the variable is copied verbatim instead).

    Floating point fields read back as ``NaN``; integer fields read back as the
    source ``_FillValue`` if present, otherwise the smallest value of the dtype.

    :param src_var: Source netCDF4 variable being cloned.
    :type src_var: netCDF4._netCDF4.Variable
    :returns: The fill value for the clone, or ``None`` for unsupported dtypes.
    """
    if np.issubdtype(src_var.dtype, np.floating):
        return src_var.dtype.type(np.nan)
    if np.issubdtype(src_var.dtype, np.integer):
        fill = getattr(src_var, "_FillValue", None)
        if fill is None:
            fill = np.iinfo(src_var.dtype).min
        return src_var.dtype.type(fill)
    return None


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


def _is_valid_netcdf(path: Path) -> bool:
    """
    Report whether ``path`` is a readable (non-corrupt) netCDF file.

    :param path: Path to test.
    :type path: pathlib.Path
    :returns: ``True`` if the file opens as a netCDF dataset.
    :rtype: bool
    """
    try:
        with Dataset(str(path), "r"):
            return True
    except Exception:
        return False


def _resolve_clone_jobs(clone_jobs: list, overwrite: bool) -> list:
    """
    Decide which clone jobs to run given any pre-existing destination files.

    With ``overwrite`` set, every existing destination is deleted and all jobs
    are kept. Otherwise an existing *valid* clone is left in place and its job
    dropped, while a *corrupt* existing clone is deleted and its job retained so
    it is rebuilt.

    :param clone_jobs: List of ``(src_path, dst_path)`` tuples.
    :type clone_jobs: list
    :param overwrite: Overwrite existing clones unconditionally.
    :type overwrite: bool
    :returns: The subset of ``clone_jobs`` that still needs to be built.
    :rtype: list
    """
    pending = []
    skipped = 0
    for src_path, dst_path in clone_jobs:
        if dst_path.exists():
            if not overwrite and _is_valid_netcdf(dst_path):
                skipped += 1
                continue
            dst_path.unlink()
        pending.append((src_path, dst_path))

    if skipped:
        logger.info("Skipping %d existing valid clone(s).", skipped)
    return pending


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
    clone_jobs = [
        (src_path, out_dir / src_path.relative_to(head_dir))
        for src_path in src_paths
    ]

    # Drop jobs whose valid clone already exists (and rebuild corrupt ones)
    # before creating directories or spawning workers.
    clone_jobs = _resolve_clone_jobs(clone_jobs, args.overwrite)
    if not clone_jobs:
        logger.info("Nothing to clone; all destinations already valid.")
        print("GenTS done!")
        return

    # Create the mirrored directory tree serially up front so the parallel
    # workers never race to create the same (possibly shared) parent directory.
    for parent in {dst_path.parent for _, dst_path in clone_jobs}:
        parent.mkdir(parents=True, exist_ok=True)

    max_copy_bytes = int(max(0.0, args.max_copy_mib) * 1024**2)
    prog_bar = ProgressBar(total=len(clone_jobs), label="Creating NaN-Filled Clone")
    with ProcessPoolExecutor(max_workers=args.num_processes) as executor:
        futures = {}
        for src_path, dst_path in clone_jobs:
            future = executor.submit(
                clone_netcdf_with_missing,
                str(src_path), str(dst_path), max_copy_bytes, args.upgrade_netcdf3,
            )
            futures[future] = src_path

        for future in as_completed(futures):
            src_path = futures[future]
            try:
                future.result()
            except Exception as exc:
                logger.warning(f"Failed to clone {src_path}: {exc}", exc_info=True)
            finally:
                prog_bar.step()

    print("GenTS done!")