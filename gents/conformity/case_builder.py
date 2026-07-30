from gents.hfcollection import find_files, sort_hf_groups
from gents.meta import is_var_secondary, get_time_variables_names
from gents.datastore import GenTSDataStore
from gents.timeseries import get_timestep_label
from gents.utils import enable_logging, ProgressBar, get_time_stamp, get_version
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from netCDF4 import Dataset
from cftime import num2date
from fnmatch import fnmatch
import numpy as np
import argparse
import logging
import shlex
import socket
import sys

logger = logging.getLogger(__name__)

# Name of the file recording how a clone directory was built, written at the top
# of the clone. Matches the convention already used by existing clone bundles.
CLONE_COMMAND_FILENAME = "cmd.txt"

# Multi-dimensional variables whose logical (uncompressed) size exceeds this are
# never copied verbatim, even if classified as secondary. See the size-guard
# note in ``clone_netcdf_with_missing``. Kept below 1 MiB so that ~1 MiB static
# grid-geometry arrays (e.g. CICE's per-grid TLON/TLAT/tarea/tmask sets) are
# filled rather than copied.
DEFAULT_MAX_COPY_MIB = 0.5


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="GenTS Conformity Case Builder Tool"
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
        help="Number of worker processes used to clone files, and to read file "
             "headers for the --verbose summary, in parallel. (Default 1)"
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
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output and summarise the filtered case (file count, "
             "unique variables, output frequencies, years spanned). The summary "
             "reads every history file header, so it costs an extra metadata pass."
    )
    parser.add_argument(
        "-d", "--dryrun",
        action="store_true",
        help="Inspect the case and report what would be cloned without writing "
             "anything to disk. Implies --verbose, since the summary is the point "
             "of a dry run."
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


def record_clone_command(out_dir: Path):
    """
    Append the current invocation to the clone directory's command log.

    ``run_gents`` records the command that produced each time series in that
    file's ``gents_command`` attribute. A clone directory has no equivalent place
    to put that, so the command is written to a plain text file at the top of the
    clone instead, preserving how the clone was generated and making it
    reproducible from a real case later.

    Each invocation contributes two lines: a comment carrying the date, host and
    GenTS version, followed by the command itself::

        # 2026-07-30 14:55 | host: derecho01 | GenTS 1.1.3
        gents_conform_build /glade/.../my_case -o . -n 126 --include '*.0001-*.nc'

    The provenance goes on its own ``#`` comment line rather than onto the command
    line so the command stays directly pasteable and the whole file remains valid
    shell. The host is worth recording because a clone's source path is usually
    specific to the machine it was built on, and the GenTS version because
    ``is_var_secondary`` decides which variables get emptied — a clone built by a
    different version may have classified them differently.

    A clone is normally built by a single command and so holds exactly one entry.
    Entries are nonetheless appended rather than overwritten: a run only touches
    the files its filters select, so pointing a second command with different
    filters at the same directory adds to it, and overwriting would discard the
    command that produced everything already there. Building a variant is
    ordinarily cheaper and clearer than layering onto an existing clone.

    Arguments are quoted with :func:`shlex.join` so a recorded command can be
    pasted back into a shell unchanged. Without it, glob patterns passed to
    ``--include``/``--exclude`` would be recorded bare (the shell having already
    stripped their quotes) and would expand against the working directory if the
    line were re-run.

    :param out_dir: Head directory of the clone being written.
    :type out_dir: pathlib.Path
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # sys.argv[0] is the full path to the installed entry point; only its name is
    # meaningful to someone reading or re-running the recorded line.
    argv = [Path(sys.argv[0]).name] + sys.argv[1:]

    provenance = (
        f"# {get_time_stamp()} | host: {socket.gethostname()} | GenTS {get_version()}"
    )

    command_path = out_dir / CLONE_COMMAND_FILENAME
    with open(command_path, "a") as command_file:
        command_file.write(provenance + "\n")
        command_file.write(shlex.join(argv) + "\n")

    logger.info(f"Recorded clone command in '{command_path}'.")


def _summarize_case_file(path):
    """
    Reduce one file to its contribution to the case summary.

    Mirrors how :class:`~gents.meta.netCDFMeta` locates a time coordinate, but
    reads through :class:`~gents.datastore.GenTSDataStore` directly and
    *returns* rather than raises when a file has no usable time axis. That
    difference is the point: the clone deliberately mirrors the whole raw case
    tree, including restart, static, grid and log files that carry no time
    coordinate at all, and those must be summarised rather than rejected.

    Runs in a worker process and decodes nothing: ``num2date`` is monotonic for
    a fixed units and calendar, so raw values order exactly as the dates they
    denote and the extremes can be picked without converting anything. The
    caller decodes only the few values that survive aggregation.

    The file's *second* latest value rides along with its latest because a
    group's output frequency is the gap between two consecutive time steps. For
    a file holding many steps that gap lies inside the file, and the earliest
    value is a whole file-span away from the latest, not one step.

    :param path: netCDF file to inspect.
    :type path: pathlib.Path
    :returns: ``(variable_names, earliest, latest_values, reference)``, where
        ``latest_values`` holds the up-to-two latest raw time values in
        ascending order and ``reference`` is the ``(units, calendar)`` they are
        expressed in. For a file with no decodable time coordinate ``earliest``
        and ``reference`` are ``None`` and ``latest_values`` is empty.
    :rtype: tuple[list[str], float or None, list[float], tuple or None]
    """
    with GenTSDataStore(str(path), "r") as ds:
        variable_names = list(ds.variables)

        time_name, _ = get_time_variables_names(ds)
        if time_name is None:
            return variable_names, None, [], None

        time_var = ds[time_name]
        # Undecodable without both attributes; netCDFMeta raises here, but a
        # non-history file legitimately lacks them.
        if "units" not in time_var.ncattrs() or "calendar" not in time_var.ncattrs():
            return variable_names, None, [], None

        values = np.sort(np.atleast_1d(np.squeeze(time_var[:])).astype("float64"))
        reference = (time_var.units, time_var.calendar)

    return variable_names, float(values[0]), [float(v) for v in values[-2:]], reference


def log_case_summary(src_paths, num_processes=1):
    """
    Prints a summary of what a case covers once the clone filters are applied.

    Describes what a conformity case actually exercises, which is what decides
    how much a conformity run over it can prove: a case with only monthly streams
    spanning six years cannot exercise daily output or ten-year slicing, however
    many files it holds.

    Every file the clone will copy is inspected, not just the ones GenTS would
    accept as history files. Files are grouped by
    :func:`~gents.hfcollection.sort_hf_groups`, which works on paths alone, and
    read with :func:`_summarize_case_file`, which skips GenTS's metadata
    validation. Building an ``HFCollection`` here instead would drop every file
    without a time coordinate and abort outright on a group holding a single
    time step -- both routine in a raw case tree, and both files the clone still
    has to copy.

    Reads every file header, so it is only invoked under ``--verbose``. Those
    reads are the whole cost of the summary and are independent of one another,
    so they are spread over ``num_processes`` worker processes exactly as the
    clone itself is. Group membership is decided from paths alone, before any
    file is opened, so results can be folded back into their group in whatever
    order they arrive; the printed summary does not depend on that order.

    Each group's files are pooled under the ``(units, calendar)`` they were
    written with, because raw values only mean anything against their own
    reference and component models within one case do *not* reliably share one
    -- an ocean stream written against a different start date is routine. That
    also keeps mismatched calendars apart, which matters because ``cftime``
    raises rather than compares across them. One ``num2date`` call per pool then
    yields both the years it spans and its output frequency, so the whole case
    costs a handful of conversions rather than one per time step.

    :param src_paths: Filtered source files the clone will mirror.
    :type src_paths: list[pathlib.Path]
    :param num_processes: Number of worker processes used to read file headers
        in parallel. Defaults to ``1``.
    :type num_processes: int
    """
    hf_groups = sort_hf_groups(src_paths)
    if not hf_groups:
        print("  Case summary unavailable        : no files to summarise")
        return

    variables = set()
    timed_files = 0
    # (group key, units, calendar) -> [earliest raw value, latest raw values].
    pools = {}

    prog_bar = ProgressBar(total=len(src_paths), label="Summarizing Case")
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = {
            executor.submit(_summarize_case_file, path): (group_key, path)
            for group_key, group_paths in hf_groups.items()
            for path in group_paths
        }

        for future in as_completed(futures):
            group_key, path = futures[future]
            try:
                variable_names, earliest, latest_values, reference = future.result()
            except Exception as exc:
                logger.warning(f"Could not summarise '{path}': {exc}")
                continue
            finally:
                prog_bar.step()

            variables.update(variable_names)
            if reference is None:
                continue

            timed_files += 1
            pool = pools.setdefault((group_key,) + reference, [earliest, []])
            pool[0] = min(pool[0], earliest)
            pool[1] += latest_values

    years = []
    frequencies = set()
    for (_, units, calendar), (earliest, latest_values) in pools.items():
        latest_pair = sorted(latest_values)[-2:]
        dates = num2date([earliest] + latest_pair, units=units, calendar=calendar)
        # Years are plain integers and so compare across calendars; the dates
        # they came from do not, which is why each pool decodes with its own.
        years += [dates[0].year, dates[-1].year]
        frequencies.add(
            get_timestep_label(dates[2] - dates[1]) if len(latest_pair) == 2 else "unsorted"
        )

    print(f"  Files with time coordinates     : {timed_files}")
    print(f"  Unique variables                : {len(variables)}")
    print(f"  Output frequencies              : {', '.join(sorted(frequencies)) or 'none'}")
    print(f"  Years spanned                   : {f'{min(years)} - {max(years)}' if years else 'none'}")


def main():
    args = parse_arguments()
    verbose = args.verbose or args.dryrun

    if verbose:
        print(f"  Input (case) directory path     : {args.hf_head_dir}")
        print(f"  Output (clone) directory path   : {args.outputdir}")
        print(f"  Discovery glob                  : {args.pattern}")
        print(f"  Include filters                 : {args.include}")
        print(f"  Exclude filters                 : {args.exclude}")
        print(f"  Number of processes (cores)     : {args.num_processes}")
        print(f"  Preserve netCDF3 format         : {not args.upgrade_netcdf3}")
        print(f"  Max copy size (MiB)             : {args.max_copy_mib}")
        print(f"  Dry run                         : {args.dryrun}")
        enable_logging(verbose=True)

    if args.outputdir is None:
        raise ValueError("No output directory specified.")

    head_dir = Path(args.hf_head_dir).resolve()
    src_paths = [
        path for path in find_files(head_dir, args.pattern)
        if _passes_filters(str(path), args.include, args.exclude)
    ]
    logger.info("Found %d file(s) under %s to mirror.", len(src_paths), head_dir)

    if verbose:
        print(f"  Files to clone                  : {len(src_paths)}")
        log_case_summary(src_paths, args.num_processes)

    out_dir = Path(args.outputdir)
    clone_jobs = [
        (src_path, out_dir / src_path.relative_to(head_dir))
        for src_path in src_paths
    ]

    if not clone_jobs:
        logger.info("No files matched; nothing to clone.")
        print("GenTS done!")
        return

    if args.dryrun:
        print(f"Dry run: {len(clone_jobs)} file(s) would be cloned.")
        print("GenTS done!")
        return

    record_clone_command(out_dir)

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