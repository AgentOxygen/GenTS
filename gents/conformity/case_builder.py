"""
``gents_conform_build`` -- mirror a real case directory as tiny missing-value clones.

Primary (scientific) variables are created but never written, so HDF5 stores
nothing for them and returns their fill value on read. A multi-GB case therefore
mirrors down to KB while staying structurally identical, which is what makes
end-to-end conformity testing against real cases affordable.
"""
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

# Records how a clone directory was built; written at the top of the clone.
CLONE_COMMAND_FILENAME = "cmd.txt"

# Size guard threshold (see ``clone_netcdf_with_missing``). Kept below 1 MiB so
# that ~1 MiB static grid-geometry arrays (e.g. CICE's TLON/TLAT/tarea/tmask) are
# filled rather than copied.
DEFAULT_MAX_COPY_MIB = 0.5


def parse_arguments():
    """
    Parses ``gents_conform_build`` command line arguments.
    """
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
    Collects the ``createVariable`` arguments that mirror a source variable's
    on-disk layout: filters, chunking, endianness and fill value.

    The clone adds no compression of its own, so mirroring the source keeps the
    clone as faithful to the original as possible.

    :param src_var: Source netCDF4 variable to mirror.
    :type src_var: netCDF4._netCDF4.Variable
    :returns: Keyword arguments for ``Dataset.createVariable``.
    :rtype: dict
    """
    kwargs = {}

    # ``filters()`` returns None for types that cannot carry filters (e.g. VLEN).
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

    # ``_FillValue`` cannot be assigned after creation, so it goes through the
    # kwarg here and is skipped when the remaining attributes are copied.
    fill_value = getattr(src_var, "_FillValue", None)
    if fill_value is not None:
        kwargs["fill_value"] = fill_value

    return kwargs


def clone_netcdf_with_missing(src_path: str, dst_path: str,
                              max_copy_bytes: int = int(DEFAULT_MAX_COPY_MIB * 1024**2),
                              upgrade_netcdf3: bool = True):
    """
    Writes a structurally identical copy of a netCDF file holding no primary data.

    Primary variables (per :func:`gents.meta.is_var_secondary`) are created with a
    fill value and never written; HDF5 allocates nothing for them and returns the
    fill on read, so they read back at full shape while occupying no space. One
    consequence: a floating point primary always carries ``_FillValue = NaN`` in
    the clone, whatever the source used. Secondary variables (coordinates, time,
    bounds) are copied verbatim so the clone stays self-describing, as are
    dimensions, attributes, endianness and groups.

    Size comes purely from what is left unwritten -- no compression is added; each
    variable's source filters and chunking are mirrored instead.

    **Size guard.** Any *multi-dimensional* variable larger than
    ``max_copy_bytes`` is filled rather than copied even if classified secondary,
    which catches large fields on unrecognised record dimensions. One-dimensional
    coordinates are always copied. This can fill 2-D curvilinear lat/lon; raise
    the threshold or pass ``0`` if a case needs them.

    **File format.** Lazy allocation is an HDF5 feature, so a netCDF3-model source
    (classic, 64-bit offset, CDF-5) would not shrink at all and is rewritten as
    ``NETCDF4`` by default -- not ``NETCDF4_CLASSIC``, which cannot hold CDF-5's
    extended integer types.

    :param src_path: Source netCDF file to clone.
    :type src_path: str
    :param dst_path: Where to write the clone; parent directories are created.
    :type dst_path: str
    :param max_copy_bytes: Size guard threshold in bytes; ``0`` disables it.
    :type max_copy_bytes: int
    :param upgrade_netcdf3: Rewrite netCDF3 sources as ``NETCDF4`` so they can
        shrink, rather than preserving their format exactly.
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

        # Size guard: a large multi-dimensional field is filled like a primary
        # even when the classifier calls it secondary.
        if secondary and max_copy_bytes and len(src_var.dimensions) > 1:
            itemsize = getattr(src_var.dtype, "itemsize", 0)
            logical_bytes = int(np.prod(src_var.shape, dtype=np.int64)) * itemsize
            if logical_bytes > max_copy_bytes:
                secondary = False

        kwargs = _copy_variable_creation_kwargs(src_var)

        # A primary's missing value comes from its fill value, overriding the
        # source's own fill.
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
            dst_var[:] = src_var[:]
        elif missing is None:
            # Char/compound dtype we cannot express as a fill value: copy it.
            dst_var[:] = src_var[:]
        # Otherwise the primary is left unwritten -- that is the whole trick.

    for name, subgroup in src_grp.groups.items():
        _copy_group(subgroup, dst_grp.createGroup(name), max_copy_bytes)


def _missing_value(src_var):
    """
    Returns the value an unwritten primary variable should read back as.

    Floating point fields read back as ``NaN``; integer fields as the source
    ``_FillValue``, or the dtype minimum if it has none.

    :param src_var: Source netCDF4 variable being cloned.
    :type src_var: netCDF4._netCDF4.Variable
    :returns: The clone's fill value, or ``None`` for a dtype that cannot carry
        one (such a variable is copied verbatim instead).
    :rtype: numpy.generic or None
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
    Applies ``fnmatch`` include/exclude globs to an absolute path.

    Note the empty-list convention differs from
    :meth:`~gents.hfcollection.HFCollection.include`: here an empty ``include``
    means "no filter", there it means "match nothing".

    :param path: Absolute path string to test.
    :type path: str
    :param include: Globs; if non-empty, the path must match at least one.
    :type include: list[str]
    :param exclude: Globs; the path must match none of them.
    :type exclude: list[str]
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
    Appends this invocation to the clone directory's ``cmd.txt``.

    It is the clone-tree equivalent of the ``gents_command`` attribute
    ``run_gents`` stamps into each output file: a record of how the clone was
    built, so it can be rebuilt from the real case later. Each invocation writes
    a provenance comment and the command itself::

        # 2026-07-30 14:55 | host: derecho01 | GenTS 1.1.3
        gents_conform_build /glade/.../my_case -o . -n 126 --include '*.0001-*.nc'

    The version matters because :func:`~gents.meta.is_var_secondary` decides which
    variables get emptied, and the host because a source path is usually specific
    to the machine that had the case. Arguments are :func:`shlex.join`-quoted so
    the line stays pasteable without its globs expanding. Lines are appended, not
    replaced, because a second run with different filters adds to a clone rather
    than rebuilding it.

    :param out_dir: Head directory of the clone being written.
    :type out_dir: pathlib.Path
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Only the entry point's name is meaningful in a recorded command line.
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
    Reduces one file to its contribution to the case summary.

    Locates a time coordinate as :class:`~gents.meta.netCDFMeta` does, but
    *returns* empty-handed instead of raising when a file has none: the clone
    mirrors the whole raw tree, restart and grid files included, and those have
    to be summarised rather than rejected.

    Decodes nothing. ``num2date`` is monotonic for a fixed units and calendar, so
    raw values order exactly as the dates they denote and only the few that
    survive aggregation need converting. The second-latest value rides along with
    the latest because a frequency is the gap between *consecutive* steps, which
    for a multi-step file lies inside the file.

    :param path: netCDF file to inspect.
    :type path: pathlib.Path
    :returns: ``(variable_names, earliest, latest_values, reference)``, where
        ``latest_values`` holds the up-to-two latest raw time values ascending and
        ``reference`` is the ``(units, calendar)`` they are expressed in. A file
        with no usable time coordinate returns ``None`` for both the earliest
        value and the reference.
    :rtype: tuple[list[str], float or None, list[float], tuple or None]
    """
    with GenTSDataStore(str(path), "r") as ds:
        variable_names = list(ds.variables)

        time_name, _ = get_time_variables_names(ds)
        if time_name is None:
            return variable_names, None, [], None

        time_var = ds[time_name]
        # netCDFMeta raises without both attributes, but a non-history file
        # legitimately lacks them.
        if "units" not in time_var.ncattrs() or "calendar" not in time_var.ncattrs():
            return variable_names, None, [], None

        values = np.sort(np.atleast_1d(np.squeeze(time_var[:])).astype("float64"))
        reference = (time_var.units, time_var.calendar)

    return variable_names, float(values[0]), [float(v) for v in values[-2:]], reference


def log_case_summary(src_paths, num_processes=1):
    """
    Prints what a case covers once the clone filters are applied.

    The coverage is what decides how much a conformity run over the case can
    prove: monthly streams spanning six years cannot exercise daily output or
    ten-year slicing, however many files they amount to.

    Every file the clone will copy is inspected, not only those GenTS accepts as
    history files, so grouping uses the path-only
    :func:`~gents.hfcollection.sort_hf_groups` and reading uses
    :func:`_summarize_case_file`. An ``HFCollection`` would drop every file
    lacking a time coordinate and abort on a single-timestep group, both routine
    in a raw case tree.

    Files are pooled by ``(group, units, calendar)``: component models within one
    case do not reliably share a time reference, and raw values only mean anything
    against their own. Pooling also keeps calendars apart, which ``cftime`` will
    not compare across. One ``num2date`` per pool then yields both its span and
    its frequency, so the whole case costs a handful of conversions.

    Reads every file header, which is the entire cost of the summary and why it
    is only invoked under ``--verbose``.

    :param src_paths: Filtered source files the clone will mirror.
    :type src_paths: list[pathlib.Path]
    :param num_processes: Worker processes used to read headers in parallel.
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
        # Years are plain integers and compare across calendars; the dates they
        # came from do not, which is why each pool decodes with its own.
        years += [dates[0].year, dates[-1].year]
        frequencies.add(
            get_timestep_label(dates[2] - dates[1]) if len(latest_pair) == 2 else "unsorted"
        )

    print(f"  Files with time coordinates     : {timed_files}")
    print(f"  Unique variables                : {len(variables)}")
    print(f"  Output frequencies              : {', '.join(sorted(frequencies)) or 'none'}")
    print(f"  Years spanned                   : {f'{min(years)} - {max(years)}' if years else 'none'}")


def main():
    """
    Entry point for ``gents_conform_build``.

    Every run rebuilds every file its filters select: there is deliberately no
    resume, skip-existing or overwrite flag, since clones are cheap and a fresh
    variant beats reasoning about what a directory already holds. Files outside
    the current filters are left untouched, so a second run with different
    filters adds to a clone.
    """
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