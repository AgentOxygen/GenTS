#!/usr/bin/env python
"""
Discovery, filtering, grouping and slicing of model history files.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
from gents.meta import get_meta_from_path
from gents.utils import ProgressBar, LOG_LEVEL_IO_WARNING
from cftime import num2date
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import os
import fnmatch
import cftime
import warnings
import logging
import copy

logging.captureWarnings(True)
logger = logging.getLogger(__name__)


def find_files(head_path, pattern):
    """
    Recursively finds files whose names match an ``fnmatch`` pattern.

    :param head_path: Root directory to search.
    :type head_path: str or pathlib.Path
    :param pattern: Wildcard pattern matched against file names (e.g. ``'*.nc'``).
    :type pattern: str
    :returns: Sorted list of matching paths.
    :rtype: list[pathlib.Path]
    """
    matched_files = []

    for root, dirs, files in os.walk(head_path):
        for file in files:
            if fnmatch.fnmatch(file, pattern):
                matched_files.append(Path(os.path.join(root, file)))

    return sorted(matched_files)


def calculate_year_slices(slice_size_years, min_year, max_year):
    """
    Computes non-overlapping year ranges covering a span.

    Each range is at most ``slice_size_years`` wide, with the upper boundary
    rounded up to the next multiple of that width. A span no wider than one slice
    is returned unsliced.

    :param slice_size_years: Maximum width of each slice in years.
    :type slice_size_years: int
    :param min_year: First year in the range (inclusive).
    :type min_year: int
    :param max_year: Last year in the range (inclusive).
    :type max_year: int
    :returns: List of ``(start_year, end_year)`` tuples, one per slice.
    :rtype: list[tuple[int, int]]
    :raises ValueError: If ``max_year`` is less than ``min_year``.
    """
    if max_year < min_year:
        raise ValueError("Maximum year cannot exceed minimum year.")
    if slice_size_years >= max_year - min_year:
        return [(min_year, max_year)]

    start_year = min_year
    end_year = int(np.ceil(max_year / slice_size_years)*slice_size_years)

    ranges = []
    for year in np.arange(start_year, end_year+slice_size_years, slice_size_years, dtype=int):
        ranges.append((int(year), int(year+slice_size_years-1)))
        if year >= max_year or year+slice_size_years-1 >= max_year:
            break
    
    return ranges


def sort_hf_groups(hf_paths, delimiter=".", substring_index=2):
    """
    Groups history file paths by parent directory and shared filename prefix.

    The prefix is the filename minus its last ``substring_index``
    ``delimiter``-separated tokens, so ``model.h0.0001-01.nc`` and
    ``model.h0.0001-02.nc`` both group under ``model.h0``. A name with fewer
    tokens than that keeps what it has (``gridfile.nc`` groups under
    ``gridfile*``).

    Ordering is part of the contract: keys come out ordered by parent directory
    (first appearance) then prefix, and paths keep their input order within a
    group. Downstream output order inherits it.

    :param hf_paths: History file paths to group.
    :type hf_paths: list[pathlib.Path]
    :param delimiter: Token delimiter within the filename.
    :type delimiter: str
    :param substring_index: Number of trailing tokens to strip for the prefix.
    :type substring_index: int
    :returns: ``{'<parent_dir>/<prefix>*': [paths]}``.
    :rtype: dict[str, list[pathlib.Path]]
    """
    # One bucketing pass over the paths; rescanning the file list per prefix
    # made this quadratic in the number of streams per directory.
    directory_groups = {}
    for path in hf_paths:
        prefix = path.name.rsplit(delimiter, substring_index)[0]
        directory_groups.setdefault(path.parent, {}).setdefault(prefix, []).append(path)

    hf_groups = {}
    for parent_path, prefix_groups in directory_groups.items():
        for prefix in sorted(prefix_groups):
            hf_groups[f"{parent_path}/{prefix}*"] = prefix_groups[prefix]

    return hf_groups


def get_year_boundary_num(year, units, calendar):
    """
    Returns the raw time value of midnight, January 1 of ``year`` under the
    given time reference.

    Comparing raw time values against these boundaries reproduces year-based
    tests (``start <= time.year <= end``) without decoding every time step. A
    year the calendar cannot represent (year 0 in a ``standard`` calendar)
    steps forward to the nearest representable year, which selects the same set
    of times: no time value can fall inside the missing year.

    :param year: Calendar year of the boundary.
    :type year: int
    :param units: CF time units the result is expressed in.
    :type units: str
    :param calendar: CF calendar name.
    :type calendar: str
    :returns: Boundary expressed as a raw time value.
    :rtype: float
    """
    for candidate_year in (year, year + 1):
        try:
            boundary = cftime.datetime(candidate_year, 1, 1, calendar=calendar)
            return cftime.date2num(boundary, units, calendar=calendar)
        except ValueError:
            continue
    raise ValueError(f"Cannot represent year {year} (or {year + 1}) in calendar '{calendar}'.")


def get_year_bounds(hf_to_meta_map):
    """
    Returns the ``(min_year, max_year)`` covered by a set of history files.

    A file's year comes from the midpoint of each time bound, or from the time
    value itself when the file has no bounds.

    :param hf_to_meta_map: ``{path: netCDFMeta}`` mapping to inspect.
    :type hf_to_meta_map: dict
    :rtype: tuple[int, int]
    """
    min_year = np.inf
    max_year = -np.inf

    for path in list(hf_to_meta_map.keys()):
        meta = hf_to_meta_map[path]
        float_bounds = meta.get_float_time_bounds()
        # Midpoints are computed on the raw values, which order identically to
        # their decoded dates, so only the two extremes need decoding per file.
        if float_bounds is None:
            midpoints = np.ma.getdata(np.atleast_1d(meta.get_float_times()))
            decode = meta.decode_time_values
        else:
            bounds = np.ma.getdata(float_bounds)
            midpoints = bounds[:, 0] + (bounds[:, 1] - bounds[:, 0]) / 2
            decode = meta.decode_time_bounds_values

        extremes = np.atleast_1d(decode(np.array([np.min(midpoints), np.max(midpoints)])))
        if extremes[-1].year > max_year:
            max_year = extremes[-1].year
        if extremes[0].year < min_year:
            min_year = extremes[0].year
    return min_year, max_year


def get_group_timestep_delta(metas):
    """
    Computes the duration of one time step for a group of history files.

    The duration is the gap between the two latest time values in the group, i.e.
    the resolution at the end of the record. Fragmented tiles share every time
    step, so their two latest values are identical and the delta is zero.

    Only the two latest steps *per file* are compared as CFTime objects; each
    file's pair is picked in linear time from its raw float times, which are
    monotonic with the decoded values. That avoids an object-dtype sort over
    every step in the group while still ordering files with differing
    ``units``/``calendar`` correctly.

    :param metas: Metadata objects for the history files in one group.
    :type metas: list[gents.meta.netCDFMeta]
    :returns: Duration of one time step.
    :rtype: datetime.timedelta
    :raises ValueError: If the group holds fewer than two time steps in total.
    """
    latest_candidates = []
    total_steps = 0
    for meta in metas:
        # Time coordinates are never masked; dropping the unused netCDF mask
        # keeps the partition routines on plain arrays and warning-free.
        float_times = np.ma.getdata(np.atleast_1d(meta.get_float_times()))
        total_steps += float_times.shape[0]

        if float_times.shape[0] <= 2:
            candidates = float_times
        else:
            candidates = float_times[np.argpartition(float_times, -2)[-2:]]
        latest_candidates.append(np.atleast_1d(meta.decode_time_values(candidates)))

    if total_steps < 2:
        raise ValueError(f"Expected time array of size 2 or greater, got {total_steps}.")

    pooled = np.concatenate(latest_candidates)
    latest_pair = np.partition(pooled, pooled.shape[0] - 2)[-2:]
    latest_pair.sort()
    return latest_pair[1] - latest_pair[0]


def is_ds_within_years(ds_meta, min_year, max_year):
    """
    Returns whether a file's representative time falls within a year range.

    The representative year is the midpoint of the first time bound, or the first
    time value when the file has no bounds.

    :param ds_meta: Metadata object for the file to check.
    :type ds_meta: gents.meta.netCDFMeta
    :param min_year: Lower bound of the range (inclusive).
    :type min_year: int
    :param max_year: Upper bound of the range (inclusive).
    :type max_year: int
    :rtype: bool
    """
    time_bounds = ds_meta.get_cftime_bounds()
    if time_bounds is not None:
        year = (time_bounds[0].year + time_bounds[1].year) / 2
    else:
        year = ds_meta.get_cftimes()[0]

    if min_year <= year <= max_year:
        return True
    else:
        return False


def filter_by_variables(meta_datasets):
    """
    Splits history files by variable set, majority against the rest.

    Files are fingerprinted by their sorted variable names. If no single
    fingerprint is most common, both returned values are ``None``.

    :param meta_datasets: Metadata objects to examine.
    :type meta_datasets: list[gents.meta.netCDFMeta]
    :returns: ``(majority, others)``; ``others`` is ``None`` when every file
        shares the same variable set.
    :rtype: tuple[list or None, list or None]
    """
    variable_sets = {}
    for index in range(len(meta_datasets)):
        var_set = meta_datasets[index].get_variables()
        var_set.sort()

        if str(var_set) in variable_sets:
            variable_sets[str(var_set)].append(index)
        else:
            variable_sets[str(var_set)] = [index]

    majority = None
    others = None
    
    if len(variable_sets) > 1:
        counts = []
        for var_set in variable_sets:
            counts.append(len(variable_sets[var_set]))

        majority_index = np.argmax(counts)
        if np.sum(counts[majority_index] == np.array(counts)) == 1:
            majority_set = list(variable_sets)[majority_index]
    
            majority = []
            others = []
            for index in range(len(meta_datasets)):
                if index in variable_sets[majority_set]:
                    majority.append(meta_datasets[index])
                else:
                    others.append(meta_datasets[index])
    else:
        majority = meta_datasets
    
    return (majority, others)


def sort_metas_by_time(metas):
    """
    Returns a new list of metadata objects sorted by their first time value.

    :param metas: Unsorted metadata objects; not modified.
    :type metas: list[gents.meta.netCDFMeta]
    :rtype: list[gents.meta.netCDFMeta]
    """
    # One decoded value per file: CFTime keys keep cross-file comparisons valid
    # even when files carry different time units.
    return sorted(
        metas,
        key=lambda meta: meta.decode_time_values(np.ma.getdata(np.atleast_1d(meta.get_float_times()))[0]),
    )

    
def check_groups_by_variables(sliced_groups):
    """
    Drops files whose variable set differs from the majority of their group.

    Minority files are discarded with a warning and the survivors re-sorted by
    time; a group with no clear majority is dropped entirely.

    :param sliced_groups: ``{group ID: [netCDFMeta]}`` to filter.
    :type sliced_groups: dict
    :returns: The same mapping, holding only majority-consistent files.
    :rtype: dict
    """
    filtered_sliced_groups = {}
    for group in sliced_groups:
        meta_datasets = sliced_groups[group]
        majority, others = filter_by_variables(meta_datasets)
        if majority is not None:
            filtered_sliced_groups[group] = sort_metas_by_time(majority)
            if others is not None:
                for meta_ds in others:
                    logger.log(LOG_LEVEL_IO_WARNING, f"Dataset has inconsistent variable list with directory group: {meta_ds.get_path()}")
        else:
            logger.log(LOG_LEVEL_IO_WARNING, f"Unable to determine majority dataset, check variable configurations between directory groups, group ID: {group}")
    return filtered_sliced_groups


def merge_fragmented_groups(hf_groups, hf_meta_map):
    """
    Merges spatially fragmented (tiled) history file groups into single groups.

    A group is taken to be fragmented when its first path does not end in ``.nc``
    (tiles look like ``*.nc.0001``). Fragmented groups sharing the same non-time
    dimension bounds are merged under one wildcard key; other groups pass through
    unchanged.

    :param hf_groups: ``{group pattern: [paths]}`` to merge.
    :type hf_groups: dict
    :param hf_meta_map: ``{path: netCDFMeta}``, used for dimension bounds.
    :type hf_meta_map: dict
    :returns: New group mapping with fragmented groups merged.
    :rtype: dict
    :raises KeyError: If a merged group's label collides with an existing group.
    """
    new_groups = {}
    fragmented_groups = {}

    for pattern in hf_groups:
        init_path = str(hf_groups[pattern][0])
        if init_path[-3:] == '.nc':
            new_groups[pattern] = hf_groups[pattern]
        else:
            fragmented_groups[pattern] = hf_groups[pattern]

    if len(fragmented_groups) > 0:
        num_fragmented_files = sum([len(fragmented_groups[pattern]) for pattern in fragmented_groups])
        logger.info(f"Found {num_fragmented_files} spatially fragmented files in {len(fragmented_groups)} groups.")

    dim_hashes = {}
    for pattern in fragmented_groups:
        dims = hf_meta_map[fragmented_groups[pattern][0]].get_dim_bounds()
        dims = {variable: dims[variable] for variable in dims if variable != "time"}
        dims_hash = str(dims)

        if dims_hash not in dim_hashes:
            dim_hashes[dims_hash] = []
        
        for path in fragmented_groups[pattern]:
            dim_hashes[dims_hash].append(path)

    for dim_hash in dim_hashes:
        paths = dim_hashes[dim_hash]
        label = str(paths[0]).split(".nc")[0] + "*"
        if label not in new_groups:
            new_groups[label] = paths
        else:
            raise KeyError(f"Fragmentation merge failed! History file group '{label}' already exists. Try filtering to fragmented files only to minimize confusion with non-fragmented history files.")

    return new_groups


class HFCollection:
    """
    A set of history files and their metadata.

    Holds a ``{path: netCDFMeta | None}`` mapping whose metadata is read lazily by
    :meth:`pull_metadata`, so path-glob filters can run before any file is opened.
    Every filter and transform returns a new instance, and a copy inherits the
    metadata already read.

    Built for *viable history files*: files without a usable time coordinate are
    dropped and a group holding a single time step raises. Use
    :func:`find_files` / :func:`sort_hf_groups` directly to inspect a raw case
    tree.
    """

    def __init__(self, hf_dir, num_processes=1, meta_map=None, hf_groups=None, step_map=None, hf_glob_pattern="*.nc*", dask_client=None, multistep_slice_map={}):
        """
        Discovers history files under ``hf_dir``, without reading their metadata.

        The pre-computed arguments below are how :meth:`copy` hands state to a
        derived collection; callers normally pass only the first few.

        :param hf_dir: Root directory to search for history files.
        :type hf_dir: str
        :param num_processes: Worker processes used for parallel metadata reads.
        :type num_processes: int
        :param meta_map: Pre-populated ``{path: netCDFMeta}`` mapping.
        :type meta_map: dict or None
        :param hf_groups: Pre-computed group mapping.
        :type hf_groups: dict or None
        :param step_map: Pre-computed ``{path: timedelta}`` mapping.
        :type step_map: dict or None
        :param hf_glob_pattern: ``fnmatch`` pattern used to discover files.
        :type hf_glob_pattern: str
        :param multistep_slice_map: Pre-computed slice indices for multi-timestep
            files (see :meth:`get_multistep_slices`).
        :type multistep_slice_map: dict
        :param dask_client: Deprecated. Pass ``num_processes`` instead.
        :raises FileNotFoundError: If no file under ``hf_dir`` matches the pattern.
        """
        if dask_client is not None:
            warnings.warn("Dask is no longer implemented in GenTS. Use the 'num_processes' argument to enable parallelism or reference the ReadTheDocs for using Dask..", DeprecationWarning, stacklevel=2)

        self.__raw_paths = find_files(hf_dir, hf_glob_pattern)
        self.__num_processes = num_processes

        if len(self.__raw_paths) == 0:
            raise FileNotFoundError(f"No files matching '{hf_glob_pattern}' found in '{hf_dir}'")

        self.__hf_to_meta_map = {}
        self.__hf_multistep_slices = multistep_slice_map
        if meta_map is None:
            for path in self.__raw_paths:
                self.__hf_to_meta_map[path] = None
        else:
            self.__hf_to_meta_map = meta_map
        
        self.__hf_groups = hf_groups
        self.__hf_dir = hf_dir

        if meta_map is None and hf_groups is None:
            logger.info(f"Initialized HFCollection at '{hf_dir}'")
            logger.info(f"{len(self.__raw_paths)} netCDF files found.")

        self.__hf_to_timestep_delta_map = step_map

    def __getitem__(self, key):
        return self.__hf_to_meta_map[key]

    def __contains__(self, key):
        return key in self.__hf_to_meta_map

    def __iter__(self):
        return iter(self.__hf_to_meta_map)

    def __len__(self):
        return len(self.__hf_to_meta_map)

    def items(self):
        return self.__hf_to_meta_map.items()

    def values(self):
        return self.__hf_to_meta_map.values()

    def keys(self):
        return self.__hf_to_meta_map.keys()

    def is_pulled(self):
        """
        Returns whether metadata has been loaded for every file in the collection.

        :rtype: bool
        """
        for path in self.__hf_to_meta_map:
            if self.__hf_to_meta_map[path] is None:
                return False
        return True

    def get_multistep_slices(self, hf_path):
        """
        Returns where to cut a multi-timestep history file that straddles a slice
        boundary, or ``None`` if it does not need cutting.

        :param hf_path: Path to the history file.
        :type hf_path: pathlib.Path
        :returns: ``{'<start>-<end>': (start_index, end_index)}``, one entry per
            slice the file contributes to.
        :rtype: dict or None
        :raises KeyError: If the path is not in this collection.
        """
        self.check_pulled()
        if hf_path in self.__hf_multistep_slices:
            return self.__hf_multistep_slices[hf_path]
        elif hf_path not in self.__hf_to_meta_map:
            raise KeyError(f"History file '{hf_path}' not found in HFCollection")
        else:
            return None

    def get_timestep_delta(self, hf_path):
        """
        Returns the duration of one time step in the file's group, pulling
        metadata first if necessary.

        :param hf_path: Path to the history file.
        :type hf_path: pathlib.Path
        :rtype: datetime.timedelta
        """
        self.check_pulled()
        return self.__hf_to_timestep_delta_map[hf_path]

    def get_input_dir(self):
        """
        Returns the head directory this collection was initialised from.

        :rtype: str
        """
        return self.__hf_dir

    def check_pulled(self):
        """
        Pulls metadata if it has not been pulled already.
        """
        if not self.is_pulled():
            self.pull_metadata()

    def copy(self, num_processes=None, meta_map=None, hf_groups=None, step_map=None, multistep_slice_map=None):
        """
        Returns a new collection derived from this one, with optional overrides.

        Shares the original's ``hf_dir``. Every filter and transform returns
        through here, which is what keeps the API immutable. Arguments left
        ``None`` are inherited.

        :param num_processes: Worker process count for the copy.
        :type num_processes: int or None
        :param meta_map: Metadata map to assign to the copy.
        :type meta_map: dict or None
        :param hf_groups: Group mapping to assign to the copy.
        :type hf_groups: dict or None
        :param step_map: Timestep delta mapping to assign to the copy.
        :type step_map: dict or None
        :param multistep_slice_map: Multistep slice indices to assign to the copy.
        :type multistep_slice_map: dict or None
        :rtype: HFCollection
        """
        if num_processes is None:
            num_processes = self.__num_processes
        if meta_map is None:
            meta_map = self.__hf_to_meta_map
        if hf_groups is None and self.is_pulled():
            hf_groups = self.get_groups()
        if step_map is None:
            step_map = self.__hf_to_timestep_delta_map
        if multistep_slice_map is None:
            multistep_slice_map = self.__hf_multistep_slices
        return HFCollection(self.__hf_dir, num_processes=num_processes, meta_map=meta_map, hf_groups=hf_groups, step_map=step_map, multistep_slice_map=multistep_slice_map)

    def sort_along_time(self):
        """
        Returns a new collection with its files ordered by their first time value.

        :rtype: HFCollection
        """
        self.check_pulled()

        sorted_map = dict(sorted(self.__hf_to_meta_map.items(), key=lambda item: item[1].get_float_times()[0]))
        self.__hf_to_meta_map = sorted_map
        logger.info(f"Sorted along time.")
        return self.copy(meta_map=sorted_map)
    
    def pull_metadata(self, check_valid=True, raise_errors=False, show_progress=True):
        """
        Reads the header of every history file in the collection.

        Runs :func:`~gents.meta.get_meta_from_path` over a process pool when
        ``num_processes > 1`` and serially otherwise, then computes each group's
        timestep delta.

        :param check_valid: Run :meth:`check_validity` afterwards, dropping files
            with invalid or incomplete metadata.
        :type check_valid: bool
        :param raise_errors: Raise per-file failures instead of logging them and
            carrying on.
        :type raise_errors: bool
        :param show_progress: If ``False``, suppress the stdout progress bar.
        :type show_progress: bool
        :raises ValueError: If any group holds fewer than two time steps in total,
            regardless of ``raise_errors``.
        """
        logger.info(f"Pulling metadata...")
        paths = list(self.__hf_to_meta_map.keys())

        prog_bar = ProgressBar(total=len(paths), label="Pulling Metadata", quiet=not show_progress)
        if self.__num_processes > 1:
            with ProcessPoolExecutor(max_workers=self.__num_processes) as executor:
                futures = {executor.submit(get_meta_from_path, path): path for path in paths}
                for future in as_completed(futures):
                    path = futures[future]
                    try:
                        result = future.result()
                        self.__hf_to_meta_map[path] = result
                    except Exception as exc:
                        logger.warning(f"Failed to load metadata for {path}: {exc}", exc_info=True)
                        if raise_errors:
                            raise
                    finally:
                        prog_bar.step()
        else:
            for path in paths:
                try:
                    self.__hf_to_meta_map[path] = get_meta_from_path(path)
                except Exception as exc:
                    logger.warning(f"Failed to load metadata for {path}: {exc}", exc_info=True)
                    if raise_errors:
                        raise
                finally:
                    prog_bar.step()

        if check_valid:
            self.check_validity()
        else:
            logger.warning(f"Skipping metadata validation may result in errors due to missing attributes or coordinate data.")
        logger.info(f"Metadata pulled.")

        if self.__hf_to_timestep_delta_map is None:
            self.__hf_to_timestep_delta_map = {}
            for group, group_paths in self.get_groups().items():
                metas = [self.__hf_to_meta_map[path] for path in group_paths]
                try:
                    delta = get_group_timestep_delta(metas)
                except ValueError as exc:
                    raise ValueError(f"{exc} Group with paths: {group_paths}") from exc
                for path in group_paths:
                    self.__hf_to_timestep_delta_map[path] = delta

    def check_validity(self):
        """
        Drops files whose metadata is missing or not
        :meth:`~gents.meta.netCDFMeta.is_valid`, warning about each.

        :returns: The removed ``{path: metadata}`` entries.
        :rtype: dict
        """
        logger.debug(f"Validating metadata...")
        new_map = {}
        removed = {}
        for path in self.__hf_to_meta_map:
            if self.__hf_to_meta_map[path] is not None and self.__hf_to_meta_map[path].is_valid():
                new_map[path] = self.__hf_to_meta_map[path]
            else:
                removed[path] = self.__hf_to_meta_map[path]
                logger.warning(f"Could not pull valid/complete metadata for '{path}'.")
        self.__hf_to_meta_map = new_map
        logger.debug(f"{len(new_map)} files valdiated ({len(removed)} removed).")
        return removed
    
    def include_patterns(self, glob_patterns):
        """
        .. deprecated::
            Use :meth:`include` instead.
        """
        warnings.warn("TSCollection.include_patterns is deprecated in favor of TSCollection.include")
        return self.include(glob_patterns)

    def exclude_patterns(self, glob_patterns):
        """
        .. deprecated::
            Use :meth:`exclude` instead.
        """
        warnings.warn("TSCollection.exclude_patterns is deprecated in favor of TSCollection.exclude")
        return self.exclude(glob_patterns)

    def include(self, glob_patterns):
        """
        Returns a new collection holding only files matching at least one pattern.

        Patterns are ``fnmatch`` globs tested against absolute path strings. An
        empty list matches nothing and so empties the collection.

        :param glob_patterns: One or more glob patterns; a single string is also
            accepted.
        :type glob_patterns: list[str] or str
        :rtype: HFCollection
        """
        if type(glob_patterns) is str:
            glob_patterns = [glob_patterns]

        filtered_path_map = {}
        for path in self.__hf_to_meta_map:
            for pattern in glob_patterns:
                if fnmatch.fnmatch(str(path), pattern):
                    filtered_path_map[path] = self.__hf_to_meta_map[path]
                    break
        logger.debug(f"Inclusive filter(s) applied: '{glob_patterns}'")
        return self.copy(meta_map=filtered_path_map)

    def exclude(self, glob_patterns):
        """
        Returns a new collection with files matching any pattern removed.

        Patterns are ``fnmatch`` globs tested against absolute path strings.

        :param glob_patterns: One or more glob patterns; a single string is also
            accepted.
        :type glob_patterns: list[str] or str
        :rtype: HFCollection
        """
        if type(glob_patterns) is str:
            glob_patterns = [glob_patterns]

        filtered_path_map = {}
        for path in self.__hf_to_meta_map:
            matches = False
            for pattern in glob_patterns:
                if fnmatch.fnmatch(str(path), pattern):
                    matches = True
                
            if not matches:
                filtered_path_map[path] = self.__hf_to_meta_map[path]
        logger.debug(f"Exclusive filter(s) applied: '{glob_patterns}'")
        return self.copy(meta_map=filtered_path_map)

    def include_years(self, start_year, end_year, glob_patterns=["*"]):
        """
        Returns a new collection holding only files within a year range.

        A file's year is the midpoint of its first time bound, or its first time
        value when it has no bounds. Metadata is pulled if necessary, so prefer
        :meth:`include` when a path filter would do.

        :param start_year: First year in the range (inclusive).
        :type start_year: int
        :param end_year: Last year in the range (inclusive).
        :type end_year: int
        :param glob_patterns: Restricts which files the year filter applies to.
        :type glob_patterns: list[str]
        :rtype: HFCollection
        """
        self.check_pulled()
        filtered_path_map = {}
        remove_paths = []
        for pattern in glob_patterns:
            for path in self.__hf_to_meta_map:
                if fnmatch.fnmatch(path, pattern):
                    meta_ds = self.__hf_to_meta_map[path]
                    float_bounds = meta_ds.get_float_time_bounds()
                    if float_bounds is not None:
                        first_pair = np.ma.getdata(float_bounds)[0]
                        midpoint = first_pair[0] + (first_pair[1] - first_pair[0]) / 2
                        time = meta_ds.decode_time_bounds_values(midpoint)
                    else:
                        time = meta_ds.decode_time_values(np.ma.getdata(np.atleast_1d(meta_ds.get_float_times()))[0])
                    
                    if start_year <= time.year <= end_year:
                        filtered_path_map[path] = self.__hf_to_meta_map[path]

        logger.debug(f"Filtered from {start_year} to {end_year} applied to following glob patterns: '{glob_patterns}'")
        hf_groups = None
        if self.__hf_groups is not None:
            hf_groups = sort_hf_groups(list(filtered_path_map.keys()))

        return self.copy(meta_map=filtered_path_map, hf_groups=hf_groups)

    def get_groups(self, check_fragmented=True):
        """
        Returns the collection's ``{group ID: [paths]}`` mapping.

        Groups are built by :func:`sort_hf_groups` on the first call and cached.

        :param check_fragmented: Also merge spatially tiled groups via
            :func:`merge_fragmented_groups`, which requires metadata.
        :type check_fragmented: bool
        :rtype: dict[str, list[pathlib.Path]]
        """
        if self.__hf_groups is None:
            self.__hf_groups = sort_hf_groups(list(self.__hf_to_meta_map.keys()))
        
            if check_fragmented:
                self.check_pulled()
                self.__hf_groups = merge_fragmented_groups(self.__hf_groups, self.__hf_to_meta_map)

        return self.__hf_groups

    def slice_groups(self, slice_size_years=10, start_year=0, pattern="*", time_alignment_method="midpoint"):
        """
        Returns a new collection with its groups partitioned into year windows.

        Each group is split into windows of ``slice_size_years``, and every file
        assigned to the window its representative time falls in. Files straddling
        a boundary record per-slice cut points (see :meth:`get_multistep_slices`).
        Sub-group keys gain a ``[sorting_pivot]<start>-<end>`` suffix, which
        :class:`~gents.timeseries.TSCollection` parses back out.

        :param slice_size_years: Maximum width of each window in years.
        :type slice_size_years: int
        :param start_year: Year to align windows to; ``None`` uses the collection's
            own earliest year.
        :type start_year: int or None
        :param pattern: ``fnmatch`` glob restricting which groups are sliced.
        :type pattern: str
        :param time_alignment_method: How to pick a file's representative time:
            ``'midpoint'`` of its first time bound, ``'direct_time'`` (ignoring
            bounds), ``'start_bound'`` or ``'end_bound'``.
        :type time_alignment_method: str
        :rtype: HFCollection
        :raises ValueError: If ``time_alignment_method`` is not one of those four.
        """
        sliced_groups = {}
        self.check_pulled()

        for group in self.get_groups():
            hf_paths = self.get_groups()[group]
            if not fnmatch.fnmatch(group, pattern):
                sliced_groups[group] = hf_paths
                continue
            
            if len(hf_paths) == 1:
                sliced_groups[group] = hf_paths
                warnings.warn("Cannot slice history file group of size 1.", RuntimeWarning)
                continue

            group_meta_map = {path: self.__hf_to_meta_map[path] for path in hf_paths}
            
            min_year, max_year = get_year_bounds(group_meta_map)
            if start_year is not None:
                min_year = start_year
            
            time_slices = calculate_year_slices(slice_size_years, min_year, max_year)

            hf_slices = {}
            boundary_cache = {}
            for hf_path in hf_paths:
                meta_ds = self.__hf_to_meta_map[hf_path]
                float_bnds = meta_ds.get_float_time_bounds()
                if float_bnds is None or time_alignment_method == "direct_time":
                    times = np.ma.getdata(np.atleast_1d(meta_ds.get_float_times()))
                    ref_units = meta_ds.get_time_units()
                    ref_calendar = meta_ds.get_time_calendar()
                else:
                    bnds = np.ma.getdata(float_bnds)
                    if time_alignment_method == "midpoint":
                        times = bnds[:, 0] + (bnds[:, 1] - bnds[:, 0]) / 2
                    elif time_alignment_method == "start_bound":
                        times = bnds[:, 0]
                    elif time_alignment_method == "end_bound":
                        times = bnds[:, 1]
                    else:
                        raise ValueError(f"'{time_alignment_method}' is an invalid time-alignment method. Valid methods are ['direct_time', 'midpoint', 'start_bound', 'end_bound']")
                    ref_units = meta_ds.get_time_bounds_units()
                    ref_calendar = meta_ds.get_time_bounds_calendar()

                for time_slice in time_slices:
                    cache_key = (time_slice, ref_units, ref_calendar)
                    if cache_key not in boundary_cache:
                        boundary_cache[cache_key] = (
                            get_year_boundary_num(time_slice[0], ref_units, ref_calendar),
                            get_year_boundary_num(time_slice[1] + 1, ref_units, ref_calendar),
                        )
                    lower_num, upper_num = boundary_cache[cache_key]

                    in_window = (times >= lower_num) & (times < upper_num)
                    if not in_window.any():
                        continue
                    start_index = int(np.argmax(in_window))
                    if time_slice in hf_slices:
                        hf_slices[time_slice].append(hf_path)
                    else:
                        hf_slices[time_slice] = [hf_path]

                    if len(times) > 1:
                        past_window = times >= upper_num
                        end_index = int(np.argmax(past_window)) if past_window.any() else len(times) - 1
                        if start_index != 0 or times[-1] >= upper_num:
                            if hf_path in self.__hf_multistep_slices:
                                assert f"{time_slice[0]}-{time_slice[1]}" not in self.__hf_multistep_slices[hf_path]
                                self.__hf_multistep_slices[hf_path][f"{time_slice[0]}-{time_slice[1]}"] = (start_index, end_index)
                            else:
                                self.__hf_multistep_slices[hf_path] = {f"{time_slice[0]}-{time_slice[1]}": (start_index, end_index)}
            for time_slice in hf_slices:
                sliced_groups[f"{group}[sorting_pivot]{time_slice[0]}-{time_slice[1]}"] = hf_slices[time_slice]
        logger.debug(f"Slicing groups into {slice_size_years} year long slices for '{pattern}'.")
        return self.copy(hf_groups=sliced_groups)
