#!/usr/bin/env python
"""
hfcollection.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 04/30/25
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


def check_config(config):
    """
    Validates that a configuration dictionary contains the required keys and types.

    Asserts the presence of ``'name'``, ``'include'``, and ``'exclude'`` keys and
    that their values are of the expected types.

    :param config: Configuration dictionary to validate.
    :type config: dict
    :raises AssertionError: If any required key is missing or has an unexpected type.
    """
    assert "name" in config
    assert "include" in config
    assert "exclude" in config
    assert type(config["include"]) is dict or config["include"] is None
    assert type(config["exclude"]) is list or config["exclude"] is None


def get_default_config():
    """
    Returns a configuration dictionary populated with default GenTS settings.

    :returns: Dictionary with ``name="default"``, ``include=None``,
        ``exclude=None``.
    :rtype: dict
    """
    return {
        "name": "default",
        "include": None,
        "exclude": None
    }


def find_files(head_path, pattern):
    """
    Recursively searches a directory tree for files matching a glob pattern.

    Walks ``head_path`` with ``os.walk`` and collects every file whose name
    matches ``pattern`` via ``fnmatch``.

    :param head_path: Root directory to begin the recursive search from.
    :type head_path: str or pathlib.Path
    :param pattern: ``fnmatch``-style wildcard pattern to match file names
        against (e.g. ``'*.nc'``).
    :type pattern: str
    :returns: Sorted list of matching file paths.
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
    Computes non-overlapping year-range tuples covering a given span.

    Each slice is at most ``slice_size_years`` wide.  The upper boundary is
    aligned by rounding ``max_year`` up to the next multiple of
    ``slice_size_years``.  Returns a single tuple if the full span fits within
    one slice.

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


def find_all_indices(string, substring):
    """
    Returns all start indices where ``substring`` occurs within ``string``.

    Uses a sliding-window ``str.find`` loop so overlapping occurrences are
    all reported.

    :param string: The string to search in.
    :type string: str
    :param substring: The substring to search for.
    :type substring: str
    :returns: List of integer indices where ``substring`` begins.
    :rtype: list[int]
    """
    indices = []
    start = 0
    while True:
        index = string.find(substring, start)
        if index == -1:
            break
        indices.append(index)
        start = index + 1
    return indices


def sort_hf_groups(hf_paths, delimiter=".", substring_index=2):
    """
    Groups history file paths by directory and shared filename prefix.

    Files are first grouped by their parent directory, then within each
    directory by a common filename prefix derived by dropping the last
    ``substring_index`` ``delimiter``-delimited tokens from each filename.

    For example, ``model.h0.0001-01.nc`` and ``model.h0.0001-02.nc`` share
    the prefix ``model.h0`` and end up in the same group.

    :param hf_paths: List of history file paths to group.
    :type hf_paths: list[pathlib.Path]
    :param delimiter: Token delimiter used to parse the filename prefix.
        Defaults to ``'.'``.
    :type delimiter: str
    :param substring_index: Number of trailing delimiter-separated tokens to
        strip when deriving the group prefix. Defaults to ``2``.
    :type substring_index: int
    :returns: Dictionary mapping ``'<parent_dir>/<prefix>*'`` pattern strings
        to lists of matching file paths.
    :rtype: dict[str, list[pathlib.Path]]
    """
    directory_groups = {}
    for path in hf_paths:
        if path.parent in directory_groups:
            directory_groups[path.parent].append(path)
        else:
            directory_groups[path.parent]= [path]

    hf_groups = {}
    for parent_path in directory_groups:
        group_paths = [path for path in directory_groups[parent_path]]
        substrings = []
        for path in group_paths:
            parsed = path.name[:find_all_indices(path.name, delimiter)[-1 * substring_index]]
            substrings.append(parsed)
        
        for substring in np.unique(substrings):
            hf_groups[f"{parent_path}/{substring}*"] = []
            for path in group_paths:
                parsed = path.name[:find_all_indices(path.name, delimiter)[-1 * substring_index]]
                if substring == parsed:
                    hf_groups[f"{parent_path}/{substring}*"].append(path)
        
    return hf_groups


def get_year_bounds(hf_to_meta_map):
    """
    Determines the minimum and maximum year covered by a set of history files.

    Uses the midpoint of each time bound (or the time value itself if no bounds
    are present) to determine which year each file belongs to.

    :param hf_to_meta_map: Dictionary mapping file paths to their
        :class:`~gents.meta.netCDFMeta` objects.
    :type hf_to_meta_map: dict
    :returns: Tuple of ``(min_year, max_year)`` as integers.
    :rtype: tuple[int, int]
    """
    min_year = np.inf
    max_year = -np.inf
    
    for path in list(hf_to_meta_map.keys()):
        time_bounds = hf_to_meta_map[path].get_cftime_bounds()
        if time_bounds is None:
            time_bounds = []
            for ts in hf_to_meta_map[path].get_cftimes():
                time_bounds.append([ts, ts])
        
        for index in range(len(time_bounds)):
            lower_bound, upper_bound = time_bounds[index]
            
            mid_time = lower_bound + ((upper_bound - lower_bound) / 2)

            if mid_time.year > max_year:
                max_year = mid_time.year
            if mid_time.year < min_year:
                min_year = mid_time.year
    return min_year, max_year


def generate_output_template(hf_head_dir, group_path_id, output_head_dir=None, directory_swaps={"hist": "tseries"}, filename_delimiter=".", cutoff_index=None):
    """
    Constructs a time-series output path template from a history file group path.

    Builds the output path (excluding the variable-name and timestamp suffix) by
    extracting the subdirectory structure relative to ``hf_head_dir``, applying
    any ``directory_swaps`` renames, and stripping date tokens from the filename
    prefix up to ``cutoff_index``.

    :param hf_head_dir: Head directory used when reading the history files.
    :type hf_head_dir: str
    :param group_path_id: Group path pattern produced by :func:`sort_hf_groups`
        (e.g. ``'/data/hist/model.h0*'``).
    :type group_path_id: str or pathlib.Path
    :param output_head_dir: Alternate head directory for output. Defaults to
        ``None`` (uses ``hf_head_dir``).
    :type output_head_dir: str or None
    :param directory_swaps: Mapping of directory name substrings to replace
        (e.g. ``{'hist': 'tseries'}``). Defaults to ``{'hist': 'tseries'}``.
    :type directory_swaps: dict
    :param filename_delimiter: Delimiter used to split the filename into tokens.
        Defaults to ``'.'``.
    :type filename_delimiter: str
    :param cutoff_index: Character index at which to truncate the filename prefix.
        Defaults to ``None`` (cuts at the last delimiter occurrence).
    :type cutoff_index: int or None
    :returns: Path template for time-series output (without variable/timestamp suffix).
    :rtype: pathlib.Path
    """
    group_path_id = Path(group_path_id)
                         
    raw_filename_prefix = group_path_id.name
    if cutoff_index is None:
        cutoff_index = find_all_indices(raw_filename_prefix, ".")[-1]
    filename_prefix = raw_filename_prefix[:cutoff_index]
    
    sub_dir_structure = (str(group_path_id.parent).split(hf_head_dir)[-1]).split("/")

    for key in directory_swaps:
        for index in range(len(sub_dir_structure)):
            if sub_dir_structure[index] == key:
                sub_dir_structure[index] = directory_swaps[key]

    sub_dir_path = "/"
    for directory in sub_dir_structure:
        sub_dir_path += f"{directory}/"

    if output_head_dir is None:
        output_template = Path(f"{hf_head_dir}/{sub_dir_path}/{filename_prefix}")
    else:
        output_template = Path(f"{output_head_dir}/{sub_dir_path}/{filename_prefix}")
    return output_template


def is_ds_within_years(ds_meta, min_year, max_year):
    """
    Checks whether a dataset's representative time falls within a year range.

    Uses the midpoint of the first time bound as the representative year, or
    the first time value directly if no time bounds are present.

    :param ds_meta: Metadata object for the dataset to check.
    :type ds_meta: gents.meta.netCDFMeta
    :param min_year: Lower bound of the year range (inclusive).
    :type min_year: int
    :param max_year: Upper bound of the year range (inclusive).
    :type max_year: int
    :returns: ``True`` if the representative year falls within
        ``[min_year, max_year]``, ``False`` otherwise.
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
    Identifies the majority variable set among a list of history file metadata objects.

    Groups metadata objects by their sorted variable-name fingerprint and
    returns the set belonging to the most common variable list alongside any
    outliers.

    :param meta_datasets: List of metadata objects to examine.
    :type meta_datasets: list[gents.meta.netCDFMeta]
    :returns: Tuple of ``(majority, others)`` where ``majority`` is the list of
        metadata objects sharing the most common variable set and ``others``
        contains the remainder.  ``others`` is ``None`` if all objects share
        the same variable set.
    :rtype: tuple[list, list or None]
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
    Returns a new list of metadata objects sorted by their first CFTime value.

    Performs an insertion sort; the original list is not modified.

    :param metas: Unsorted list of metadata objects.
    :type metas: list[gents.meta.netCDFMeta]
    :returns: New list sorted in ascending time order.
    :rtype: list[gents.meta.netCDFMeta]
    """
    time_sorted_metas = [metas[0]]

    if len(metas) > 1:
        for meta in metas[1:]:
            meta_sorted = False
            
            for index in range(len(time_sorted_metas)):
                if meta.get_cftimes()[0] < time_sorted_metas[index].get_cftimes()[0]:
                    time_sorted_metas.insert(index, meta)
                    meta_sorted = True
                    break
            if not meta_sorted:
                time_sorted_metas.append(meta)

    return time_sorted_metas

    
def check_groups_by_variables(sliced_groups):
    """
    Filters history file groups to ensure variable-set consistency within each group.

    For each group, calls :func:`filter_by_variables` to identify the majority
    variable set, discards minority files with a logged warning, and re-sorts the
    retained files by time via :func:`sort_metas_by_time`.  Groups for which no
    majority can be determined are dropped entirely with a warning.

    :param sliced_groups: Dictionary mapping group IDs to lists of
        :class:`~gents.meta.netCDFMeta` objects.
    :type sliced_groups: dict
    :returns: Filtered dictionary containing only the majority-consistent metadata
        objects per group, sorted by time.
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
    Merges spatially fragmented (tiled) history file groups into unified groups.

    Iterates through ``hf_groups`` and separates fragmented files (identified by
    paths that do not end with ``.nc``) from standard files.  Fragmented groups
    are hashed by their non-time dimension bounds; groups sharing the same hash
    are merged into a single entry under a new wildcard key.  Non-fragmented
    files are passed through unchanged.

    :param hf_groups: Dictionary mapping group pattern strings to lists of
        history file paths.
    :type hf_groups: dict
    :param hf_meta_map: Dictionary mapping file paths to their
        :class:`~gents.meta.netCDFMeta` objects (used to retrieve dimension bounds).
    :type hf_meta_map: dict
    :returns: New group dictionary with fragmented groups merged.
    :rtype: dict
    :raises KeyError: If a merged fragmented group label already exists among
        the non-fragmented groups.
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
    Manages a collection of netCDF history files and their metadata.

    Holds a ``{path: netCDFMeta | None}`` mapping and lazily loads metadata
    on demand via :meth:`pull_metadata`.  All filter and slice operations return
    new ``HFCollection`` instances, preserving an immutable-style fluent API.
    """

    def __init__(self, hf_dir, num_processes=1, meta_map=None, hf_groups=None, step_map=None, hf_glob_pattern="*.nc*", dask_client=None):
        """
        Initialises the collection by discovering history files under ``hf_dir``.

        If ``meta_map`` is not supplied, all discovered files are registered with
        ``None`` metadata (populated later by :meth:`pull_metadata`).  When
        constructing a derived collection via :meth:`copy`, pre-computed maps and
        groups are passed in directly and the file-discovery log messages are
        suppressed.

        :param hf_dir: Root directory to search for history files.
        :type hf_dir: str
        :param num_processes: Maximum number of worker processes for parallel
            metadata loading. Defaults to ``1`` (single process).
        :type num_processes: int or None
        :param meta_map: Pre-populated ``{path: netCDFMeta}`` mapping. When
            supplied, overrides the recursive file search. Defaults to ``None``.
        :type meta_map: dict or None
        :param hf_groups: Pre-computed group dictionary. Defaults to ``None``.
        :type hf_groups: dict or None
        :param step_map: Pre-computed ``{path: timedelta}`` timestep delta map.
            Defaults to ``None``.
        :type step_map: dict or None
        :param hf_glob_pattern: ``fnmatch`` pattern used when searching for files.
            Defaults to ``'*.nc*'``.
        :type hf_glob_pattern: str
        :param dask_client: Deprecated. Pass ``num_processes`` instead.
        """
        if dask_client is not None:
            warnings.warn("Dask is no longer implemented in GenTS. Use the 'num_processes' argument to enable parallelism or reference the ReadTheDocs for using Dask..", DeprecationWarning, stacklevel=2)

        self.__raw_paths = find_files(hf_dir, hf_glob_pattern)
        self.__num_processes = num_processes

        if len(self.__raw_paths) == 0:
            raise FileNotFoundError(f"No files matching '{hf_glob_pattern}' found in '{hf_dir}'")

        self.__hf_to_meta_map = {}
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
        Returns whether metadata has been loaded for all files in the collection.

        :returns: ``True`` if every path has a non-``None`` metadata value,
            ``False`` otherwise.
        :rtype: bool
        """
        for path in self.__hf_to_meta_map:
            if self.__hf_to_meta_map[path] is None:
                return False
        return True

    def get_timestep_delta(self, hf_path):
        """
        Returns the pre-computed time-step duration for a given history file.

        Triggers :meth:`pull_metadata` if metadata has not yet been loaded.

        :param hf_path: Path to the history file.
        :type hf_path: pathlib.Path
        :returns: Duration of one time step as a ``cftime`` timedelta object.
        :rtype: datetime.timedelta
        """
        self.check_pulled()
        return self.__hf_to_timestep_delta_map[hf_path]

    def get_input_dir(self):
        """
        Returns the head directory this collection was initialised from.

        :returns: Root input directory path.
        :rtype: str
        """
        return self.__hf_dir

    def check_pulled(self):
        """
        Ensures metadata is loaded, triggering :meth:`pull_metadata` if necessary.
        """
        if not self.is_pulled():
            self.pull_metadata()

    def copy(self, num_processes=None, meta_map=None, hf_groups=None, step_map=None):
        """
        Creates a new ``HFCollection`` derived from this one with optional overrides.

        Shares the same ``hf_dir`` as the original.  Used as the return mechanism
        for all filter and transform operations to preserve immutability.

        :param num_processes: Worker process count for the copy. Defaults to the
            current value.
        :type num_processes: int or None
        :param meta_map: Metadata map to assign to the copy. Defaults to the
            current map.
        :type meta_map: dict or None
        :param hf_groups: Group dictionary to assign to the copy. Defaults to
            the current groups.
        :type hf_groups: dict or None
        :param step_map: Timestep delta map to assign to the copy. Defaults to
            the current map.
        :type step_map: dict or None
        :returns: New ``HFCollection`` instance.
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
        return HFCollection(self.__hf_dir, num_processes=num_processes, meta_map=meta_map, hf_groups=hf_groups, step_map=step_map)

    def sort_along_time(self):
        """
        Returns a new ``HFCollection`` with files sorted by their first time value.

        :returns: New ``HFCollection`` with the metadata map re-ordered
            in ascending time order.
        :rtype: HFCollection
        """
        self.check_pulled()

        sorted_map = dict(sorted(self.__hf_to_meta_map.items(), key=lambda item: item[1].get_float_times()[0]))
        self.__hf_to_meta_map = sorted_map
        logger.info(f"Sorted along time.")
        return self.copy(meta_map=sorted_map)
    
    def pull_metadata(self, check_valid=True, raise_errors=False):
        """
        Loads metadata for all history files in the collection in parallel.

        Submits :func:`~gents.meta.get_meta_from_path` calls to a
        ``ProcessPoolExecutor`` worker pool and populates the internal metadata
        map with the results.  After loading, computes the timestep delta for each
        group by sorting all CFTime values and taking the interval between the last
        two steps.

        :param check_valid: If ``True`` (default), calls :meth:`check_validity`
            after loading to remove files with incomplete or invalid metadata.
        :type check_valid: bool
        :param raise_errors: If ``True`` (default ``False``), calls errors are raised
            rather than just logged.
        :type raise_errors: bool
        """
        logger.info(f"Pulling metadata...")
        paths = list(self.__hf_to_meta_map.keys())

        with ProcessPoolExecutor(max_workers=self.__num_processes) as executor:
            futures = {executor.submit(get_meta_from_path, path): path for path in paths}
            results = []
            prog_bar = ProgressBar(total=len(futures), label="Pulling Metadata")
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

        if check_valid:
            self.check_validity()
        else:
            logger.warning(f"Skipping metadata validation may result in errors due to missing attributes or coordinate data.")
        logger.info(f"Metadata pulled.")

        if self.__hf_to_timestep_delta_map is None:
            self.__hf_to_timestep_delta_map = {}
            for group in self.get_groups():
                times = []
                for path in self.get_groups()[group]:
                    cftimes = self.__hf_to_meta_map[path].get_cftimes()
                    if isinstance(cftimes, (list, np.ndarray)):
                        for ts in cftimes:
                            times.append(ts)
                    else:
                        times.append(cftimes)
                times = np.sort(times)
                if len(times) < 2:
                    raise ValueError(f"Expected time array of size 2 or greater, got {len(times)} for group with paths: {self.get_groups()[group]}")
                for path in self.get_groups()[group]:
                    self.__hf_to_timestep_delta_map[path] = times[-1] - times[-2]

    def check_validity(self):
        """
        Removes history files with missing or invalid metadata from the collection.

        Iterates over the metadata map and drops any entry where the metadata is
        ``None`` or :meth:`~gents.meta.netCDFMeta.is_valid` returns ``False``,
        logging a warning for each removed file.

        :returns: Dictionary of the removed ``{path: metadata}`` entries.
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
        Returns a new collection containing only files whose paths match the patterns.

        A file is retained if its path matches *at least one* of the provided glob
        patterns via ``fnmatch``.

        :param glob_patterns: One or more ``fnmatch``-style glob patterns. A single
            string is also accepted.
        :type glob_patterns: list[str] or str
        :returns: New ``HFCollection`` restricted to matching files.
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
        Returns a new collection with files whose paths match the patterns removed.

        A file is excluded if its path matches *any* of the provided glob patterns
        via ``fnmatch``.

        :param glob_patterns: One or more ``fnmatch``-style glob patterns. A single
            string is also accepted.
        :type glob_patterns: list[str] or str
        :returns: New ``HFCollection`` with matching files removed.
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
        Returns a new collection filtered to files whose midpoint time falls within a year range.

        Only files whose paths also match ``glob_patterns`` are considered for
        filtering.  The representative year is the midpoint of the first time bound,
        or the first time value if no bounds are present.  Requires metadata to have
        been loaded.

        :param start_year: First year in the range (inclusive).
        :type start_year: int
        :param end_year: Last year in the range (inclusive).
        :type end_year: int
        :param glob_patterns: Glob patterns restricting which files are subject to
            the year filter. Defaults to ``['*']`` (all files).
        :type glob_patterns: list[str]
        :returns: New ``HFCollection`` containing only files within the year range.
        :rtype: HFCollection
        """
        self.check_pulled()
        filtered_path_map = {}
        remove_paths = []
        for pattern in glob_patterns:
            for path in self.__hf_to_meta_map:
                if fnmatch.fnmatch(path, pattern):
                    meta_ds = self.__hf_to_meta_map[path]
                    if meta_ds.get_cftime_bounds() is not None:
                        time_bnds = meta_ds.get_cftime_bounds()[0]
                        time = time_bnds[0] + ((time_bnds[1] - time_bnds[0]) / 2)
                    else:
                        time = meta_ds.get_cftimes()[0]
                    
                    if start_year <= time.year <= end_year:
                        filtered_path_map[path] = self.__hf_to_meta_map[path]

        logger.debug(f"Filtered from {start_year} to {end_year} applied to following glob patterns: '{glob_patterns}'")
        hf_groups = None
        if self.__hf_groups is not None:
            hf_groups = sort_hf_groups(list(filtered_path_map.keys()))

        return self.copy(meta_map=filtered_path_map, hf_groups=hf_groups)

    def get_groups(self, check_fragmented=True):
        """
        Returns the dictionary of history file groups.

        On the first call, groups are built by :func:`sort_hf_groups`.  If
        ``check_fragmented`` is ``True``, spatially tiled groups are additionally
        merged via :func:`merge_fragmented_groups`.  Subsequent calls return the
        cached result.

        :param check_fragmented: If ``True`` (default), detect and merge spatially
            fragmented file groups.
        :type check_fragmented: bool
        :returns: Dictionary mapping group ID strings to lists of history file paths.
        :rtype: dict[str, list[pathlib.Path]]
        """
        if self.__hf_groups is None:
            self.__hf_groups = sort_hf_groups(list(self.__hf_to_meta_map.keys()))
        
            if check_fragmented:
                self.check_pulled()
                self.__hf_groups = merge_fragmented_groups(self.__hf_groups, self.__hf_to_meta_map)

        return self.__hf_groups

    def slice_groups(self, slice_size_years=10, start_year=0, pattern="*"):
        """
        Returns a new collection with history file groups partitioned into time slices.

        For each group (optionally filtered by ``pattern``), determines the year
        range via :func:`get_year_bounds`, computes slice boundaries via
        :func:`calculate_year_slices`, and assigns each file to the appropriate
        sub-group based on its midpoint year.  Sub-group keys are suffixed with
        ``[sorting_pivot]<start>-<end>`` to carry the year range through to
        :class:`~gents.timeseries.TSCollection`.

        :param slice_size_years: Maximum width of each time slice in years.
            Defaults to ``10``.
        :type slice_size_years: int
        :param start_year: Override for the starting year; set to ``None`` to begin
            at the dataset's own minimum year. Defaults to ``0``.
        :type start_year: int or None
        :param pattern: ``fnmatch`` glob to restrict slicing to matching group IDs.
            Defaults to ``*`` (all groups).
        :type pattern: str or None
        :returns: New ``HFCollection`` with sliced groups embedded.
        :rtype: HFCollection
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
            else:
                group_meta_map = {path: self.__hf_to_meta_map[path] for path in hf_paths}
                
                min_year, max_year = get_year_bounds(group_meta_map)
                if start_year is not None:
                    min_year = start_year
                
                time_slices = calculate_year_slices(slice_size_years, min_year, max_year)

                hf_slices = {}
                variable_set = None
                for hf_path in hf_paths:
                    meta_ds = self.__hf_to_meta_map[hf_path]
        
                    if meta_ds.get_cftime_bounds() is not None:
                        time_bnds = meta_ds.get_cftime_bounds()[0]
                        time = time_bnds[0] + ((time_bnds[1] - time_bnds[0]) / 2)
                    else:
                        time = meta_ds.get_cftimes()[0]
                    
                    for time_slice in time_slices:
                        if time_slice[0] <= time.year <= time_slice[1]:
                            if time_slice in hf_slices:
                                hf_slices[time_slice].append(hf_path)
                            else:
                                hf_slices[time_slice] = [hf_path]
                            break

                for time_slice in hf_slices:
                    sliced_groups[f"{group}[sorting_pivot]{time_slice[0]}-{time_slice[1]}"] = hf_slices[time_slice]
        logger.debug(f"Slicing groups into {slice_size_years} year long slices for '{pattern}'.")
        return self.copy(hf_groups=sliced_groups)
