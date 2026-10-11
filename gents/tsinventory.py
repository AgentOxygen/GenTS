#!/usr/bin/env python
"""
Discovery, filtering, and indexing of existing GenTS time series files.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
from gents.datastore import GenTSDataStore
from gents.hfcollection import find_files, get_group_timestep_delta
from gents.meta import netCDFMeta, read_meta_map, split_valid
from gents.timeseries import get_timestep_label
from gents.utils import filter_by_globs
import numpy as np
import logging

logger = logging.getLogger(__name__)


def get_ts_meta_from_path(path):
    """
    Reads a time series file's header into a :class:`~gents.meta.netCDFMeta`.

    Unlike :func:`gents.meta.get_meta_from_path`, keeps per-variable attributes
    and skips dimension bounds, which only history-file tiling needs. Picklable,
    for process pools.

    :param path: Path to the time series file.
    :type path: str
    :rtype: gents.meta.netCDFMeta
    """
    try:
        with GenTSDataStore(path, "r") as ds:
            return netCDFMeta(ds, path, decode_dates=False, compute_dim_bounds=False)
    except Exception as e:
        raise type(e)(f"{e} Path: {path}") from e


def is_gents_output(ts_meta):
    """
    Returns whether a file was produced, and completely written, by GenTS.

    The opposite of :meth:`gents.meta.netCDFMeta.is_valid`, which treats the
    ``gents_version`` attribute as disqualifying. GenTS stamps it last, so it
    also marks a finished write.

    :param ts_meta: Metadata for the time series file.
    :type ts_meta: gents.meta.netCDFMeta
    :rtype: bool
    """
    return "gents_version" in ts_meta.get_attributes()


def get_meta_timestep_label(ts_meta):
    """
    Returns a file's frequency label (``'month_1'``, ``'day_1'``, ...).

    Derived from the file's own time coordinate. Fewer than two time steps, or
    no usable time coordinate, gives ``'unsorted'``.

    :param ts_meta: Metadata for the time series file.
    :type ts_meta: gents.meta.netCDFMeta
    :rtype: str
    """
    try:
        delta = get_group_timestep_delta([ts_meta])
    except (ValueError, TypeError):
        return get_timestep_label(None)
    return get_timestep_label(delta)


def get_time_sort_key(ts_meta):
    """
    Returns a sort key ordering time series files by earliest time value.

    Compares raw float times, which are monotonic with their decoded dates.
    Files with no usable time coordinate sort last rather than raising.

    :param ts_meta: Metadata for the time series file.
    :type ts_meta: gents.meta.netCDFMeta
    :rtype: tuple[int, float]
    """
    float_times = ts_meta.get_float_times()
    if float_times is None:
        return (1, 0.0)

    values = np.ma.getdata(np.atleast_1d(float_times))
    if values.shape[0] == 0:
        return (1, 0.0)
    return (0, float(np.min(values)))


class TSInventory:
    """
    An index of the GenTS time series files that already exist on disk.

    Mirrors :class:`~gents.hfcollection.HFCollection` for output instead of
    input: discover files, filter them by path, pull their metadata lazily, then
    group them by variable. Filters return new inventories; only
    :meth:`pull_metadata` and :meth:`check_validity` mutate in place.
    """

    def __init__(self, ts_head_dir, num_processes=1, ts_glob_pattern="*.nc*", meta_map=None, ts_var_map=None):
        """
        A missing or empty directory is not an error, since a first run has no
        output yet; it gives an empty inventory.

        :param ts_head_dir: Root directory to search for time series files.
        :type ts_head_dir: str
        :param num_processes: Worker processes used for parallel metadata reads.
        :type num_processes: int
        :param ts_glob_pattern: ``fnmatch`` pattern used to discover files.
        :type ts_glob_pattern: str
        :param meta_map: Pre-populated ``{path: netCDFMeta}`` mapping, from :meth:`copy`.
            Skips discovery.
        :type meta_map: dict or None
        :param ts_var_map: Pre-built variable grouping, from :meth:`copy`. Stale
            entries are pruned by :meth:`get_variables`.
        :type ts_var_map: dict or None
        """
        self.__num_processes = num_processes
        self.__ts_dir = ts_head_dir
        self.__ts_glob_pattern = ts_glob_pattern
        self.__ts_var_map = ts_var_map

        if meta_map is None:
            self.__ts_to_meta_map = {path: None for path in find_files(ts_head_dir, ts_glob_pattern)}
            logger.info(f"Initialized TSInventory at '{ts_head_dir}'")
            logger.info(f"{len(self.__ts_to_meta_map)} netCDF files found.")
        else:
            self.__ts_to_meta_map = meta_map

    def __getitem__(self, key):
        return self.__ts_to_meta_map[key]

    def __contains__(self, key):
        return key in self.__ts_to_meta_map

    def __iter__(self):
        return iter(self.__ts_to_meta_map)

    def __len__(self):
        return len(self.__ts_to_meta_map)

    def items(self):
        return self.__ts_to_meta_map.items()

    def values(self):
        return self.__ts_to_meta_map.values()

    def keys(self):
        return self.__ts_to_meta_map.keys()

    def include(self, glob_patterns):
        """
        Returns a new inventory holding only files matching at least one pattern.

        Patterns are ``fnmatch`` globs tested against path strings; an empty list
        matches nothing and so empties the inventory.

        :param glob_patterns: One or more glob patterns; a single string is also
            accepted.
        :type glob_patterns: list[str] or str
        :rtype: TSInventory
        """
        filtered_path_map = filter_by_globs(self.__ts_to_meta_map, glob_patterns, keep_matches=True)
        logger.debug(f"Inclusive filter(s) applied: '{glob_patterns}'")
        return self.copy(meta_map=filtered_path_map)

    def exclude(self, glob_patterns):
        """
        Returns a new inventory with files matching any pattern removed.

        Patterns are ``fnmatch`` globs tested against path strings.

        :param glob_patterns: One or more glob patterns; a single string is also
            accepted.
        :type glob_patterns: list[str] or str
        :rtype: TSInventory
        """
        filtered_path_map = filter_by_globs(self.__ts_to_meta_map, glob_patterns, keep_matches=False)
        logger.debug(f"Exclusive filter(s) applied: '{glob_patterns}'")
        return self.copy(meta_map=filtered_path_map)

    def is_pulled(self):
        """
        Returns whether metadata has been loaded for every file.

        :rtype: bool
        """
        return all(meta is not None for meta in self.__ts_to_meta_map.values())

    def get_input_dir(self):
        """
        Returns the head directory this inventory was initialised from.

        :rtype: str
        """
        return self.__ts_dir

    def check_pulled(self):
        """
        Pulls metadata if it has not been pulled already.
        """
        if not self.is_pulled():
            self.pull_metadata()

    def copy(self, num_processes=None, meta_map=None, ts_var_map=None):
        """
        Returns a new inventory derived from this one, with optional overrides.

        Shares the head directory and glob pattern; arguments left ``None`` are
        inherited. Every filter returns through here, keeping the API immutable.

        :param num_processes: Worker process count for the copy.
        :type num_processes: int or None
        :param meta_map: Metadata map to assign to the copy.
        :type meta_map: dict or None
        :param ts_var_map: Variable grouping to assign to the copy. Inheriting is
            safe when ``meta_map`` narrows, since :meth:`get_variables` prunes
            stale entries; a caller widening it must pass a grouping instead.
        :type ts_var_map: dict or None
        :rtype: TSInventory
        """
        if num_processes is None:
            num_processes = self.__num_processes
        if meta_map is None:
            meta_map = self.__ts_to_meta_map
        if ts_var_map is None:
            ts_var_map = self.__ts_var_map
        return TSInventory(
            self.__ts_dir,
            num_processes=num_processes,
            ts_glob_pattern=self.__ts_glob_pattern,
            meta_map=meta_map,
            ts_var_map=ts_var_map
        )

    def check_validity(self):
        """
        Drops files whose metadata is missing or not :func:`is_gents_output`,
        warning about each.

        :returns: The removed ``{path: metadata}`` entries.
        :rtype: dict
        """
        logger.debug(f"Validating metadata...")
        new_map, removed = split_valid(self.__ts_to_meta_map, is_gents_output)
        self.__ts_to_meta_map = new_map
        self.__ts_var_map = None
        logger.debug(f"{len(new_map)} files validated ({len(removed)} removed).")
        return removed

    def pull_metadata(self, check_valid=True, raise_errors=False, show_progress=True):
        """
        Reads the header of every time series file in the inventory.

        Runs :func:`get_ts_meta_from_path` over a process pool when
        ``num_processes > 1``, serially otherwise.

        :param check_valid: Run :meth:`check_validity` afterwards, dropping files
            GenTS did not produce or did not finish writing.
        :type check_valid: bool
        :param raise_errors: Raise per-file failures instead of logging them.
        :type raise_errors: bool
        :param show_progress: If ``False``, suppress the stdout progress bar.
        :type show_progress: bool
        :returns: This inventory, mutated in place, so the call can be chained.
        :rtype: TSInventory
        """
        logger.info(f"Pulling metadata...")
        self.__ts_var_map = None
        paths = list(self.__ts_to_meta_map.keys())

        self.__ts_to_meta_map.update(read_meta_map(
            paths, get_ts_meta_from_path, self.__num_processes, raise_errors, show_progress
        ))

        if check_valid:
            self.check_validity()
        else:
            logger.warning(f"Skipping metadata validation may result in errors due to missing attributes or coordinate data.")
        logger.info(f"Metadata pulled.")
        return self

    def get_variables(self):
        """
        Groups the inventory's files by variable and frequency.

        Keys are ``'<variable>.<timestep_label>'``, e.g. ``'TS.month_1'``. A file
        holding several primary variables is listed under each; one holding none
        is skipped with a warning.

        Paths are ordered by earliest time value. A grouping inherited from a
        filtered copy is pruned rather than rebuilt, which preserves that order.

        :returns: ``{'<variable>.<timestep_label>': [pathlib.Path]}``.
        :rtype: dict[str, list[pathlib.Path]]
        """
        if self.__ts_var_map is None:
            self.check_pulled()

            grouped = {}
            for path, meta in self.__ts_to_meta_map.items():
                primary_vars = meta.get_primary_variables()

                if len(primary_vars) == 0:
                    logger.warning(f"No primary variable found for '{path}', skipping.")
                    continue
                if len(primary_vars) > 1:
                    logger.warning(
                        f"Expected one primary variable per time series file, "
                        f"found {len(primary_vars)} in '{path}': {primary_vars}."
                    )

                timestep_label = get_meta_timestep_label(meta)
                for var_name in primary_vars:
                    grouped.setdefault(f"{var_name}.{timestep_label}", []).append((path, meta))

            self.__ts_var_map = {
                key: [path for path, _ in sorted(entries, key=lambda entry: get_time_sort_key(entry[1]))]
                for key, entries in grouped.items()
            }
            logger.debug(f"{len(self.__ts_var_map)} variable groups built.")
        else:
            pruned = {}
            reassign = False
            for key, paths in self.__ts_var_map.items():
                kept = [path for path in paths if path in self.__ts_to_meta_map]
                if len(paths) != len(kept):
                    reassign = True
                if len(kept) > 0:
                    pruned[key] = kept

            if reassign:
                self.__ts_var_map = pruned

        return self.__ts_var_map
