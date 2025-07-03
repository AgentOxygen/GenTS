#!/usr/bin/env python
"""
hfcollection.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 04/30/25
"""
from gents.utils import log
from gents.meta import get_meta_from_path
from dask.distributed import client
from cftime import num2date
from pathlib import Path
import numpy as np
import os
import fnmatch
import cftime
import netCDF4
import dask
import warnings


def check_config(config):
    """
    Used to ensure configuration dictionary has necessary parameters.

    :param config: The config directory to check.
    :return: True if config dictionary is correctly set, false if not.
    """
    assert "name" in config
    assert "include" in config
    assert "exclude" in config
    assert type(config["include"]) is dict or config["include"] is None
    assert type(config["exclude"]) is list or config["exclude"] is None


def get_default_config():
    """
    Generates dictionary containing default configurations for GenTS.

    :return: Dictionary with default parameters.
    """
    return {
        "name": "default",
        "include": None,
        "exclude": None
    }


def find_files(head_path, pattern):
    """
    Search for files in the specified head directory and all subdirectories that match the given wildcard pattern.

    :param head_path: The head directory to start searching from.
    :param pattern: The wildcard pattern to match files against (e.g., '*.nc').
    :return: A list of file paths that match the pattern.
    """
    matched_files = []

    for root, dirs, files in os.walk(head_path):
        for file in files:
            if fnmatch.fnmatch(file, pattern):
                matched_files.append(Path(os.path.join(root, file)))

    return matched_files


def calculate_year_slices(slice_size_years, min_year, max_year):
    """
    Calculates the ranges for each slice in years within a given range.

    :param slice_size_years: Length of each slice in years.
    :param min_year: Minimum or starting year for the full range.
    :param max_year: Maximum or ending year for the full range.
    :return: List of tuples where each tuple defines the year range for each slice
    """
    start_year = int(np.floor(min_year / slice_size_years)*slice_size_years)
    end_year = int(np.ceil(max_year / slice_size_years)*slice_size_years)

    ranges = []
    for year in np.arange(start_year, end_year, slice_size_years, dtype=int):
        ranges.append((int(year), int(year+slice_size_years-1)))
    
    return ranges


def find_all_indices(string, substring):
    """
    Finds all indices where the substring occurs in a string.

    :param string: The string to search in.
    :param substring: The substring to search for.
    :return: A list of indices where the substring is found.
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
    Groups history file paths by directory and substrings.

    :param hf_paths: List of paths to history files.
    :param delimiter: The delimiter to find the substrings with.
    :param substring_index: Substrings to skip after splitting with the delimiter (from right to left).
    :return: A dictionary that maps group substrings (key) to history file paths (value)
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
    Determines the minimum and maximum year for a series of mapped history files.

    :param hf_to_time_map: Dictionary that maps history file paths (key) to their time and time bound data (value)
    :return: The minimum and maximum years from the specified history files as a tuple
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
            
            mid_ordinal = np.median([lower_bound.toordinal(), upper_bound.toordinal()])
            mid_time = cftime.datetime.fromordinal(mid_ordinal, calendar=lower_bound.calendar)
                    
            if mid_time.year > max_year:
                max_year = mid_time.year
            if mid_time.year < min_year:
                min_year = mid_time.year
    return min_year, max_year


def generate_output_template(hf_head_dir, group_path_id, output_head_dir=None, directory_swaps={"hist": "tseries"}, filename_delimiter=".", cutoff_index=None):
    """
    Creates file path template (missing file name suffix) for outputing timeseries files to.

    :param hf_head_dir: Head directory used to read in the history files.
    :param group_path_id: One group path ID produced by slice_hf_groups.
    :param output_head_dir: Head directory to output to, if not the same as the input head directory.
    :param directory_swaps: Dictionary with directory names (key) to rename (value)
    :param filename_delimiter: Delimiter used to separate file name tags
    :param cutoff_index: Index to cutoff tags that are generated using the delimiter (typically used to remove the date)
    :return: Output template path for generating time series files.
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
    Checks if time bounds from dataset metadata are between the year range.
    
    :param ds_meta: Metadata associated with the dataset.
    :param min_year: Minimum year in range.
    :param max_year: Maximum year in range.
    :return: True if time bounds are within the year range, false if not.
    """
    time_bounds = ds_meta.get_cftime_bounds()[0]
    year = (time_bounds[0].year + time_bounds[1].year) / 2

    if min_year <= year <= max_year:
        return True
    else:
        return False


def filter_by_variables(meta_datasets):
    """
    Checks for consistency within a list of datasets.
    
    :param ds_meta: List of metadatas associated with the datasets.
    :return: Tuple containing majority variables
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
    Re-arranges the metadata objects in a list so that they are ordered by time.
    
    :param metas: List of unsorted metadata.
    :return: List of metadata sorted by time.
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
    Checks if the history files for each group have the same sets of variables.
    If an inconsistent set is discovered, it is thrown out and a warning is logged.
    
    :param sliced_groups: Sliced groups of history files to check.
    :return: Sliced groups with inconsistent sets filtered out.
    """
    filtered_sliced_groups = {}
    for group in sliced_groups:
        meta_datasets = sliced_groups[group]
        majority, others = filter_by_variables(meta_datasets)
        if majority is not None:
            filtered_sliced_groups[group] = sort_metas_by_time(majority)
            if others is not None:
                for meta_ds in others:
                    log(f"Dataset has inconsistent variable list with directory group: {meta_ds.get_path()}")
        else:
            log(f"Unable to determine majority dataset, check variable configurations between directory groups, group ID: {group}")
    return filtered_sliced_groups


class HFCollection:
    """History File Collection, holds paths to all history files and serves as an interface for interpreting the metadata."""
    def __init__(self, hf_dir, dask_client=None, meta_map=None):
        """
        :param hf_dir: Head directory to history files
        :param dask_client: Dask client object. If not given, the global client is used instead.
        :param meta_map: History file to metadata map to use (overrides recursive search with hf_dir).
        """
        self.__raw_paths = find_files(hf_dir, "*.nc")

        if dask_client is None:
            self.__client = dask.distributed.client._get_global_client()
        else:
            self.__client = dask_client

        self.__hf_to_meta_map = {}
        if meta_map is None:
            for path in self.__raw_paths:
                self.__hf_to_meta_map[path] = None
        else:
            self.__hf_to_meta_map = meta_map

        self.__meta_pulled = True
        for path in self.__hf_to_meta_map:
            if self.__hf_to_meta_map[path] is None:
                self.__meta_pulled = False
                break
        
        self.__hf_groups = None
        self.__hf_dir = hf_dir

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

    def get_input_dir(self):
        """Return the input directory"""
        return self.__hf_dir
    
    def check_pulled(self):
        """Checks if metadata has been pulled. If not, then pull."""
        if not self.__meta_pulled:
            self.pull_metadata()

    def copy(self, dask_client=None, meta_map=None):
        """
        Copies data of this HFCollection into a new one.
    
        :param dask_client: Dask client to assign to copy.
        :param meta_map: history file to metadata map to use when copying (defaults to existing).
        :return: HFCollection that is a copy.
        """
        if dask_client is None:
            dask_client = self.__client
        if meta_map is None:
            meta_map = self.__hf_to_meta_map
        return HFCollection(self.__hf_dir, dask_client=dask_client, meta_map=meta_map)
    
    def pull_metadata(self, check_valid=True):
        """Pulls metadata associated with each history file in the collection."""
        ds_metas_futures = []
        ds_metas = []
        paths = list(self.__hf_to_meta_map.keys())

        if self.__client is None:
            for path in paths:
                ds_metas.append(get_meta_from_path(path))
        else:
            for index in range(0, len(paths), 10000):
                ds_metas_subset = self.__client.map(get_meta_from_path, paths[index:index + 10000])
                ds_metas_futures += ds_metas_subset
            
            ds_metas = self.__client.gather(ds_metas_futures, direct=True)
            del ds_metas_futures
        
        for index, path in enumerate(paths):
            if ds_metas[index] is not None:
                self.__hf_to_meta_map[path] = ds_metas[index]
        self.__meta_pulled = True
        if check_valid:
            self.check_validity()

    def check_validity(self):
        """Checks validity of metadata for each history file. Removes missing or incomplete metadata."""
        new_map = {}
        removed = {}
        for path in self.__hf_to_meta_map:
            if self.__hf_to_meta_map[path] is not None and self.__hf_to_meta_map[path].is_valid():
                new_map[path] = self.__hf_to_meta_map[path]
            else:
                removed[path] = self.__hf_to_meta_map[path]
        self.__hf_to_meta_map = new_map
        return removed
    
    def include_patterns(self, glob_patterns):
        """
        Filters out history files in the collection with paths that do not match the glob patterns.

        :param glob_patterns: List of patterns to compare paths against.
        """
        filtered_path_map = {}
        for path in self.__hf_to_meta_map:
            for pattern in glob_patterns:
                if fnmatch.fnmatch(str(path), pattern):
                    filtered_path_map[path] = self.__hf_to_meta_map[path]
        return self.copy(meta_map=filtered_path_map)

    def exclude_patterns(self, glob_patterns):
        """
        Filters out history files in the collection with paths that do match the glob patterns.

        :param glob_patterns: List of patterns to compare paths against.
        """
        filtered_path_map = {}
        for path in self.__hf_to_meta_map:
            for pattern in glob_patterns:
                if not fnmatch.fnmatch(str(path), pattern):
                    filtered_path_map[path] = self.__hf_to_meta_map[path]
        return self.copy(meta_map=filtered_path_map)

    def include_years(self, start_year, end_year, glob_patterns=["*"]):
        """
        Filters out history files in the collection that fall outside the specified range of years.
        Glob patterns can be used to limit this filter to specific history files.
        

        :param start_year: First year in range.
        :param end_year: Last year in range.
        :param glob_patterns: Glob patterns to match history files that recieve this filter.
        """
        self.check_pulled()
        filtered_path_map = self.__hf_to_meta_map
        remove_paths = []
        for pattern in glob_patterns:
            for path in filtered_path_map:
                if fnmatch.fnmatch(path, pattern):
                    meta_ds = filtered_path_map[path]
                    avg_year = np.mean([ts[0].year for ts in meta_ds.get_cftime_bounds()])
                    
                    if not start_year <= avg_year <= end_year:
                        remove_paths.append(path)

        for path in remove_paths:
            del filtered_path_map[path]

        return self.copy(meta_map=filtered_path_map)

    def get_groups(self):
        """
        Returns history file groupings.

        :return: Dictionary containing group ID (key) and history file metadatas (value).
        """
        if self.__hf_groups is None:
            self.check_pulled()
            self.__hf_groups = sort_hf_groups(list(self.__hf_to_meta_map.keys()))
        return self.__hf_groups

    def slice_groups(self, slice_size_years=10, pattern=None):
        """
        Slices history file groupings that match the glob pattern (if specified) into subsets by time.

        :param slice_size_years: Size of slices to make, in years.
        :param pattern: Glob pattern to match history file grouping IDs to.
        :return: New groupings that are subset into time periods specified by 'slice_size_years'
        """
        hf_groups = self.get_groups()
        sliced_groups = {}
        
        for group in hf_groups:
            hf_paths = hf_groups[group]
            if pattern is not None and not fnmatch.fnmatch(group, pattern):
                sliced_groups[group] = hf_paths
                continue
            
            if len(hf_paths) == 1:
                warnings.warn("Cannot slice history file group of size 1.", RuntimeWarning)
                continue
            else:
                group_meta_map = {path: self.__hf_to_meta_map[path] for path in hf_paths}
                
                min_year, max_year = get_year_bounds(group_meta_map)
                time_slices = calculate_year_slices(slice_size_years, min_year, max_year+1)
                time_slices[0] = (min_year, time_slices[0][1])
                time_slices[-1] = (time_slices[-1][0], max_year)
                
                hf_slices = {}
                variable_set = None
                for hf_path in hf_paths:
                    meta_ds = self.__hf_to_meta_map[hf_path]
                    time_bnds = meta_ds.get_cftime_bounds()[0]
        
                    if time_bnds is not None:
                        time = time_bnds[0] + (time_bnds[1] - time_bnds[0]) / 2
                    else:
                        time = meta_ds.get_cftimes()[0]
                    
                    for time_slice in time_slices:
                        if time_slice[0] <= time.year <= time_slice[1]:
                            if time_slice in hf_slices:
                                hf_slices[time_slice].append(meta_ds)
                            else:
                                hf_slices[time_slice] = [meta_ds]
                            break
        
                for time_slice in hf_slices:
                    sliced_groups[f"{group}{time_slice[0]}-{time_slice[1]}"] = hf_slices[time_slice]
        return self.__hf_groups