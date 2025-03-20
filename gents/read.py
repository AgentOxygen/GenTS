#!/usr/bin/env python
"""
read.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 01/28/25
"""
from gents.utils import log
from gents.meta import get_meta_from_path
from cftime import num2date
from pathlib import Path
import numpy as np
import os
import fnmatch
import cftime
import netCDF4
import dask


def is_ds_within_years(ds_meta, min_year, max_year):
    time_bounds = ds_meta.get_cftime_bounds()[0]
    year = (time_bounds[0].year + time_bounds[1].year) / 2

    if min_year <= year <= max_year:
        return True
    else:
        return False


def apply_filter(meta_ds, filter_dict):
    if "year_bounds" in filter_dict:
        min_year, max_year = filter_dict["year_bounds"]
        if is_ds_within_years(meta_ds, min_year, max_year):
            return True
    return False


def apply_inclusive_filters(path_meta_map, filters):
    filtered_mapping = {}

    for path in path_meta_map:
        meta_ds = path_meta_map[path]
    
        for tag in filters:
            if tag in str(path) and apply_filter(meta_ds, filters[tag]):
                filtered_mapping[path] = meta_ds
                break
    return filtered_mapping


def apply_exclusive_filters(path_meta_map, filters):
    filtered_mapping = {}
    for path in path_meta_map:
        tag_found = False
        for tag in filters:
            if tag in str(path):
                tag_found = True
                break

        if not tag_found:
            filtered_mapping[path] = path_meta_map[path]
        
    return filtered_mapping


def check_config(config):
    assert "name" in config
    assert "include" in config
    assert "exclude" in config
    assert type(config["include"]) is dict or config["include"] is None
    assert type(config["exclude"]) is list or config["exclude"] is None


def get_default_config():
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
            hf_groups[f"{parent_path}/{substring}"] = []
            for path in group_paths:
                parsed = path.name[:find_all_indices(path.name, delimiter)[-1 * substring_index]]
                if substring == parsed:
                    hf_groups[f"{parent_path}/{substring}"].append(path)
        
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


def slice_hf_groups(hf_groups, hf_to_meta_map, slice_size_years):
    """
    Seperates history files path groups into sliced time chunks.

    :param hf_groups: A dictionary that maps group substrings (key) to history file paths (value), generated by sort_hf_groups
    :param hf_to_meta_map: Dictionary that maps history file paths (key) to respective metadata (value)
    :param slice_size_years: Number of years per slice.
    :return: 
    """
    sliced_groups = {}
    for group in hf_groups:
        hf_paths = hf_groups[group]
        if len(hf_paths) == 1:
            # Probably good to throw a warning here
            continue
        else:
            group_meta_map = {path: hf_to_meta_map[path] for path in hf_paths}
            
            min_year, max_year = get_year_bounds(group_meta_map)
            time_slices = calculate_year_slices(slice_size_years, min_year, max_year+1)
            time_slices[0] = (min_year, time_slices[0][1])
            time_slices[-1] = (time_slices[-1][0], max_year)
            
            hf_slices = {}
            variable_set = None
            for hf_path in hf_paths:
                meta_ds = hf_to_meta_map[hf_path]
                time = meta_ds.get_cftimes()
                time_bnds = meta_ds.get_cftime_bounds()
                variables = meta_ds.get_variables()

                if time is not None:
                    time = time[0]
                
                if time_bnds is not None:
                    time_bnds = time_bnds[0]
                    time = time_bnds[0] + (time_bnds[1] - time_bnds[0]) / 2
                
                for time_slice in time_slices:
                    if time_slice[0] <= time.year <= time_slice[1]:
                        if time_slice in hf_slices:
                            hf_slices[time_slice].append(meta_ds)
                        else:
                            hf_slices[time_slice] = [meta_ds]
                        break
    
            for time_slice in hf_slices:
                sliced_groups[f"{group}.{time_slice[0]}-{time_slice[1]}"] = hf_slices[time_slice]
    return sliced_groups


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


def filter_by_variables(meta_datasets):
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


def get_metas_from_paths(paths, dask_client=None):
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()
        
    ds_metas_futures = dask_client.map(get_meta_from_path, paths)
    ds_metas = dask_client.gather(ds_metas_futures)
    del ds_metas_futures
    
    hf_to_meta_map = {path: ds_metas[index] for index, path in enumerate(paths) if ds_metas[index] is not None and ds_metas[index].get_cftime_bounds() is not None}
    return hf_to_meta_map


def get_groups_from_paths(paths, slice_size_years=10, dask_client=None):
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()

    hf_to_meta_map = get_metas_from_paths(paths, dask_client=dask_client)
    
    groups = sort_hf_groups(list(hf_to_meta_map.keys()))
    sliced_groups = slice_hf_groups(groups, hf_to_meta_map, slice_size_years)
    filtered_sliced_groups = check_groups_by_variables(sliced_groups)

    return filtered_sliced_groups