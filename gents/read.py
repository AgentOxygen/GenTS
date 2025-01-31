#!/usr/bin/env python
"""
read.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 01/28/25
"""
import numpy as np
import os
import fnmatch
from cftime import num2date
import cftime
from pathlib import Path
import netCDF4


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


def read_time_variables(file_path):
    """
    Reads specified netCDF file and returns the values of the time and time bounds variable if they exist.

    :param file_path: Path to the netCDF file.
    :return: List of values for time and time bound variables or None if either doesn't exist.
    """
    times = None
    bounds = None
    
    try:
        with netCDF4.Dataset(file_path, 'r') as ds:
            if 'time' in ds.variables:
                times = num2date(ds['time'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            if 'time_bnds' in ds.variables:
                bounds = num2date(ds['time_bnds'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bnd' in ds.variables:
                bounds = num2date(ds['time_bnd'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bounds' in ds.variables:
                bounds = num2date(ds['time_bounds'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bound' in ds.variables:
                bounds = num2date(ds['time_bound'][:], units=ds["time"].units, calendar=ds["time"].calendar)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    return [times, bounds]


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


def get_year_bounds(hf_paths, hf_to_time_map):
    """
    Determines the minimum and maximum year for a series of mapped history files.

    :param hf_paths: List of paths to history files 
    :param hf_to_time_map: Dictionary that maps history file paths (key) to their time and time bound data (value)
    :return: The minimum and maximum years from the specified history files as a tuple
    """
    min_year = np.inf
    max_year = -np.inf
    
    for path in hf_paths:
        time_bounds = hf_to_time_map[path][1]
        if time_bounds is None:
            time_bounds = []
            for ts in hf_to_time_map[path][0]:
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


def slice_hf_groups(hf_groups, hf_to_time_map, slice_size_years):
    """
    Seperates history files path groups into sliced time chunks.

    :param hf_groups: A dictionary that maps group substrings (key) to history file paths (value), generated by sort_hf_groups
    :param hf_to_time_map: Dictionary that maps history file paths (key) to their time and time bound data (value)
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
            min_year, max_year = get_year_bounds(hf_paths, hf_to_time_map)
            time_slices = calculate_year_slices(slice_size_years, min_year, max_year+1)
            time_slices[0] = (min_year, time_slices[0][1])
            time_slices[-1] = (time_slices[-1][0], max_year)
            
            hf_slices = {}
            for hf_path in hf_paths:
                time, time_bnds = hf_to_time_map[hf_path]
                if time is not None:
                    time = time[0]
                
                if time_bnds is not None:
                    time_bnds = time_bnds[0]
                    time = time_bnds[0] + (time_bnds[1] - time_bnds[0]) / 2
                
                for time_slice in time_slices:
                    if time_slice[0] <= time.year <= time_slice[1]:
                        if time_slice in hf_slices:
                            hf_slices[time_slice].append(hf_path)
                        else:
                            hf_slices[time_slice] = [hf_path]
                        break
    
            for time_slice in hf_slices:
                hf_slices[time_slice].sort()
                sliced_groups[f"{group}.{time_slice[0]}-{time_slice[1]}"] = hf_slices[time_slice]
    return sliced_groups


def generate_output_template(hf_head_dir, group_path_id, output_head_dir=None, directory_swaps={"hist": "tseries"}):
    """
    Creates timeseries dataset from specified history file paths.

    :param hf_head_dir: Head directory used to read in the history files.
    :param group_path_id: One group path ID produced by slice_hf_groups.
    :param output_head_dir: Head directory to output to, if not the same as the input head directory.
    :param directory_swaps: Dictionary with directory names (key) to rename (value)
    :return: Output template path for generating time series files.
    """
    group_path_id = Path(group_path_id)
                         
    raw_filename_prefix = group_path_id.name
    excess_date_tag_index = find_all_indices(raw_filename_template, ".")[-1]
    filename_prefix = raw_filename_prefix[:excess_date_tag_index]
    
    sub_dir_structure = (str(group_path_id.parent).split(hf_head_dir)[-1]).split("/")

    for key in directory_swaps:
        for index in range(len(sub_dir_structure)):
            if sub_dir_structure[index] == key:
                sub_dir_structure[index] = directory_swaps[key]

    sub_dir_path = "/"
    for directory in sub_dir_structure:
        sub_dir_path += f"{directory}/"

    if output_head_dir is None:
        output_template = Path(f"{hf_head_dir}/{sub_dir_path}")
    else:
        output_template = Path(f"{output_head_dir}/{sub_dir_path}")
    return output_template