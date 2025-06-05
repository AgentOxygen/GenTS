#!/usr/bin/env python
"""
timeseries.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 01/31/25
"""
import netCDF4
import numpy as np
from os.path import isfile
from os import remove, makedirs
from cftime import num2date
from gents.meta import get_attributes
from gents.utils import get_version, log


def get_timestamp_str(times):
    """
    Creates timestamp string to describe time range for netCDF dataset

    :param times: Time values for netCDF dataset in integer form with units and calendar attributes.
    :return: String containing appropriate timestamp.
    """
    calendar = times.calendar
    units = times.units
    data = np.sort(times[:])
    
    start_t = num2date(data[0], units=units, calendar=calendar)
    if times.shape[0] == 1:
        return start_t.strftime("%Y%m%d%H")
    else:
        dt = num2date(data[1], units=units, calendar=calendar) - start_t
        minutes = dt.total_seconds() / 60
        hours = minutes / 60
        days = hours / 24
        months = days / 30
    
        if minutes < 1:
            time_format = "%Y%m%d%H%M%S"
        elif hours < 1:
            time_format = "%Y%m%d%H"
        elif days < 1:
            time_format = "%Y%m%d"
        elif months < 1:
            time_format = "%Y%m"
        else:
            time_format = "%Y"
        
        end_t = num2date(data[-1], units=units, calendar=calendar)
        return f"{start_t.strftime(time_format)}-{end_t.strftime(time_format)}"


def check_timeseries_integrity(ts_path: str):
    try:
        ts_ds = netCDF4.Dataset(path, mode="r")
        if "gents_version" in get_attributes(ts_ds):
            return True
    except OSError:
        log(f"Corrupt timeseries output: '{ts_path}'")
    return False


def generate_time_series(hf_paths, ts_path_template, primary_var, secondary_vars, complevel=0, compression=None, overwrite=False):
    """
    Creates timeseries dataset from specified history file paths.

    :param hf_paths: List of paths to history files to generate time series from
    :param ts_out_dir: Directory to output time series files to.
    :param prefix: Prefix to add to beginning of the names of the generated time series files.
    :param complevel: Compression level to apply through netCDF4 API.
    :param compression: Compression algorithm to use through netCDF4 API.
    :param overwrite: Whether or not to delete existing time series files with the same names as those being generated.
    :param target_variable: Primary variable to extract from history files.
    :return: List of paths to time series generated.
    """
    agg_hf_ds = netCDF4.MFDataset(hf_paths, aggdim="time")
    
    global_attrs = get_attributes(agg_hf_ds)
    secondary_vars_data = {}
    
    for variable in secondary_vars:
        secondary_vars_data[variable] = agg_hf_ds[variable][:]
    
    ts_string = get_timestamp_str(agg_hf_ds["time"])
    ts_out_path = f"{ts_path_template}.{variable}.{ts_string}.nc"

    var_ds = agg_hf_ds[variable]
    
    if overwrite and isfile(ts_out_path):
        remove(ts_out_path)
    elif not overwrite and isfile(ts_out_path):
        if check_timeseries_integrity(ts_ds):
            return ts_out_path
        else:
            remove(ts_out_path)

    ts_ds = netCDF4.Dataset(ts_out_path, mode="w")
    
    for index, dim in enumerate(var_ds.dimensions):
        if dim == "time":
            ts_ds.createDimension(dim, None)
        else:
            ts_ds.createDimension(dim, var_ds.shape[index])

    var_data = ts_ds.createVariable(variable,
                                    var_ds.dtype,
                                    var_ds.dimensions,
                                    complevel=complevel,
                                    compression=compression)
    var_data.set_auto_mask(False)
    var_data.set_auto_scale(False)
    var_data.set_always_mask(False)
    
    ts_ds[variable].setncatts(get_attributes(var_ds))

    time_chunk_size = 1
    if len(var_ds.shape) > 0 and "time" in var_ds.dimensions:
        for i in range(0, var_ds.shape[0], time_chunk_size):
            if i + time_chunk_size > var_ds.shape[0]:
                time_chunk_size = var_ds.shape[0] - i
            var_data[i:i + time_chunk_size] = var_ds[i:i + time_chunk_size]
    else:
        var_data[:] = var_ds[:]

    for secondary_var in secondary_vars_data:
        svar_ds = agg_hf_ds[secondary_var]
        
        for index, dim in enumerate(svar_ds.dimensions):
            if dim not in ts_ds.dimensions:
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, svar_ds.shape[index])
        
        svar_data = ts_ds.createVariable(secondary_var,
                                         svar_ds.dtype,
                                         svar_ds.dimensions,
                                         complevel=complevel,
                                         compression=compression)
        
        svar_data.set_auto_mask(False)
        svar_data.set_auto_scale(False)
        svar_data.set_always_mask(False)

        ts_ds[secondary_var].setncatts(get_attributes(svar_ds))
        svar_data[:] = secondary_vars_data[secondary_var]
    
    ts_ds.setncatts(global_attrs | {"gents_version": str(get_version())})
    ts_ds.close()
    return ts_out_path