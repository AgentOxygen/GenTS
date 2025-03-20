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


def is_var_secondary(variable: netCDF4._netCDF4._Variable,
                     secondary_vars: list = ["time_bnds", "time_bnd", "time_bounds", "time_bound"],
                     secondary_dims: list = ["nbnd", "chars", "string_length", "hist_interval"],
                     max_num_dims: int = 1,
                     primary_dims: list = ["time"]) -> bool:
    """
    Determines if a variable is secondary or not (and then should be included in all time series files).
    Criteria are applied in order of the parameters described.

    :param dimensions: Dimensions for the variable.
    :param secondary_dims: List of secondary variable names
    :param secondary_dims: List of dimensions that make a variable secondary
    :param max_num_dims: Maximum number of dimensions a secondary variable should have.
    :param primary_dims: List of dimensions that make a variable primary (thus not secondary).
    :return: True if the variable is secondary, false if not.
    """
    if variable.name in secondary_vars:
        return True
        
    dims = np.unique(variable.dimensions)

    for tag in secondary_dims:
        if tag in dims:
            return True

    if len(dims) > max_num_dims:
        for tag in primary_dims:
            if tag in dims:
                return False

    return True


def get_timestamp_str(times):
    """
    Creates timestamp string to describe time range for netCDF dataset

    :param times: Time values for netCDF dataset in integer form with units and calendar attributes.
    :return: String containing appropriate timestamp.
    """
    start_t = num2date(times[0], units=times.units, calendar=times.calendar)
    if times.shape[0] == 1:
        return start_t.strftime("%Y-%m-%d-%H:%M:%S")
    else:
        dt = num2date(times[1], units=times.units, calendar=times.calendar) - start_t
        minutes = dt.total_seconds() / 60
        hours = minutes / 60
        days = hours / 24
        months = days / 30
    
        if minutes < 1:
            time_format = "%Y-%m-%d-%H:%M:%S"
        elif hours < 1:
            time_format = "%Y-%m-%d-%H"
        elif days < 1:
            time_format = "%Y-%m-%d"
        elif months < 1:
            time_format = "%Y-%m"
        else:
            time_format = "%Y"
        
        end_t = num2date(times[-1], units=times.units, calendar=times.calendar)
        return f"{start_t.strftime(time_format)}.{end_t.strftime(time_format)}"


def generate_time_series(hf_paths, ts_out_dir, prefix=None, complevel=0, compression=None, overwrite=False):
    """
    Creates timeseries dataset from specified history file paths.

    :param hf_paths: List of paths to history files to generate time series from
    :param ts_out_dir: Directory to output time series files to.
    :param prefix: Prefix to add to beginning of the names of the generated time series files.
    :param complevel: Compression level to apply through netCDF4 API.
    :param compression: Compression algorithm to use through netCDF4 API.
    :param overwrite: Whether or not to delete existing time series files with the same names as those being generated.
    :return: List of paths to time series generated.
    """
    agg_hf_ds = netCDF4.MFDataset(hf_paths, aggdim="time")
    
    global_attrs = get_attributes(agg_hf_ds)
    primary_vars = []
    secondary_vars_data = {}
    output_paths = []
    
    for variable in agg_hf_ds.variables:
        if is_var_secondary(agg_hf_ds[variable]):
            secondary_vars_data[variable] = agg_hf_ds[variable][:]
        else:
            primary_vars.append(variable)
    
    for variable in primary_vars:
        ts_string = get_timestamp_str(agg_hf_ds["time"])
        if prefix is not None:
            ts_out_path = f"{ts_out_dir}/{prefix}.{variable}.{ts_string}.nc"
        else:
            ts_out_path = f"{ts_out_dir}/{variable}.{ts_string}.nc"

        if not overwrite and isfile(ts_out_path):
            # Can add a check here to see if the file is readable and the version attribute is added
            continue
        elif overwrite and isfile(ts_out_path):
            remove(ts_out_path)

        if not ts_out_dir.exists():
            makedirs(ts_out_dir)
        
        var_ds = agg_hf_ds[variable]
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
        
        # Put version here later
        ts_ds.setncatts(global_attrs | {"gents_version": "put version here later"})
        ts_ds.close()
        output_paths.append(ts_out_path)
    return output_paths