#!/usr/bin/env python
"""
timeseries.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 01/31/25
"""
import netCDF4
import numpy as np
import fnmatch
import dask
from os.path import isfile
from os import remove, makedirs
from cftime import num2date
from pathlib import Path
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
    """
    Checks integrity of time series netCDF file by confirming `gents_version` attribute.

    :param ts_path: Path to time series file.
    :return: True if `gents_version` attribute is found. False if not (suggesting possible corruption).
    """
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
    ts_out_path = f"{ts_path_template}.{primary_var}.{ts_string}.nc"

    var_ds = agg_hf_ds[primary_var]
    
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

    var_data = ts_ds.createVariable(primary_var,
                                    var_ds.dtype,
                                    var_ds.dimensions,
                                    complevel=complevel,
                                    compression=compression)
    var_data.set_auto_mask(False)
    var_data.set_auto_scale(False)
    var_data.set_always_mask(False)
    
    ts_ds[primary_var].setncatts(get_attributes(var_ds))

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


class TSCollection:
    """Time Series Collection that faciliates the creation of time series from a HFCollection."""
    def __init__(self, hf_collection, output_dir, ts_orders, dask_client=None):
        """
        :param hf_collection: History file collection to derive time series from
        :param output_dir: Directory to output time series files to
        :param ts_orders: List of Dask delayed functions of generate_time_series
        :param dask_client: Dask client to use when executing time series batches (Default: global client).
        """
        if dask_client is None:
            self.__dask_client = dask.distributed.client._get_global_client()
        
        self.__hf_collection = hf_collection
        self.__groups = self.__hf_collection.get_groups()
        self.__output_dir = output_dir
        
        if ts_orders is None:
            self.__orders = []
            for glob_template in self.__groups:
                output_template = glob_template.split(self.__hf_collection.get_input_dir())[1]
                ts_path_template = f"{self.__output_dir}{output_template}"
                hf_paths = self.__groups[glob_template]
    
                # Assuming history files are compatable, we should check that first
                primary_vars = self.__hf_collection[hf_paths[0]].get_primary_variables()
                secondary_vars = self.__hf_collection[hf_paths[0]].get_secondary_variables()
    
                for var in primary_vars:
                    self.__orders.append({
                        "hf_paths": hf_paths,
                        "ts_path_template": ts_path_template[:-1],
                        "primary_var": var,
                        "secondary_vars": secondary_vars
                    })
        else:
            self.__orders = ts_orders

    def __contains__(self, key):
        return key in self.__orders

    def __iter__(self):
        return iter(self.__orders)

    def __getitem__(self, index):
        return self.__orders[index]

    def __len__(self):
        return len(self.__orders)

    def items(self):
        return self.__orders.items()

    def values(self):
        return self.__orders.values()
    
    def get_hf_collection(self):
        return self.__hf_collection
    
    def include(self, path_glob, var_glob="*"):
        """
        Applies inclusive filter to time series orders.

        :param path_glob: Glob pattern to apply to source history files.
        :param var_glob: Glob pattern to apply to primary variable names. Defaults to "*"
        :return: A new TSCollection that only includes time series orders that match the filter.
        """
        filtered_orders = []
        for order_dict in self.__orders:
            path_matched = False
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_matched = True
                    break
            
            if path_matched and fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                filtered_orders.append(order_dict)
        return TSCollection(self.__hf_collection, self.__output_dir, ts_orders=filtered_orders)

    def exclude(self, path_glob, var_glob="*"):
        """
        Applies exclusive filter to time series orders.

        :param path_glob: Glob pattern to apply to source history files.
        :param var_glob: Glob pattern to apply to primary variable names. Defaults to "*"
        :return: A new TSCollection that excludes time series orders that match the filter.
        """
        filtered_orders = []
        for order_dict in self.__orders:
            path_unmatched = True
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_unmatched = False
                    break
            
            if path_unmatched and not fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                filtered_orders.append(order_dict)
        return TSCollection(self.__hf_collection, self.__output_dir, ts_orders=filtered_orders)

    def add_args(self, path_glob="*", var_glob="*", level=None, alg=None, overwrite=None):
        """
        Applies arguments to pass to generate_time_series when processing time series orders.
        Filters specify which time series orders should be updated. If value is None, then the
        argument is not changed.

        :param path_glob: Glob pattern to match to source history files. Defaults to "*".
        :param var_glob: Glob pattern to match to primary variable names. Defaults to "*".
        :param level: Level of compresison to pass to the netCDF4 backend. Defaults to None.
        :param alg: Compression algorithm to pass to the netCDF4 backend. Defaults to None.
        :param overwrite: Whether or not to overwrite a time series output file if it already exists. Defaults to None.
        :return: A new TSCollection that includes time series orders with arguments applied.
        """
        new_orders = []
        for order_dict in self.__orders:
            path_matched = False
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_matched = True
                    break
            
            if path_matched and fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                if level is not None:
                    order_dict["complevel"] = level
                if alg is not None:
                    order_dict["compression"] = alg
                if overwrite is not None:
                    order_dict["overwrite"] = overwrite
            new_orders.append(order_dict)

        self.__orders = new_orders
        return TSCollection(self.__hf_collection, self.__output_dir, ts_orders=new_orders)

    def apply_path_swap(self, string_match, string_swap, path_glob="*", var_glob="*"):
        """
        Iterates over time series output path templates, finds the ones that mach the filter, and
        replaces the matching string with a swap string if it exists.
    
        :param path_glob: Glob pattern to apply to source history files. Defaults to "*"
        :param var_glob: Glob pattern to apply to primary variable names. Defaults to "*".
        :param string_match: String to match to in output template path.
        :param string_swap: String to replace match with, if found.
        :return: A new TSCollection with updated output path templates
        """
        filtered_orders = []
        for order_dict in self.__orders:
            path_matched = False
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_matched = True
                    order_dict["ts_path_template"].replace(string_match, string_swap)
            filtered_orders.append(order_dict)
    
        return TSCollection(self.__hf_collection, self.__output_dir, ts_orders=filtered_orders)
        
    def apply_compression(self, level, alg, path_glob, var_glob="*"):
        """
        Applies compression arguments to time series orders.

        :param level: Level of compresison to pass to the netCDF4 backend.
        :param alg: Compression algorithm to pass to the netCDF4 backend.
        :param path_glob: Glob pattern to match to source history files.
        :param var_glob: Glob pattern to match to primary variable names. Defaults to "*".
        :return: A new TSCollection that includes time series orders with arguments applied.
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, level=level, alg=alg)

    def apply_overwrite(self, path_glob, var_glob="*"):
        """
        Applies overwrite argument to time series orders.

        :param path_glob: Glob pattern to match to source history files.
        :param var_glob: Glob pattern to match to primary variable names. Defaults to "*".
        :return: A new TSCollection that includes time series orders with arguments applied.
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=True)

    def remove_overwrite(self, path_glob, var_glob="*"):
        """
        Removes overwrite argument to time series orders.

        :param path_glob: Glob pattern to match to source history files.
        :param var_glob: Glob pattern to match to primary variable names. Defaults to "*".
        :return: A new TSCollection that includes time series orders with arguments applied.
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=False)

    def get_dask_delayed(self):
        """Gets list of delayed time series generation functions."""
        delayed_orders = []
        for args in self.__orders:
            delayed_orders.append(dask.delayed(generate_time_series)(**args))
        return delayed_orders

    def create_directories(self, exist_ok=True):
        """Creates directory structure to output time series files to."""
        for order_dict in self.__orders:
            makedirs(Path(order_dict['ts_path_template']).parent, exist_ok=exist_ok)
    
    def execute(self):
        """Execute delayed time series generation functions across the Dask cluster."""
        self.create_directories()
        return self.__dask_client.compute(self.get_dask_delayed(), sync=True)