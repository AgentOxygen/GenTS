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
from os.path import isfile
from os import remove, makedirs
from pathlib import Path
from gents.meta import get_attributes
from gents.mhfdataset import MHFDataset
from gents.utils import get_version, LOG_LEVEL_IO_WARNING, ProgressBar
import traceback
import logging
import copy

logger = logging.getLogger(__name__)

try:
    import dask
    DASK_INSTALLED = True
except ImportError:
    DASK_INSTALLED = False
    logger.debug("Dask not installed. Proceeding in serial.")


def check_timeseries_integrity(ts_path: str):
    """
    Checks integrity of time series netCDF file by confirming `gents_version` attribute.

    :param ts_path: Path to time series file.
    :return: True if `gents_version` attribute is found. False if not (suggesting possible corruption).
    """
    try:
        with netCDF4.Dataset(ts_path, mode="r") as ts_ds:
            attrs = get_attributes(ts_ds)
        if "gents_version" in attrs:
            return True
    except OSError:
        logger.log(LOG_LEVEL_IO_WARNING, f"Corrupt timeseries output: '{ts_path}'")
    return False


def check_timeseries_conform(ts_path: str):
    with netCDF4.Dataset(ts_path, mode="r") as ts_ds:
        if list(ts_ds["time"].chunking()) != list(ts_ds["time"].shape):
            return False
        for variable in ts_ds.variables:
            if list(ts_ds[variable].chunking()) == list(ts_ds[variable].shape):
                continue
                
            if "time" not in ts_ds[variable].dimensions or len(ts_ds[variable].shape) == 1:
                return False
            else:
                chunking = list(ts_ds[variable].chunking())
                chunking[0] += 1
                bumped_size = np.prod(chunking)*ts_ds[variable].dtype.itemsize
                if bumped_size < 4*(1024**2):
                    return False
        
    return True


def generate_time_series_error_wrapper(**args):
    try:
        return generate_time_series(**args)
    except Exception as e:
        print("=====================================================\n")
        print(f"START of GenTS Argument Dump for {type(e)}\n")
        print("=====================================================")
        for entry in args:
            print(f"\nArgument: '{entry}' \n")
            print(args[entry])
        print(f"\nError: '{type(e)}' \n")
        print(e)
        traceback.print_exc()
        print("=====================================================\n")
        print(f"END of GenTS Argument Dump for {type(e)}\n")
        print("=====================================================")
        raise type(e)(f"{e}") from e


def generate_time_series(hf_paths, ts_path_template, primary_var, secondary_vars, ts_string, complevel=0, compression=None, overwrite=False, reference_structure=None):
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

    ts_out_path = None
    with MHFDataset(hf_paths) as agg_hf_ds:
        global_attrs = agg_hf_ds.get_global_attrs()
        secondary_vars_data = {}
        
        for variable in secondary_vars:
            secondary_vars_data[variable] = agg_hf_ds.get_var_vals(variable)
        
        ts_out_path = f"{ts_path_template}.{primary_var}.{ts_string}.nc"

        if overwrite and isfile(ts_out_path):
            remove(ts_out_path)
        elif not overwrite and isfile(ts_out_path):
            if check_timeseries_integrity(ts_out_path):
                return ts_out_path
            else:
                remove(ts_out_path)

        with netCDF4.Dataset(ts_out_path, mode="w") as ts_ds:
            if primary_var != "auxiliary":
                var_shape = agg_hf_ds.get_var_data_shape(primary_var)
                var_dims = agg_hf_ds.get_var_dimensions(primary_var)
                for index, dim in enumerate(var_dims):
                    if dim == "time":
                        ts_ds.createDimension(dim, None)
                    else:
                        ts_ds.createDimension(dim, var_shape[index])

                var_dtype = agg_hf_ds.get_var_dtype(primary_var)
                chunksizes = None
                if np.prod(var_shape)*var_dtype.itemsize < 4*(1024**2):
                    chunksizes = var_shape

                var_data = ts_ds.createVariable(primary_var,
                                                agg_hf_ds.get_var_dtype(primary_var),
                                                var_dims,
                                                complevel=complevel,
                                                compression=compression,
                                                chunksizes=chunksizes)
                var_data.set_auto_mask(False)
                var_data.set_auto_scale(False)
                var_data.set_always_mask(False)
                
                ts_ds[primary_var].setncatts(agg_hf_ds.get_var_attrs(primary_var))

                time_chunk_size = 1
                if len(var_shape) > 0 and "time" in var_dims:
                    for i in range(0, var_shape[0], time_chunk_size):
                        if i + time_chunk_size > var_shape[0]:
                            time_chunk_size = var_shape[0] - i
                        var_data[i:i + time_chunk_size] = agg_hf_ds.get_var_vals(
                            primary_var, time_index_start=i, time_index_end=i+time_chunk_size
                        )
                else:
                    var_data[:] = agg_hf_ds.get_var_vals(primary_var)

            for secondary_var in secondary_vars_data:
                var_shape = agg_hf_ds.get_var_data_shape(secondary_var)
                var_dims = agg_hf_ds.get_var_dimensions(secondary_var)

                for index, dim in enumerate(var_dims):
                    if dim not in ts_ds.dimensions:
                        if dim == "time":
                            ts_ds.createDimension(dim, None)
                        else:
                            ts_ds.createDimension(dim, var_shape[index])
                
                svar_data = ts_ds.createVariable(secondary_var,
                                                agg_hf_ds.get_var_dtype(secondary_var),
                                                var_dims,
                                                complevel=complevel,
                                                compression=compression,
                                                chunksizes=var_shape)
                
                svar_data.set_auto_mask(False)
                svar_data.set_auto_scale(False)
                svar_data.set_always_mask(False)

                ts_ds[secondary_var].setncatts(agg_hf_ds.get_var_attrs(secondary_var))
                svar_data[:] = secondary_vars_data[secondary_var]
            
            ts_ds.setncatts(global_attrs | {"gents_version": str(get_version())})
    return ts_out_path


def get_timestamp_format(dt):
    """
    Creates timestamp string to describe time range for netCDF dataset

    :param times: Time values for netCDF dataset in integer form with units and calendar attributes.
    :return: String containing appropriate timestamp.
    """
    minutes = dt.total_seconds() / 60
    hours = minutes / 60
    days = hours / 24
    months = days / 30

    if minutes < 1:
        time_format = "%Y%m%d%H%M%S"
    elif 0 < hours < 24:
        time_format = "%Y%m%d%H"
    elif 0 < days < 28:
        time_format = "%Y%m%d"
    elif 0 < months < 12:
        time_format = "%Y%m"
    else:
        time_format = "%Y"
    
    return time_format


class TSCollection:
    """Time Series Collection that faciliates the creation of time series from a HFCollection."""
    def __init__(self, hf_collection, output_dir, ts_orders=None, dask_client=None):
        """
        :param hf_collection: History file collection to derive time series from
        :param output_dir: Directory to output time series files to
        :param ts_orders: List of Dask delayed functions of generate_time_series
        :param dask_client: Dask client to use when executing time series batches (Default: global client).
        """
        if dask_client is None and DASK_INSTALLED:
            self.__dask_client = dask.distributed.client._get_global_client()
        else:
            self.__dask_client = dask_client
        
        hf_collection = hf_collection.sort_along_time()

        self.__hf_collection = hf_collection
        self.__groups = self.__hf_collection.get_groups()
        self.__output_dir = output_dir
        
        if ts_orders is None:
            self.__hf_collection.pull_metadata()
            self.__orders = []
            for glob_template in self.__groups:
                output_template = glob_template.split(str(self.__hf_collection.get_input_dir()))[1]
                if "[sorting_pivot]" in output_template:
                    output_template = output_template.split("[sorting_pivot]")[0]
                ts_path_template = f"{self.__output_dir}{output_template}"
                hf_paths = self.__groups[glob_template]

                primary_vars = self.__hf_collection[hf_paths[0]].get_primary_variables()
                secondary_vars = self.__hf_collection[hf_paths[0]].get_secondary_variables()
                time_format = get_timestamp_format(self.__hf_collection.get_timestep_delta(hf_paths[0]))
                
                times = []
                for path in hf_paths:
                    times.append(self.__hf_collection[path].get_cftimes())
                start_time = np.min(times)
                end_time = np.max(times)

                timestamp_str = f"{start_time.strftime(time_format)}-{end_time.strftime(time_format)}"

                if len(primary_vars) > 0:
                    for var in primary_vars:
                        self.__orders.append({
                            "hf_paths": hf_paths,
                            "ts_path_template": ts_path_template[:-1],
                            "primary_var": var,
                            "secondary_vars": secondary_vars,
                            "ts_string": timestamp_str
                        })
                else:
                    self.__orders.append({
                        "hf_paths": hf_paths,
                        "ts_path_template": ts_path_template[:-1],
                        "primary_var": "auxiliary",
                        "secondary_vars": secondary_vars,
                        "ts_string": timestamp_str
                    })

            logger.debug(f"TSCollection initialized at '{output_dir}'.")
            logger.debug(f"{len(self.__orders)} timeseries orders generated.")
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
    
    def copy(self, hf_collection=None, output_dir=None, ts_orders=None, dask_client=None):
        """
        Copies data of this TSCollection into a new one.
    
        :param hf_collection: HFCollection to assign to copy (defaults to existing).
        :param output_dir: Head output directory to assign to copy (defaults to existing).
        :param ts_orders: Time series orders to assign to copy (defaults to existing).
        :param dask_client: Dask client to assign to copy (defaults to existing).
        :return: TSCollection that is a copy.
        """
        if hf_collection is None:
            hf_collection = self.__hf_collection
        if output_dir is None:
            output_dir = self.__output_dir
        if ts_orders is None:
            ts_orders = self.__orders
        if dask_client is None:
            dask_client = self.__dask_client

        return TSCollection(hf_collection=hf_collection, output_dir=output_dir, ts_orders=ts_orders, dask_client=dask_client)

    def include(self, path_glob, var_glob="*"):
        """
        Applies inclusive filter to time series orders.

        :param path_glob: Glob pattern to apply to source history files.
        :param var_glob: Glob pattern to apply to primary variable names. Defaults to "*"
        :return: A new TSCollection that only includes time series orders that match the filter.
        """
        filtered_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            path_matched = False
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_matched = True
                    break
            
            if path_matched and fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                filtered_orders.append(order_dict)
        logger.debug(f"Inclusive filter(s) applied: '{var_glob}' to history files matching '{path_glob}'")
        return self.copy(ts_orders=filtered_orders)

    def exclude(self, path_glob, var_glob=""):
        """
        Applies exclusive filter to time series orders.

        :param path_glob: Glob pattern to apply to source history files.
        :param var_glob: Glob pattern to apply to primary variable names. Defaults to ""
        :return: A new TSCollection that excludes time series orders that match the filter.
        """
        filtered_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            path_unmatched = True
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    path_unmatched = False
                    break
            
            if path_unmatched and not fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                filtered_orders.append(order_dict)
        logger.debug(f"Exclusive filter(s) applied: '{var_glob}' to history files matching '{path_glob}'")
        return self.copy(ts_orders=filtered_orders)

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
        for order_dict in copy.deepcopy(self.__orders):
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

        logger.debug(f"Arguments applied (excluding None): ['level': {level}, 'alg': {alg}, 'overwrite': {overwrite}] to history files matching '{path_glob}' and variables matching '{var_glob}'.")
        return self.copy(ts_orders=new_orders)

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
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            for path in order_dict["hf_paths"]:
                if fnmatch.fnmatch(path, path_glob):
                    order_dict["ts_path_template"] = order_dict["ts_path_template"].replace(string_match, string_swap)
            new_orders.append(order_dict)
    
        logger.debug(f"Path swap '{string_match}' -> '{string_swap}' to history files matching '{path_glob}' and variables matching '{var_glob}'.")
        return self.copy(ts_orders=new_orders)
        
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

    def append_timestep_dirs(self, var_glob="*"):
        """
        Appends directories named according to time-step to the end of the timeseries output path templates.
        This sorts the output into bins by the time-step frequency (i.e. hour_1, day_1, month_1, year_1)

        :param var_glob: Glob pattern to match to primary variable names. Defaults to "*".
        :return: A new TSCollection with time-step directories added.
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            if fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                dt = self.__hf_collection.get_timestep_delta(order_dict["hf_paths"][0])
                hours = np.rint(dt.total_seconds() / 60.0 / 60.0)
                days = np.rint(hours / 24.0)
                months = np.rint(days / 30)
                years = np.rint(months / 12)

                if dt is None:
                    timestep_label = "unsorted"
                elif hours < 24:
                    timestep_label = f"hour_{int(hours)}"
                elif days < 28:
                    timestep_label = f"day_{int(days)}"
                elif months < 12:
                    timestep_label = f"month_{int(months)}"
                else:
                    timestep_label = f"year_{int(years)}"

                template = Path(order_dict["ts_path_template"])
                order_dict["ts_path_template"] = str(template.parent) + f"/{timestep_label}/" + template.name

                new_orders.append(order_dict)
        return self.copy(ts_orders=new_orders)

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
        if DASK_INSTALLED:
            delayed_orders = []
            for args in self.__orders:
                delayed_orders.append(dask.delayed(generate_time_series_error_wrapper)(**args))
            return delayed_orders
        else:
            raise ImportError("Dask not installed!")

    def create_directories(self, exist_ok=True):
        """Creates directory structure to output time series files to."""
        logger.info("Creating directory structure for time series output.")
        for order_dict in self.__orders:
            makedirs(Path(order_dict['ts_path_template']).parent, exist_ok=exist_ok)

    def execute(self):
        """Execute delayed time series generation functions across the Dask cluster."""
        self.create_directories()
        results = []
        if self.__dask_client is None:
            logger.info("No Dask client detected... proceeding in serial.")
            prog_bar = ProgressBar(total=len(self.__orders))
            for args in self.__orders:
                results.append(generate_time_series_error_wrapper(**args))
                prog_bar.step()
        else:
            logger.info("Dask client detected! Generating time series files in parallel.")
            results = self.__dask_client.compute(self.get_dask_delayed(), sync=True)
        return results