#!/usr/bin/env python
"""
timeseries.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 01/31/25
"""
import numpy as np
import fnmatch
from os.path import isfile
from os import remove, makedirs
from pathlib import Path
from gents.meta import get_attributes
from gents.mhfdataset import MHFDataset
from gents.datastore import GenTSDataStore
from gents.utils import get_version, LOG_LEVEL_IO_WARNING, ProgressBar
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
import logging
import copy
import warnings

logger = logging.getLogger(__name__)

def check_timeseries_integrity(ts_path: str):
    """
    Checks whether a time-series file was written completely by GenTS.

    Opens the file and looks for the ``gents_version`` global attribute, which
    is stamped on every successfully completed output file.

    :param ts_path: Path to the time-series netCDF file to inspect.
    :type ts_path: str
    :returns: ``True`` if ``gents_version`` is present (file likely complete),
        ``False`` if absent or the file cannot be opened (possible corruption).
    :rtype: bool
    """
    try:
        with GenTSDataStore(ts_path, mode="r") as ts_ds:
            attrs = get_attributes(ts_ds)
        if "gents_version" in attrs:
            return True
    except OSError:
        logger.log(LOG_LEVEL_IO_WARNING, f"Corrupt timeseries output: '{ts_path}'")
    return False


def check_timeseries_conform(ts_path: str):
    """
    Checks whether a time-series file meets the GenTS chunking conventions.

    A conforming file satisfies:

    - The ``time`` variable is stored contiguously (chunk sizes equal shape).
    - Every multi-dimensional variable is either stored contiguously, or its
      per-time-step chunk occupies at least 4 MiB.

    :param ts_path: Path to the time-series netCDF file to inspect.
    :type ts_path: str
    :returns: ``True`` if the file conforms to the chunking conventions,
        ``False`` otherwise.
    :rtype: bool
    """
    with GenTSDataStore(ts_path, mode="r") as ts_ds:
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


def write_timeseries_file(agg_hf_ds, ts_out_path, primary_var, secondary_vars_data, overwrite=False, complevel=0, compression=None):
    """
    Writes a single time-series netCDF file for one primary variable.

    Behaviour when the output file already exists:

    - ``overwrite=True``: the existing file is deleted and recreated.
    - ``overwrite=False``: :func:`check_timeseries_integrity` is called; if the
      file passes the integrity check it is returned immediately (skipped);
      otherwise the corrupt file is deleted and recreated.

    The primary variable is written with adaptive chunksizes: files smaller than
    4 MiB are stored contiguously; larger files are chunked along the time axis
    to keep each chunk near 4 MiB.  Secondary variables are written with their
    full shape as chunk sizes.  The global attributes are stamped with a
    ``gents_version`` entry on completion.

    :param agg_hf_ds: Open :class:`~gents.mhfdataset.MHFDataset` providing
        aggregated data for the history file group.
    :type agg_hf_ds: gents.mhfdataset.MHFDataset
    :param ts_out_path: Full output path for the time-series file.
    :type ts_out_path: str
    :param primary_var: Name of the primary variable to extract, or
        ``'auxiliary'`` to write only secondary variables.
    :type primary_var: str
    :param secondary_vars_data: Pre-loaded secondary variable data as a
        ``{var_name: numpy.ndarray}`` dictionary.
    :type secondary_vars_data: dict
    :param overwrite: If ``True``, overwrite any existing file. Defaults to
        ``False``.
    :type overwrite: bool
    :param complevel: netCDF4 compression level (0–9). Defaults to ``0``
        (no compression).
    :type complevel: int
    :param compression: netCDF4 compression algorithm (e.g. ``'zlib'``).
        Defaults to ``None``.
    :type compression: str or None
    :returns: Path to the written (or skipped) output file.
    :rtype: str
    """
    global_attrs = agg_hf_ds.get_global_attrs()

    if overwrite and isfile(ts_out_path):
        remove(ts_out_path)
    elif not overwrite and isfile(ts_out_path):
        if check_timeseries_integrity(ts_out_path):
            return ts_out_path
        else:
            remove(ts_out_path)

    with GenTSDataStore(ts_out_path, mode="w") as ts_ds:
        if primary_var != "auxiliary":
            var_shape = agg_hf_ds.get_var_data_shape(primary_var)
            var_dims = agg_hf_ds.get_var_dimensions(primary_var)
            for index, dim in enumerate(var_dims):
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, var_shape[index])

            var_dtype = agg_hf_ds.get_var_dtype(primary_var)
            if np.prod(var_shape)*var_dtype.itemsize < 4*(1024**2):
                chunksizes = var_shape
            else:
                time_chunk_size = max(1, 4*(1024**2) // (np.prod(var_shape[1:]) * var_dtype.itemsize))
                chunksizes = [time_chunk_size] + var_shape[1:]

            var_data = ts_ds.createVariable(primary_var,
                                            var_dtype,
                                            var_dims,
                                            complevel=complevel,
                                            compression=compression,
                                            chunksizes=chunksizes)
            var_data.set_auto_mask(False)
            var_data.set_auto_scale(False)
            var_data.set_always_mask(False)
            
            ts_ds[primary_var].setncatts(agg_hf_ds.get_var_attrs(primary_var))

            if len(var_shape) > 0 and "time" in var_dims:
                for i in range(0, var_shape[0], chunksizes[0]):
                    end = min(i + chunksizes[0], var_shape[0])
                    var_data[i:end] = agg_hf_ds.get_var_vals(
                        primary_var, time_index_start=i, time_index_end=end
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


def generate_time_series(hf_paths, ts_path_template, secondary_vars, ts_args):
    """
    Generates time-series files for a group of history files.

    Opens an :class:`~gents.mhfdataset.MHFDataset` over ``hf_paths``,
    pre-loads all secondary variable data, then calls
    :func:`write_timeseries_file` for each primary variable described in
    ``ts_args``.

    :param hf_paths: Paths to the history files forming the group.
    :type hf_paths: list[str or pathlib.Path]
    :param ts_path_template: Output path prefix (without variable name or
        timestamp suffix).
    :type ts_path_template: str
    :param secondary_vars: Names of secondary variables to read and embed in
        every output file.
    :type secondary_vars: list[str]
    :param ts_args: Dictionary mapping each primary variable name to a dict of
        keyword arguments for :func:`write_timeseries_file` (must include a
        ``'ts_string'`` key for the timestamp suffix).
    :type ts_args: dict
    :returns: List of paths to the generated time-series files.
    :rtype: list[str]
    """
    ts_paths = []
    with MHFDataset(hf_paths) as agg_hf_ds:
        secondary_vars_data = {}
        
        for variable in secondary_vars:
            secondary_vars_data[variable] = agg_hf_ds.get_var_vals(variable)
        
        for variable in ts_args:
            args = copy.deepcopy(ts_args[variable])
            ts_string = args["ts_string"]
            ts_out_path = f"{ts_path_template}.{variable}.{ts_string}.nc"
            del args["ts_string"]

            ts_paths.append(write_timeseries_file(
                agg_hf_ds=agg_hf_ds,
                ts_out_path=ts_out_path,
                primary_var=variable,
                secondary_vars_data=secondary_vars_data,
                **args
            ))
    
    return ts_paths


def get_timestamp_format(dt, subhour_format="%Y%m%d%H%M%S", hourly_format="%Y%m%d%H", daily_format="%Y%m%d", monthly_format="%Y%m", yearly_format="%Y"):
    """
    Returns a ``strftime`` format string appropriate for a given time-step duration.

    :param dt: Duration of a single model time step.
    :type dt: datetime.timedelta
    :param subhour_format: Format string for sub-minute time steps. Defaults to ``'%Y%m%d%H%M%S'``.
    :type subhour_format: str
    :param hourly_format: Format string for hour-level time steps (< 24 h). Defaults to ``'%Y%m%d%H'``.
    :type hourly_format: str
    :param daily_format: Format string for day-level time steps (< 28 days). Defaults to ``'%Y%m%d'``.
    :type daily_format: str
    :param monthly_format: Format string for month-level time steps (< 12 months). Defaults to ``'%Y%m'``.
    :type monthly_format: str
    :param yearly_format: Format string for year-level time steps. Defaults to ``'%Y'``.
    :type yearly_format: str
    :returns: ``strftime``-compatible format string.
    :rtype: str
    """
    minutes = dt.total_seconds() / 60
    hours = minutes / 60
    days = hours / 24
    months = days / 30

    if minutes < 1:
        time_format = subhour_format
    elif 0 < hours < 24:
        time_format = hourly_format
    elif 0 < days < 28:
        time_format = daily_format
    elif 0 < months < 12:
        time_format = monthly_format
    else:
        time_format = yearly_format
    
    return time_format


class TSCollection:
    """
    Manages the set of time-series generation orders derived from an ``HFCollection``.

    Each *order* is a dictionary describing one output file: source history file
    paths, output path template, primary variable name, secondary variable names,
    and generation arguments (compression, overwrite flag, etc.).  All modifier
    methods return new ``TSCollection`` instances, preserving an immutable-style
    fluent API.
    """

    def __init__(self, hf_collection, output_dir, ts_orders=None, num_processes=None, dask_client=None):
        """
        Builds the time-series order list from a processed ``HFCollection``.

        If ``ts_orders`` is not supplied, constructs one order per primary variable
        per history file group by:

        1. Sorting the collection along time via
           :meth:`~gents.hfcollection.HFCollection.sort_along_time`.
        2. Iterating over groups, reading primary/secondary variable lists from
           the first file's metadata.
        3. Selecting a timestamp format via :func:`get_timestamp_format` based on
           the group's time-step delta.
        4. Forming a ``start_time-end_time`` string from all CFTime values in the
           group.
        5. Appending one order dict per primary variable (or an ``'auxiliary'``
           order when there are no primary variables).

        :param hf_collection: History file collection to derive time-series from.
        :type hf_collection: gents.hfcollection.HFCollection
        :param output_dir: Root directory to write time-series output files to.
        :type output_dir: str
        :param ts_orders: Pre-built list of order dictionaries. When supplied,
            order construction is skipped. Defaults to ``None``.
        :type ts_orders: list or None
        :param num_processes: Maximum number of worker processes for parallel
            execution. Defaults to ``None`` (single process).
        :type num_processes: int or None
        :param dask_client: Deprecated. Pass ``num_processes`` instead.
        """
        if dask_client is not None:
            warnings.warn("Dask is no longer implemented in GenTS. Use the 'num_processes' argument to enable parallelism or reference the ReadTheDocs for using Dask.", DeprecationWarning, stacklevel=2)

        self.__num_processes = 1
        if num_processes is not None:
            self.__num_processes = num_processes
        
        hf_collection = hf_collection.sort_along_time()

        self.__hf_collection = hf_collection
        self.__groups = self.__hf_collection.get_groups()
        self.__output_dir = output_dir
        
        if ts_orders is None:
            self.__orders = list(self.update_ts_orders())
            logger.debug(f"TSCollection initialized at '{self.__output_dir}'.")
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
        return self.__orders

    def values(self):
        return self.__orders
    
    def get_hf_collection(self):
        """
        Returns the underlying ``HFCollection``.

        :returns: The history file collection this ``TSCollection`` was derived from.
        :rtype: gents.hfcollection.HFCollection
        """
        return self.__hf_collection
    
    def get_output_dir(self):
        """
        Returns the output directory path for generated time series files.

        :returns: Absolute path to the output directory.
        :rtype: str
        """
        return self.__output_dir

    def update_ts_orders(self, strfrmt_kwargs={}, time_alignment_method="midpoint"):
        """
        Rebuilds the time-series order list and returns a new ``TSCollection``.

        Re-derives one order per primary variable per history file group, applying
        ``strfrmt_kwargs`` to override individual timestamp format strings and
        ``time_alignment_method`` to control which point within each time bound is
        used when computing ``start_time`` / ``end_time`` for the output filename.

        Time alignment methods:

        - ``'midpoint'`` *(default)*: midpoint of the first time bound.
        - ``'direct_time'``: raw ``time`` coordinate values (ignores bounds).
        - ``'start_bound'``: lower edge of the first time bound.
        - ``'end_bound'``: upper edge of the first time bound.

        :param strfrmt_kwargs: Format-string overrides forwarded to
            :func:`get_timestamp_format` (e.g. ``{'monthly_format': '%Y%m%d'}``).
            Defaults to ``{}``.
        :type strfrmt_kwargs: dict
        :param time_alignment_method: Method used to select the representative
            time value from each file's time bounds. Must be one of
            ``'midpoint'``, ``'direct_time'``, ``'start_bound'``, or
            ``'end_bound'``. Defaults to ``'midpoint'``.
        :type time_alignment_method: str
        :returns: A new ``TSCollection`` with the rebuilt order list.
        :rtype: TSCollection
        :raises ValueError: If ``time_alignment_method`` is not one of the
            accepted values.
        """
        self.__hf_collection.check_pulled()
        orders = []
        for glob_template in self.__groups:
            output_template = glob_template.split(str(self.__hf_collection.get_input_dir()))[1]
            if "[sorting_pivot]" in output_template:
                output_template = output_template.split("[sorting_pivot]")[0]
            ts_path_template = f"{self.__output_dir}{output_template}"
            hf_paths = self.__groups[glob_template]

            primary_vars = self.__hf_collection[hf_paths[0]].get_primary_variables()
            secondary_vars = self.__hf_collection[hf_paths[0]].get_secondary_variables()
            time_format = get_timestamp_format(self.__hf_collection.get_timestep_delta(hf_paths[0]), **strfrmt_kwargs)
            
            times = []
            for path in hf_paths:
                time_bnds = self.__hf_collection[path].get_cftime_bounds()
                if time_alignment_method == "direct_time" or time_bnds is None:
                    time = self.__hf_collection[path].get_cftimes()
                elif time_alignment_method == "midpoint":
                    time = [time_bnds[0][0] + (time_bnds[0][1] - time_bnds[0][0]) / 2]
                elif time_alignment_method == "start_bound":
                    time = [time_bnds[0][0]]
                elif time_alignment_method == "end_bound":
                    time = [time_bnds[0][1]]
                else:
                    raise ValueError(f"'{time_alignment_method}' is an invalid time-alignment method. Valid methods are ['direct_time', 'midpoint', 'start_bound', 'end_bound']")

                times.append(time)
            times = np.concatenate(times)
            start_time = min(times)
            end_time = max(times)

            timestamp_str = f"{start_time.strftime(time_format)}-{end_time.strftime(time_format)}"

            if len(primary_vars) > 0:
                for var in primary_vars:
                    orders.append({
                        "hf_paths": hf_paths,
                        "ts_path_template": ts_path_template[:-1],
                        "primary_var": var,
                        "secondary_vars": secondary_vars,
                        "ts_string": timestamp_str
                    })
            else:
                orders.append({
                    "hf_paths": hf_paths,
                    "ts_path_template": ts_path_template[:-1],
                    "primary_var": "auxiliary",
                    "secondary_vars": secondary_vars,
                    "ts_string": timestamp_str
                })
        return self.copy(ts_orders=orders)

    def copy(self, hf_collection=None, output_dir=None, ts_orders=None, num_processes=None):
        """
        Creates a new ``TSCollection`` derived from this one with optional overrides.

        Used as the return mechanism for all modifier methods to preserve immutability.

        :param hf_collection: ``HFCollection`` to assign to the copy. Defaults to
            the current collection.
        :type hf_collection: gents.hfcollection.HFCollection or None
        :param output_dir: Output directory to assign to the copy. Defaults to the
            current directory.
        :type output_dir: str or None
        :param ts_orders: Order list to assign to the copy. Defaults to the current
            orders.
        :type ts_orders: list or None
        :param num_processes: Worker process count for the copy. Defaults to the
            current value.
        :type num_processes: int or None
        :returns: New ``TSCollection`` instance.
        :rtype: TSCollection
        """
        if hf_collection is None:
            hf_collection = self.__hf_collection
        if output_dir is None:
            output_dir = self.__output_dir
        if ts_orders is None:
            ts_orders = self.__orders
        if num_processes is None:
            num_processes = self.__num_processes

        return TSCollection(hf_collection=hf_collection, output_dir=output_dir, ts_orders=ts_orders, num_processes=num_processes)

    def include(self, path_glob, var_glob="*"):
        """
        Returns a new collection containing only orders that match both filters.

        An order is retained if at least one of its source paths matches
        ``path_glob`` *and* its primary variable matches ``var_glob``.

        :param path_glob: ``fnmatch`` glob applied to source history file paths.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` restricted to matching orders.
        :rtype: TSCollection
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
        Returns a new collection with orders that match both filters removed.

        An order is excluded if any of its source paths matches ``path_glob`` *and*
        its primary variable matches ``var_glob``.

        :param path_glob: ``fnmatch`` glob applied to source history file paths.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``''``.
        :type var_glob: str
        :returns: New ``TSCollection`` with matching orders removed.
        :rtype: TSCollection
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
        Updates generation arguments on orders that match both filters.

        Only arguments that are not ``None`` are applied; others are left unchanged.

        :param path_glob: ``fnmatch`` glob applied to source history file paths.
            Defaults to ``'*'``.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :param level: netCDF4 compression level (0–9). Defaults to ``None``
            (unchanged).
        :type level: int or None
        :param alg: netCDF4 compression algorithm (e.g. ``'zlib'``). Defaults to
            ``None`` (unchanged).
        :type alg: str or None
        :param overwrite: Overwrite flag to apply. Defaults to ``None`` (unchanged).
        :type overwrite: bool or None
        :returns: New ``TSCollection`` with updated order arguments.
        :rtype: TSCollection
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
        Replaces a substring in the output path template of matching orders.

        Iterates over orders whose source paths match ``path_glob`` and replaces
        ``string_match`` with ``string_swap`` in each order's ``ts_path_template``.
        Used to redirect outputs to a different directory structure (e.g.
        ``'/hist/'`` → ``'/proc/tseries/'``).

        :param string_match: Substring to find in the output path template.
        :type string_match: str
        :param string_swap: Replacement string.
        :type string_swap: str
        :param path_glob: ``fnmatch`` glob applied to source history file paths.
            Defaults to ``'*'``.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` with updated path templates.
        :rtype: TSCollection
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
        Applies compression settings to matching time-series orders.

        Convenience wrapper around :meth:`add_args`.

        :param level: netCDF4 compression level (0–9).
        :type level: int
        :param alg: netCDF4 compression algorithm (e.g. ``'zlib'``).
        :type alg: str
        :param path_glob: ``fnmatch`` glob applied to source history file paths.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` with compression arguments applied.
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, level=level, alg=alg)

    def apply_overwrite(self, path_glob, var_glob="*"):
        """
        Sets the overwrite flag on matching time-series orders.

        Convenience wrapper around :meth:`add_args` with ``overwrite=True``.

        :param path_glob: ``fnmatch`` glob applied to source history file paths.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` with overwrite enabled on matching orders.
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=True)

    def append_timestep_dirs(self, var_glob="*"):
        """
        Inserts a time-step frequency subdirectory into each matching order's output path.

        Determines the frequency label from the group's timestep delta:
        ``'hour_N'``, ``'day_N'``, ``'month_N'``, or ``'year_N'``.  The label is
        inserted as a new directory level immediately before the filename in the
        output path template, organising outputs by observation frequency.

        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` with updated output path templates.
        :rtype: TSCollection
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            if fnmatch.fnmatch(order_dict["primary_var"], var_glob):
                dt = self.__hf_collection.get_timestep_delta(order_dict["hf_paths"][0])

                if dt is None:
                    timestep_label = "unsorted"
                else:
                    hours = np.rint(dt.total_seconds() / 60.0 / 60.0)
                    days = np.rint(hours / 24.0)
                    months = np.rint(days / 30)
                    years = np.rint(months / 12)
                    if hours < 24:
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
        Clears the overwrite flag on matching time-series orders.

        Convenience wrapper around :meth:`add_args` with ``overwrite=False``.

        :param path_glob: ``fnmatch`` glob applied to source history file paths.
        :type path_glob: str
        :param var_glob: ``fnmatch`` glob applied to primary variable names.
            Defaults to ``'*'``.
        :type var_glob: str
        :returns: New ``TSCollection`` with overwrite disabled on matching orders.
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=False)

    def create_directories(self, exist_ok=True):
        """
        Creates the output directory tree for all time-series orders.

        :param exist_ok: If ``True`` (default), no error is raised when a
            directory already exists.
        :type exist_ok: bool
        """
        logger.info("Creating directory structure for time series output.")
        for order_dict in self.__orders:
            makedirs(Path(order_dict['ts_path_template']).parent, exist_ok=exist_ok)

    def execute(self, optimize=True, optimize_batch_n=200, raise_errors=False):
        """
        Executes all time-series generation orders in parallel.

        When ``optimize=True`` (default), orders that share the same first source
        file are batched together (up to ``optimize_batch_n`` per batch) so that
        :func:`generate_time_series` opens each group of history files only once
        and writes multiple primary-variable output files per worker invocation,
        significantly reducing file I/O overhead.

        When ``optimize=False``, each order is submitted as a separate worker task
        (one file open per variable).

        :param optimize: If ``True`` (default), batch orders sharing the same
            source files into single worker calls.
        :type optimize: bool
        :param optimize_batch_n: Maximum number of variables per optimised batch.
            Defaults to ``200``.
        :type optimize_batch_n: int
        :param raise_errors: If ``True`` (default ``False``), calls errors are raised
            rather than just logged.
        :type raise_errors: bool
        :returns: List of paths to all generated time-series output files.
        :rtype: list[str]
        """
        self.create_directories()
        results = []

        optimized_orders = []
        if optimize:
            order_index_merge_map = {}
            for index, order in enumerate(self.__orders):
                first_hf_path = order["hf_paths"][0]
                if first_hf_path in order_index_merge_map:
                    order_index_merge_map[first_hf_path].append(index)
                else:
                    order_index_merge_map[first_hf_path] = [index]
            
            batched_index_lists = []
            for first_hf_path in order_index_merge_map:
                indices = order_index_merge_map[first_hf_path]
                chunked_indices = [indices[i:i+optimize_batch_n] for i in range(0, len(indices), optimize_batch_n)]
                for index_list in chunked_indices:
                    batched_index_lists.append(index_list)


            for index_list in batched_index_lists:
                init_index = index_list[0]
                init_order = self.__orders[init_index]
                ts_args = {}

                for index in index_list:
                    primary_var = self.__orders[index]["primary_var"]
                    args = copy.deepcopy(self.__orders[index])
                    del args["hf_paths"]
                    del args["ts_path_template"]
                    del args["secondary_vars"]
                    del args["primary_var"]
                    ts_args[primary_var] = args

                optimized_orders.append({
                    "hf_paths": init_order["hf_paths"],
                    "ts_path_template": init_order["ts_path_template"],
                    "secondary_vars": init_order["secondary_vars"],
                    "ts_args": ts_args
                })
        else:
            for index, order in enumerate(self.__orders):
                args = copy.deepcopy(order)
                del args["hf_paths"]
                del args["ts_path_template"]
                del args["secondary_vars"]
                del args["primary_var"]
                ts_args = {order["primary_var"]: args}
                optimized_orders.append({
                    "hf_paths": order["hf_paths"],
                    "ts_path_template": order["ts_path_template"],
                    "secondary_vars": order["secondary_vars"],
                    "ts_args": ts_args
                })

        with ProcessPoolExecutor(max_workers=self.__num_processes) as executor:
            futures = {executor.submit(generate_time_series, **args): args for args in optimized_orders}
            prog_bar = ProgressBar(total=len(futures), label="Generating Timeseries")
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    path = futures[future]
                    logger.warning(f"Failed to load metadata for {path}: {exc}", exc_info=True)
                    if raise_errors:
                        raise
                finally:
                    prog_bar.step()
        
        output_paths = []
        for result in results:
            for path in result:
                output_paths.append(path)

        return output_paths