#!/usr/bin/env python
"""
Construction and execution of time series generation orders, and file writing.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
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

CHUNK_TARGET_BYTES = 4 * (1024**2)

DEFAULT_MEMORY_LIMIT_BYTES = 4 * (1024**3)


def compute_chunksizes(var_shape, itemsize, target_bytes=CHUNK_TARGET_BYTES):
    """
    Chooses netCDF chunk sizes for a variable shaped ``(time, ...)``.

    A variable smaller than ``target_bytes`` is stored contiguously; a larger one
    is chunked along the time axis, as many steps per chunk as fit in the target,
    with the remaining dimensions kept whole.

    :param var_shape: Full variable shape, time axis first.
    :type var_shape: list[int]
    :param itemsize: Size in bytes of one array element.
    :type itemsize: int
    :param target_bytes: Chunk size target, 4 MiB by default.
    :type target_bytes: int
    :returns: Chunk sizes, one per dimension of ``var_shape``.
    :rtype: list[int]
    """
    if np.prod(var_shape) * itemsize < target_bytes:
        return var_shape
    time_chunk_size = max(1, target_bytes // (np.prod(var_shape[1:]) * itemsize))
    return [time_chunk_size] + var_shape[1:]


def check_timeseries_integrity(ts_path: str):
    """
    Checks whether a time series file was written completely by GenTS.

    The ``gents_version`` attribute is stamped last, so its presence means the
    write finished.

    :param ts_path: Path to the time series file to inspect.
    :type ts_path: str
    :returns: ``False`` if the stamp is absent or the file cannot be opened.
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
    Checks whether a time series file meets the GenTS chunking convention.

    A conforming file stores ``time`` contiguously, and every other variable
    either contiguously or in time chunks of at least :data:`CHUNK_TARGET_BYTES`.
    Always checked against that constant, so a file written with a custom
    ``chunk_target_bytes`` will not conform.

    :param ts_path: Path to the time series file to inspect.
    :type ts_path: str
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
                if bumped_size < CHUNK_TARGET_BYTES:
                    return False
        
    return True


def _is_missing(arr, fill_value):
    """
    Reports whether every element of ``arr`` equals ``fill_value``.

    Such a slice need not be written at all: netCDF stores nothing for an
    unwritten region and returns the fill value on read. NaN is compared with
    :func:`numpy.isnan`, since ``NaN != NaN``.

    :param arr: Array of values about to be written.
    :type arr: numpy.ndarray
    :param fill_value: The variable's fill value, or ``None`` if it has none.
    :type fill_value: int or float or None
    :returns: ``False`` if ``fill_value`` is ``None`` or any value differs.
    :rtype: bool
    """
    if fill_value is None:
        return False
    if isinstance(fill_value, (float, np.floating)) and np.isnan(fill_value):
        return bool(np.all(np.isnan(arr)))
    return bool(np.all(arr == fill_value))


def write_timeseries_file(agg_hf_ds, ts_out_path, primary_var, secondary_vars_data, overwrite=False, complevel=0, compression=None, ts_start_index=None, ts_end_index=None, append_attrs=None, no_data=False, chunk_target_bytes=CHUNK_TARGET_BYTES):
    """
    Writes one time series file, holding one primary variable and every secondary.

    An existing output file is deleted and rewritten when ``overwrite`` is set;
    otherwise it is kept and skipped if it passes
    :func:`check_timeseries_integrity`, and deleted as corrupt if it does not.
    The completed file is stamped with a ``gents_version`` attribute last.

    Every variable is created with the source's ``_FillValue`` (if any) and any
    slice that is entirely that fill value is left unwritten, which is what keeps
    time series built from missing-value clones as small as their inputs. For
    ordinary data it is a no-op.

    :param agg_hf_ds: Open :class:`~gents.mhfdataset.MHFDataset` for the group.
    :type agg_hf_ds: gents.mhfdataset.MHFDataset
    :param ts_out_path: Full output path for the time series file.
    :type ts_out_path: str
    :param primary_var: Primary variable to write, or ``'auxiliary'`` to write
        only the secondary variables.
    :type primary_var: str
    :param secondary_vars_data: Pre-loaded ``{var_name: array}`` secondary data.
    :type secondary_vars_data: dict
    :param overwrite: Overwrite an existing output file rather than skip it.
    :type overwrite: bool
    :param complevel: netCDF4 compression level (0-9).
    :type complevel: int
    :param compression: netCDF4 compression algorithm, e.g. ``'zlib'``.
    :type compression: str or None
    :param ts_start_index: First time index to read from the group; ``None``
        starts at the beginning.
    :type ts_start_index: int or None
    :param ts_end_index: Time index to stop reading at; ``None`` reads to the end.
    :type ts_end_index: int or None
    :param append_attrs: Extra global attributes to stamp into the output.
    :type append_attrs: dict or None
    :param no_data: Create the primary variable but neither read nor write its
        data, leaving it to read back as its fill value. The output stays
        structurally valid and self-describing; used for conformity runs over
        missing-value clones.
    :type no_data: bool
    :param chunk_target_bytes: Chunk size target passed to
        :func:`compute_chunksizes`. A non-default value will fail
        :func:`check_timeseries_conform`.
    :type chunk_target_bytes: int
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

    if ts_start_index is None:
        ts_start_index = 0

    with GenTSDataStore(ts_out_path, mode="w") as ts_ds:
        if primary_var != "auxiliary":
            var_shape = agg_hf_ds.get_var_data_shape(primary_var)
            var_dims = agg_hf_ds.get_var_dimensions(primary_var)
            
            if ts_end_index is None:
                ts_end_index = var_shape[0]
            var_shape[0] = ts_end_index - ts_start_index

            for index, dim in enumerate(var_dims):
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, var_shape[index])

            var_dtype = agg_hf_ds.get_var_dtype(primary_var)
            chunksizes = compute_chunksizes(var_shape, var_dtype.itemsize, target_bytes=chunk_target_bytes)

            # _FillValue goes through creation so unwritten regions read back as
            # it, and is then omitted from the copied attributes (netCDF rejects
            # setting it twice).
            primary_attrs = agg_hf_ds.get_var_attrs(primary_var)
            primary_fill = primary_attrs.get("_FillValue", None)

            var_data = ts_ds.createVariable(primary_var,
                                            var_dtype,
                                            var_dims,
                                            complevel=complevel,
                                            compression=compression,
                                            chunksizes=chunksizes,
                                            fill_value=primary_fill)
            var_data.set_auto_mask(False)
            var_data.set_auto_scale(False)
            var_data.set_always_mask(False)

            ts_ds[primary_var].setncatts(
                {key: val for key, val in primary_attrs.items() if key != "_FillValue"}
            )

            if no_data:
                pass
            elif len(var_shape) > 0 and "time" in var_dims:
                for i in range(0, var_shape[0], chunksizes[0]):
                    end = min(i + chunksizes[0], var_shape[0])
                    chunk = agg_hf_ds.get_var_vals(
                        primary_var, time_index_start=ts_start_index+i, time_index_end=ts_start_index+end
                    )
                    if not _is_missing(chunk, primary_fill):
                        var_data[i:end] = chunk
            else:
                chunk = agg_hf_ds.get_var_vals(primary_var)[ts_start_index:ts_end_index]
                if not _is_missing(chunk, primary_fill):
                    var_data[:] = chunk

        for secondary_var in secondary_vars_data:
            var_shape = agg_hf_ds.get_var_data_shape(secondary_var)
            var_dims = agg_hf_ds.get_var_dimensions(secondary_var)

            if ts_end_index is None:
                ts_end_index = var_shape[0]
            if "time" in var_dims:
                var_shape[0] = ts_end_index - ts_start_index

            for index, dim in enumerate(var_dims):
                if dim not in ts_ds.dimensions:
                    if dim == "time":
                        ts_ds.createDimension(dim, None)
                    else:
                        ts_ds.createDimension(dim, var_shape[index])
            
            svar_attrs = agg_hf_ds.get_var_attrs(secondary_var)
            svar_fill = svar_attrs.get("_FillValue", None)

            svar_data = ts_ds.createVariable(secondary_var,
                                            agg_hf_ds.get_var_dtype(secondary_var),
                                            var_dims,
                                            complevel=complevel,
                                            compression=compression,
                                            chunksizes=var_shape,
                                            fill_value=svar_fill)

            svar_data.set_auto_mask(False)
            svar_data.set_auto_scale(False)
            svar_data.set_always_mask(False)

            ts_ds[secondary_var].setncatts(
                {key: val for key, val in svar_attrs.items() if key != "_FillValue"}
            )
            if "time" in var_dims:
                svar_vals = secondary_vars_data[secondary_var][ts_start_index:ts_end_index]
            else:
                svar_vals = secondary_vars_data[secondary_var]
            if not _is_missing(svar_vals, svar_fill):
                svar_data[:] = svar_vals
        
        if append_attrs is None:
            append_attrs = {}
        ts_ds.setncatts(global_attrs | append_attrs | {"gents_version": str(get_version())})
    return ts_out_path


def generate_time_series(hf_paths, ts_path_template, secondary_vars, ts_args, no_data=False, memory_limit_bytes=DEFAULT_MEMORY_LIMIT_BYTES):
    """
    Generates every time series file for one group of history files.

    Opens the group once as an :class:`~gents.mhfdataset.MHFDataset`, reads the
    secondary variables, then writes one file per primary variable in ``ts_args``.
    This is the unit of work submitted to the process pool by
    :meth:`TSCollection.execute`, so its arguments must stay picklable.

    :param hf_paths: Paths to the history files forming the group.
    :type hf_paths: list[str or pathlib.Path]
    :param ts_path_template: Output path prefix, without variable or timestamp.
    :type ts_path_template: str
    :param secondary_vars: Secondary variables to embed in every output file.
    :type secondary_vars: list[str]
    :param ts_args: ``{primary variable: kwargs}`` for
        :func:`write_timeseries_file`; each must carry a ``'ts_string'`` key
        holding the timestamp suffix.
    :type ts_args: dict
    :param no_data: Skip reading and writing primary variable data.
    :type no_data: bool
    :param memory_limit_bytes: Cache ceiling for the ``MHFDataset``.
    :type memory_limit_bytes: float
    :returns: Paths to the generated time series files.
    :rtype: list[str]
    """
    ts_paths = []
    preloads = [] if no_data else [name for name in list(ts_args) if name != "auxiliary"]
    with MHFDataset(hf_paths, preload_var_list=preloads, memory_limit_bytes=memory_limit_bytes,
                    preload_primaries=not no_data) as agg_hf_ds:
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
                no_data=no_data,
                **args
            ))
    return ts_paths


def get_timestamp_format(dt, subhour_format="%Y%m%d%H%M%S", hourly_format="%Y%m%d%H", daily_format="%Y%m%d", monthly_format="%Y%m", yearly_format="%Y"):
    """
    Returns the ``strftime`` format to timestamp output files of a given frequency.

    :param dt: Duration of a single model time step.
    :type dt: datetime.timedelta
    :param subhour_format: Format for sub-minute steps.
    :type subhour_format: str
    :param hourly_format: Format for steps under 24 hours.
    :type hourly_format: str
    :param daily_format: Format for steps under 28 days.
    :type daily_format: str
    :param monthly_format: Format for steps under 12 months.
    :type monthly_format: str
    :param yearly_format: Format for anything longer.
    :type yearly_format: str
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


def get_timestep_label(dt):
    """
    Returns the frequency label for a time-step duration: ``'hour_N'``,
    ``'day_N'``, ``'month_N'``, ``'year_N'``, or ``'unsorted'`` if unknown.

    Used by :meth:`TSCollection.append_timestep_dirs` as a directory name.

    :param dt: Duration of a single model time step, or ``None`` if unknown.
    :type dt: datetime.timedelta or None
    :rtype: str
    """
    if dt is None:
        return "unsorted"

    hours = np.rint(dt.total_seconds() / 60.0 / 60.0)
    days = np.rint(hours / 24.0)
    months = np.rint(days / 30)
    years = np.rint(months / 12)

    if hours < 24:
        return f"hour_{int(hours)}"
    elif days < 28:
        return f"day_{int(days)}"
    elif months < 12:
        return f"month_{int(months)}"
    return f"year_{int(years)}"


def _matches_any(value, globs):
    """
    Returns whether ``value`` matches at least one ``fnmatch`` glob.

    :param value: String to test, e.g. a history file path or a variable name.
    :type value: str
    :param globs: One or more glob patterns; a single string is also accepted.
    :type globs: list[str] or str
    :rtype: bool
    """
    if type(globs) is str:
        globs = [globs]
    return any(fnmatch.fnmatch(value, glob) for glob in globs)


class TSCollection:
    """
    The set of time series generation orders derived from an ``HFCollection``.

    An *order* is a dictionary describing one output file: its source history file
    paths, output path template, primary and secondary variables, timestamp
    string, and any generation arguments added by the modifier methods. Every
    modifier returns a new ``TSCollection``.
    """

    def __init__(self, hf_collection, output_dir, ts_orders=None, num_processes=None, dask_client=None):
        """
        Builds the order list from a history file collection.

        Unless ``ts_orders`` is supplied, the collection is sorted along time and
        one order is built per primary variable per group (or a single
        ``'auxiliary'`` order for a group with no primary variables). Requires
        metadata, and pulls it if necessary.

        :param hf_collection: History file collection to derive orders from.
        :type hf_collection: gents.hfcollection.HFCollection
        :param output_dir: Root directory to write time series files to.
        :type output_dir: str
        :param ts_orders: Pre-built orders; skips order construction.
        :type ts_orders: list or None
        :param num_processes: Worker processes used by :meth:`execute`.
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
        Returns the ``HFCollection`` this collection was derived from.

        :rtype: gents.hfcollection.HFCollection
        """
        return self.__hf_collection

    def get_output_dir(self):
        """
        Returns the root directory generated time series are written to.

        :rtype: str
        """
        return self.__output_dir

    def update_ts_orders(self, strfrmt_kwargs={}, time_alignment_method="midpoint"):
        """
        Rebuilds the order list and returns a new ``TSCollection``.

        One order is built per primary variable per group. The output path
        template comes from the group key with the input head directory and any
        ``[sorting_pivot]`` suffix stripped; ``hist``-to-``tseries`` style
        renaming is :meth:`apply_path_swap`'s job, applied afterwards.

        :param strfrmt_kwargs: Timestamp format overrides forwarded to
            :func:`get_timestamp_format`, e.g. ``{'monthly_format': '%Y%m%d'}``.
        :type strfrmt_kwargs: dict
        :param time_alignment_method: How to pick the representative time for the
            filename timestamp: ``'midpoint'`` of the time bound, ``'direct_time'``
            (ignoring bounds), ``'start_bound'`` or ``'end_bound'``.
        :type time_alignment_method: str
        :rtype: TSCollection
        :raises ValueError: If ``time_alignment_method`` is not one of those four.
        """
        self.__hf_collection.check_pulled()
        orders = []
        for index, glob_template in enumerate(self.__groups):
            hf_paths = self.__groups[glob_template]
            output_template = glob_template.split(str(self.__hf_collection.get_input_dir()))[1]
            if "[sorting_pivot]" in output_template:
                output_template, slice_years = output_template.split("[sorting_pivot]")
                logger.debug(f"Group [{index+1}/{len(self.__groups)}] {len(hf_paths)} files: {output_template}, sliced to [{slice_years}]")
            else:
                logger.debug(f"Group [{index+1}/{len(self.__groups)}] {len(hf_paths)} files: {output_template}")
            ts_path_template = f"{self.__output_dir}{output_template}"

            primary_vars = self.__hf_collection[hf_paths[0]].get_primary_variables()
            secondary_vars = self.__hf_collection[hf_paths[0]].get_secondary_variables()
            time_format = get_timestamp_format(self.__hf_collection.get_timestep_delta(hf_paths[0]), **strfrmt_kwargs)
            
            start_time = None
            end_time = None
            total_steps = 0
            first_cut_start = None
            last_cut_end = None
            any_cut = False
            for path in hf_paths:
                meta_ds = self.__hf_collection[path]
                float_times = np.ma.getdata(np.atleast_1d(meta_ds.get_float_times()))
                n_steps = int(float_times.shape[0])

                time_slice_bounds = self.__hf_collection.get_multistep_slices(path)
                if time_slice_bounds is not None:
                    cut_start, cut_end = time_slice_bounds[slice_years]
                else:
                    cut_start, cut_end = 0, n_steps
                effective_end = min(cut_end, n_steps)
                if cut_start != 0 or effective_end != n_steps:
                    any_cut = True
                if first_cut_start is None:
                    first_cut_start = cut_start
                last_cut_end = total_steps + effective_end
                total_steps += n_steps

                float_bnds = meta_ds.get_float_time_bounds()
                if float_bnds is None or time_alignment_method == "direct_time":
                    aligned_times = float_times[cut_start:cut_end]
                    decode = meta_ds.decode_time_values
                else:
                    bnds = np.ma.getdata(float_bnds)[cut_start:cut_end]
                    if time_alignment_method == "midpoint":
                        aligned_times = bnds[:, 0] + (bnds[:, 1] - bnds[:, 0]) / 2
                    elif time_alignment_method == "start_bound":
                        aligned_times = bnds[:, 0]
                    elif time_alignment_method == "end_bound":
                        aligned_times = bnds[:, 1]
                    else:
                        raise ValueError(f"'{time_alignment_method}' is an invalid time-alignment method. Valid methods are ['direct_time', 'midpoint', 'start_bound', 'end_bound']")
                    decode = meta_ds.decode_time_bounds_values

                if aligned_times.shape[0] > 0:
                    endpoints = np.atleast_1d(decode(np.array([np.min(aligned_times), np.max(aligned_times)])))
                    if start_time is None or endpoints[0] < start_time:
                        start_time = endpoints[0]
                    if end_time is None or endpoints[-1] > end_time:
                        end_time = endpoints[-1]

            start_index = None
            end_index = None
            if any_cut:
                start_index = int(first_cut_start)
                end_index = int(last_cut_end)
                assert start_index < end_index <= total_steps

            timestamp_str = f"{start_time.strftime(time_format)}-{end_time.strftime(time_format)}"
            if len(primary_vars) > 0:
                for var in primary_vars:
                    orders.append({
                        "hf_paths": hf_paths,
                        "ts_path_template": ts_path_template[:-1],
                        "primary_var": var,
                        "secondary_vars": secondary_vars,
                        "ts_string": timestamp_str,
                        "ts_start_index": start_index,
                        "ts_end_index": end_index
                    })
            else:
                orders.append({
                    "hf_paths": hf_paths,
                    "ts_path_template": ts_path_template[:-1],
                    "primary_var": "auxiliary",
                    "secondary_vars": secondary_vars,
                    "ts_string": timestamp_str,
                    "ts_start_index": start_index,
                    "ts_end_index": end_index
                })
        return self.copy(ts_orders=orders)

    def copy(self, hf_collection=None, output_dir=None, ts_orders=None, num_processes=None):
        """
        Returns a new collection derived from this one, with optional overrides.

        Every modifier returns through here, which is what keeps the API
        immutable. Arguments left ``None`` are inherited.

        :param hf_collection: ``HFCollection`` to assign to the copy.
        :type hf_collection: gents.hfcollection.HFCollection or None
        :param output_dir: Output directory to assign to the copy.
        :type output_dir: str or None
        :param ts_orders: Order list to assign to the copy.
        :type ts_orders: list or None
        :param num_processes: Worker process count for the copy.
        :type num_processes: int or None
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
        Returns a new collection holding only orders that match both filters.

        An order is kept if any of its source paths matches ``path_glob`` and its
        primary variable matches ``var_glob``.

        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        filtered_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            path_matched = any(_matches_any(str(path), path_glob) for path in order_dict["hf_paths"])

            if path_matched and _matches_any(order_dict["primary_var"], var_glob):
                filtered_orders.append(order_dict)
        logger.debug(f"Inclusive filter(s) applied: '{var_glob}' to history files matching '{path_glob}'")
        return self.copy(ts_orders=filtered_orders)

    def exclude(self, path_glob, var_glob="*"):
        """
        Returns a new collection with orders matching both filters removed.

        An order is dropped if any of its source paths matches ``path_glob`` and
        its primary variable matches ``var_glob``. Both must match, so the
        one-argument form drops every order under ``path_glob``.

        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        filtered_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            path_matched = any(_matches_any(str(path), path_glob) for path in order_dict["hf_paths"])

            if not (path_matched and _matches_any(order_dict["primary_var"], var_glob)):
                filtered_orders.append(order_dict)
        logger.debug(f"Exclusive filter(s) applied: '{var_glob}' to history files matching '{path_glob}'")
        return self.copy(ts_orders=filtered_orders)

    def add_args(self, path_glob="*", var_glob="*", level=None, alg=None, overwrite=None, chunk_target_bytes=None):
        """
        Sets generation arguments on orders that match both filters.

        Arguments left ``None`` are not applied. The other ``apply_*`` methods
        are thin wrappers around this one.

        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :param level: netCDF4 compression level (0-9).
        :type level: int or None
        :param alg: netCDF4 compression algorithm, e.g. ``'zlib'``.
        :type alg: str or None
        :param overwrite: Whether matching outputs are overwritten.
        :type overwrite: bool or None
        :param chunk_target_bytes: Chunk size target for
            :func:`write_timeseries_file`.
        :type chunk_target_bytes: int or None
        :rtype: TSCollection
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            path_matched = any(_matches_any(str(path), path_glob) for path in order_dict["hf_paths"])

            if path_matched and _matches_any(order_dict["primary_var"], var_glob):
                if level is not None:
                    order_dict["complevel"] = level
                if alg is not None:
                    order_dict["compression"] = alg
                if overwrite is not None:
                    order_dict["overwrite"] = overwrite
                if chunk_target_bytes is not None:
                    order_dict["chunk_target_bytes"] = chunk_target_bytes
            new_orders.append(order_dict)

        logger.debug(f"Arguments applied (excluding None): ['level': {level}, 'alg': {alg}, 'overwrite': {overwrite}, 'chunk_target_bytes': {chunk_target_bytes}] to history files matching '{path_glob}' and variables matching '{var_glob}'.")
        return self.copy(ts_orders=new_orders)

    def apply_path_swap(self, string_match, string_swap, path_glob="*", var_glob="*"):
        """
        Replaces a substring in the output path template of matching orders.

        Used to redirect output into a different directory structure, e.g.
        ``'/hist/'`` to ``'/proc/tseries/'``.

        :param string_match: Substring to find in the output path template.
        :type string_match: str
        :param string_swap: Replacement string.
        :type string_swap: str
        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            if any(_matches_any(str(path), path_glob) for path in order_dict["hf_paths"]):
                order_dict["ts_path_template"] = order_dict["ts_path_template"].replace(string_match, string_swap)
            new_orders.append(order_dict)
    
        logger.debug(f"Path swap '{string_match}' -> '{string_swap}' to history files matching '{path_glob}' and variables matching '{var_glob}'.")
        return self.copy(ts_orders=new_orders)
        
    def apply_compression(self, level, alg, path_glob, var_glob="*"):
        """
        Applies compression settings to matching orders (see :meth:`add_args`).

        :param level: netCDF4 compression level (0-9).
        :type level: int
        :param alg: netCDF4 compression algorithm, e.g. ``'zlib'``.
        :type alg: str
        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, level=level, alg=alg)

    def apply_chunk_target_bytes(self, target_bytes, path_glob="*", var_glob="*"):
        """
        Sets the chunk size target on matching orders (see :meth:`add_args`).

        Output written with a non-default target will not pass
        :func:`check_timeseries_conform`, which always checks against
        :data:`CHUNK_TARGET_BYTES`.

        :param target_bytes: Target chunk size in bytes.
        :type target_bytes: int
        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, chunk_target_bytes=target_bytes)

    def apply_overwrite(self, path_glob, var_glob="*"):
        """
        Enables overwriting of existing output for matching orders.

        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=True)

    def append_timestep_dirs(self, var_glob="*"):
        """
        Inserts a frequency directory (``hour_6``, ``month_1``, ...) before the
        filename of each matching order, organising output by frequency.

        Orders that do not match ``var_glob`` are dropped, not just left alone.

        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            if _matches_any(order_dict["primary_var"], var_glob):
                dt = self.__hf_collection.get_timestep_delta(order_dict["hf_paths"][0])
                timestep_label = get_timestep_label(dt)

                template = Path(order_dict["ts_path_template"])
                order_dict["ts_path_template"] = str(template.parent) + f"/{timestep_label}/" + template.name

                new_orders.append(order_dict)
        return self.copy(ts_orders=new_orders)

    def remove_overwrite(self, path_glob, var_glob="*"):
        """
        Disables overwriting of existing output for matching orders.

        :param path_glob: One or more ``fnmatch`` globs applied to source history
            file paths; a single string is also accepted.
        :type path_glob: list[str] or str
        :param var_glob: One or more ``fnmatch`` globs applied to primary variable
            names; a single string is also accepted.
        :type var_glob: list[str] or str
        :rtype: TSCollection
        """
        return self.add_args(path_glob=path_glob, var_glob=var_glob, overwrite=False)

    def create_directories(self, exist_ok=True):
        """
        Creates the output directory tree for every order.

        :param exist_ok: Do not raise when a directory already exists.
        :type exist_ok: bool
        """
        logger.info("Creating directory structure for time series output.")
        for order_dict in self.__orders:
            makedirs(Path(order_dict['ts_path_template']).parent, exist_ok=exist_ok)

    def execute(self, optimize=True, optimize_batch_n=200, raise_errors=False, no_data=False, show_progress=True, memory_limit_bytes=DEFAULT_MEMORY_LIMIT_BYTES):
        """
        Runs every order, writing the time series files.

        Orders sharing a first source file and time slice are batched together so
        each group of history files is opened once rather than once per variable.
        Work runs over a process pool when ``num_processes > 1`` and in-process
        otherwise; per-order failures are logged and the rest of the run continues.

        :param optimize: Batch orders sharing source files into single worker
            calls, rather than submitting one call per order.
        :type optimize: bool
        :param optimize_batch_n: Maximum number of variables per batch.
        :type optimize_batch_n: int
        :param raise_errors: Raise order failures instead of logging them.
        :type raise_errors: bool
        :param no_data: Skip reading and writing primary variable data, producing
            the full structure with primaries reading back as their fill value
            (see :func:`write_timeseries_file`).
        :type no_data: bool
        :param show_progress: If ``False``, suppress the stdout progress bar.
        :type show_progress: bool
        :param memory_limit_bytes: Cache ceiling for each
            :class:`~gents.mhfdataset.MHFDataset` opened, per worker.
        :type memory_limit_bytes: float
        :returns: Paths to every generated time series file.
        :rtype: list[str]
        """
        self.create_directories()
        results = []

        optimized_orders = []
        if optimize:
            order_index_merge_map = {}
            for index, order in enumerate(self.__orders):
                first_hf_path = order["hf_paths"][0]
                start_index = order["ts_start_index"]
                end_index = order["ts_end_index"]
                key = f"{first_hf_path}.{start_index}.{end_index}"
                if key in order_index_merge_map:
                    order_index_merge_map[key].append(index)
                else:
                    order_index_merge_map[key] = [index]
            
            batched_index_lists = []
            for key in order_index_merge_map:
                indices = order_index_merge_map[key]
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
                    "ts_args": ts_args,
                    "no_data": no_data,
                    "memory_limit_bytes": memory_limit_bytes
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
                    "ts_args": ts_args,
                    "no_data": no_data,
                    "memory_limit_bytes": memory_limit_bytes
                })
        prog_bar = ProgressBar(total=len(optimized_orders), label="Generating Timeseries", quiet=not show_progress)
        if self.__num_processes > 1:
            with ProcessPoolExecutor(max_workers=self.__num_processes) as executor:
                futures = {executor.submit(generate_time_series, **args): args for args in optimized_orders}
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        order = futures[future]
                        logger.warning(f"Failed to generate time series for {order['ts_path_template']}: {exc}", exc_info=True)
                        if raise_errors:
                            raise
                    finally:
                        prog_bar.step()
        else:
            for args in optimized_orders:
                try:
                    results.append(generate_time_series(**args))
                except Exception as exc:
                    logger.warning(f"Failed to generate time series for {args['ts_path_template']}: {exc}", exc_info=True)
                    if raise_errors:
                        raise
                finally:
                    prog_bar.step()
        
        output_paths = []
        for result in results:
            for path in result:
                output_paths.append(path)

        return output_paths

    def add_attrs(self, attrs):
        """
        Adds global attributes to every output file of this collection.

        :param attrs: Attributes to stamp into the output files.
        :type attrs: dict
        :rtype: TSCollection
        """
        new_orders = []
        for order_dict in copy.deepcopy(self.__orders):
            order_dict["append_attrs"] = attrs
            new_orders.append(order_dict)
        return self.copy(ts_orders=new_orders)