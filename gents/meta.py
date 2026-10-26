#!/usr/bin/env python
"""
Per-file netCDF metadata: variable classification, cached header contents, and bulk header reads.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
import gzip
import json
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from cftime import num2date

from gents.datastore import GenTSDataStore
from gents.utils import ProgressBar

logger = logging.getLogger(__name__)

MANIFEST_VERSION = 2
MANIFEST_COMPRESSION_LEVEL = 3  # manifests are written often and read once; favour speed


def is_var_secondary(variable,
                     secondary_vars: Sequence[str] = ("time_bnds", "time_bnd", "time_bounds", "time_bound"),
                     secondary_dims: Sequence[str] = ("nbnd", "chars", "string_length", "hist_interval"),
                     max_num_dims: int = 1,
                     primary_dims: Sequence[str] = ("time",)) -> bool:
    """
    Classifies a netCDF variable as secondary or primary.

    Primary variables are multi-dimensional, time-varying scientific fields; each
    gets its own time series file. Everything else (coordinates, bounds, metadata)
    is secondary and is copied into every output file of the group.

    A variable is secondary if its name is in ``secondary_vars``, if any of its
    dimensions is in ``secondary_dims``, or if it has at most ``max_num_dims``
    dimensions or none of ``primary_dims``. All comparisons are case-insensitive,
    so MOM6-style ``Time`` / ``Time_Bounds`` are recognised.

    :param variable: netCDF4 variable object to classify.
    :type variable: netCDF4._netCDF4.Variable
    :param secondary_vars: Variable names that are unconditionally secondary.
    :type secondary_vars: Sequence[str]
    :param secondary_dims: Dimension names whose presence makes a variable secondary.
    :type secondary_dims: Sequence[str]
    :param max_num_dims: Dimension count at or below which a variable is secondary.
    :type max_num_dims: int
    :param primary_dims: Dimension names whose presence keeps a variable primary.
    :type primary_dims: Sequence[str]
    :returns: ``True`` if the variable is secondary, ``False`` if primary.
    :rtype: bool
    """
    if variable.name.lower() in {var.lower() for var in secondary_vars}:
        return True

    dims = np.unique(variable.dimensions)
    lowered_dims = {str(dim).lower() for dim in dims}

    if any(tag.lower() in lowered_dims for tag in secondary_dims):
        return True

    if len(dims) > max_num_dims:
        if any(tag.lower() in lowered_dims for tag in primary_dims):
            return False

    return True


def get_attributes(dataset):
    """
    Extracts all attributes from a netCDF4 dataset or variable into a dictionary.

    :param dataset: netCDF4 dataset or variable to read attributes from.
    :type dataset: netCDF4.Dataset or netCDF4._netCDF4.Variable
    :returns: Dictionary mapping attribute names to their values.
    :rtype: dict
    """
    attrs = {}
    for key in dataset.ncattrs():
        attrs[key] = getattr(dataset, key)
    return attrs


def get_time_variables_names(ds):
    """
    Locates the time and time-bounds variable names in a netCDF dataset.

    Matching is case-insensitive against ``time`` and, for bounds, ``time_bnds``,
    ``time_bnd``, ``time_bounds`` or ``time_bound``.

    :param ds: Open netCDF4 dataset to inspect.
    :type ds: netCDF4.Dataset
    :returns: ``(time_name, time_bounds_name)``; either is ``None`` if not found.
    :rtype: tuple[str or None, str or None]
    """
    time_eqv = None
    time_bnds_eqv = None

    for var_name in ds.variables:
        lowercase_name = str(var_name).lower()
        if lowercase_name == 'time':
            time_eqv = var_name
        elif lowercase_name == 'time_bnds':
            time_bnds_eqv = var_name
        elif lowercase_name == 'time_bnd':
            time_bnds_eqv = var_name
        elif lowercase_name == 'time_bounds':
            time_bnds_eqv = var_name
        elif lowercase_name == 'time_bound':
            time_bnds_eqv = var_name

    return time_eqv, time_bnds_eqv


def get_file_stat(path):
    """
    Returns the size and modification time that identify a file's current contents.

    :param path: File to stat.
    :type path: str or pathlib.Path
    :returns: ``{"size": bytes, "mtime_ns": int}``, or ``None`` if the file cannot be stat-ed.
    :rtype: dict or None
    """
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _encode(value):
    """
    Converts an attribute or array value into something :mod:`json` can write.

    ``str``, ``int``, ``float``, ``bool`` and ``None`` pass through so the file
    stays readable; numpy values become ``{"nd": dtype, "v": data}`` (plus
    ``"scalar"`` if 0-d). Values widen exactly to Python numbers and
    :func:`_decode` casts back to ``dtype``, so e.g. a ``float32`` returns as one.
    Masks are dropped. Lists, tuples and dicts are converted element-wise.

    :param value: Value to encode.
    :returns: A JSON-serialisable object.
    """
    if isinstance(value, (np.ndarray, np.generic)):
        encoded = {"nd": np.asarray(value).dtype.str, "v": np.ma.getdata(value).tolist()}
        if isinstance(value, np.generic):
            encoded["scalar"] = True
        return encoded
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        return {key: _encode(item) for key, item in value.items()}
    return value


def _decode(value):
    """
    Inverts :func:`_encode`. Tuples come back as lists, so restoring shapes and
    dimension tuples is the caller's job.

    :param value: A JSON-loaded object.
    :returns: The original value.
    """
    if isinstance(value, dict):
        if "nd" in value:
            array = np.array(value["v"], dtype=value["nd"])
            return array[()] if value.get("scalar") else array
        return {key: _decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item) for item in value]
    return value


def _build_variable_tables(variables):
    """
    Builds the per-variable tables of a :class:`netCDFMeta` from its serialised
    ``variables`` entry: whether attributes were loaded, then the names, primary
    and secondary names, shapes, dims, dtypes and attributes.

    :param variables: The ``"variables"`` value of :meth:`netCDFMeta.to_dict`.
    :type variables: dict
    :rtype: tuple
    """
    load_attrs = all(var["attrs"] is not None for var in variables.values())
    primary, secondary = [], []
    shapes, dims_by_var, dtypes, attrs = {}, {}, {}, {}
    for name, var in variables.items():
        dims = tuple(var["dims"])
        if is_var_secondary(SimpleNamespace(name=name, dimensions=dims)):
            secondary.append(name)
        else:
            primary.append(name)
        shapes[name] = tuple(var["shape"])
        dims_by_var[name] = dims
        dtypes[name] = str if var["dtype"] == "str" else np.dtype(var["dtype"])
        if load_attrs:
            attrs[name] = _decode(var["attrs"])
    return load_attrs, list(variables), primary, secondary, shapes, dims_by_var, dtypes, attrs


class netCDFMeta:
    """
    Metadata read once from a single netCDF history file and cached in memory.

    Picklable, so it can be built in a worker process and returned to the parent
    (see :func:`get_meta_from_path`).
    """

    def __init__(self, ds, path: str, decode_dates=True, load_time_bounds=True,
                 load_variable_attrs=True, compute_dim_bounds=True):
        """
        Reads and caches metadata from an open netCDF4 dataset.

        Caches global attributes, raw time values, optional time bounds, the
        primary/secondary variable split with per-variable shape, dims and dtype,
        and per-dimension coordinate bounds. The file need not stay open afterward.

        The four load flags exist to skip per-file work a caller does not need;
        :class:`~gents.mhfdataset.MHFDataset` needs none of them. Opting out is
        strict: the corresponding getter raises rather than return empty data.

        :param ds: Open netCDF4 dataset for the history file.
        :type ds: netCDF4.Dataset
        :param path: File-system path to the history file.
        :type path: str
        :param decode_dates: Decode CFTime values now instead of lazily on the
            first :meth:`get_cftimes` / :meth:`get_cftime_bounds` call.
        :type decode_dates: bool
        :param load_time_bounds: Read the raw time-bounds array, if present.
        :type load_time_bounds: bool
        :param load_variable_attrs: Read every variable's own attributes.
        :type load_variable_attrs: bool
        :param compute_dim_bounds: Compute per-dimension coordinate bounds.
        :type compute_dim_bounds: bool
        :raises ValueError: If no time variable is found, or the time-bounds
            variable is a scalar.
        :raises AttributeError: If the time variable lacks ``units`` or ``calendar``.
        """
        self.__time_vals = None
        self.__cftime_vals = None
        self.__load_time_bounds = load_time_bounds
        self.__load_variable_attrs = load_variable_attrs
        self.__compute_dim_bounds = compute_dim_bounds

        # Before any data is read, so a file modified mid-read looks stale to the next run.
        self.__stat = get_file_stat(path)
        self.__attrs = get_attributes(ds)
        self.__path = path

        time_eqv, time_bnds_eqv = get_time_variables_names(ds)

        if time_eqv is None:
            raise ValueError(f"No equivalent time variable found to concatenate over. Path: {self.__path}")

        self.__time_eqv = time_eqv
        self.__time_bnds_eqv = time_bnds_eqv
        self.__time_vals = np.ma.getdata(ds[time_eqv][:]).astype(np.float64)

        if len(self.__time_vals.shape) > 1:
            self.__time_vals = np.squeeze(self.__time_vals)
        elif len(self.__time_vals.shape) == 0:
            self.__time_vals = np.array([self.__time_vals])

        if 'calendar' not in ds[time_eqv].ncattrs() or 'units' not in ds[time_eqv].ncattrs():
            raise AttributeError(
                f"Unable to pull 'calendar' and/or 'units' attributes from '{time_eqv}' time-equivalent variable. "
                f"Path: {self.__path}"
            )

        self.__time_units = ds[time_eqv].units
        self.__time_calendar = ds[time_eqv].calendar

        self.__time_bounds_vals = None
        self.__cftime_bounds_vals = None
        self.__time_bounds_units = None
        self.__time_bounds_calendar = None

        if time_bnds_eqv and load_time_bounds:
            self.__time_bounds_vals = np.ma.getdata(ds[time_bnds_eqv][:]).astype(np.float64)

            if len(self.__time_bounds_vals.shape) > 2:
                self.__time_bounds_vals = np.squeeze(self.__time_bounds_vals)
            elif len(self.__time_bounds_vals.shape) == 1:
                self.__time_bounds_vals = np.array([self.__time_bounds_vals])
            elif len(self.__time_bounds_vals.shape) == 0:
                raise ValueError(
                    "Found a 'time_bounds' equivalent variable, but it was a single value. It must have two values "
                    f"(one for each boundary). Path: {self.__path}"
                )

            try:
                self.__time_bounds_units = ds[time_bnds_eqv].units
                self.__time_bounds_calendar = ds[time_bnds_eqv].calendar
            except AttributeError:
                self.__time_bounds_units = self.__time_units
                self.__time_bounds_calendar = self.__time_calendar

        if decode_dates:
            self.__decode_dates()

        self.__var_names = list(ds.variables)
        self.__primary_var_names = []
        self.__secondary_var_names = []
        self.__variable_shapes = {}
        self.__variable_dims = {}
        self.__variable_dtypes = {}
        self.__variable_attrs = {}

        for variable in ds.variables:
            if is_var_secondary(ds[variable]):
                self.__secondary_var_names.append(variable)
            else:
                self.__primary_var_names.append(variable)
            self.__variable_shapes[variable] = ds[variable].shape
            self.__variable_dims[variable] = ds[variable].dimensions
            self.__variable_dtypes[variable] = ds[variable].dtype
            if load_variable_attrs:
                self.__variable_attrs[variable] = get_attributes(ds[variable])

        self.__dim_bounds = {}

        if compute_dim_bounds:
            for dim_variable in ds.dimensions:
                if dim_variable in ds.variables:
                    dim_data = ds[dim_variable][:]
                    if dim_data.shape[0] >= 2:
                        self.__dim_bounds[dim_variable] = [np.min(dim_data), np.max(dim_data)]
                    else:
                        self.__dim_bounds[dim_variable] = [np.min(dim_data)]

    def get_path(self):
        """
        Returns the path of the history file this object was built from.

        :rtype: str
        """
        return self.__path

    def get_time_var_name(self):
        """
        Returns the name of the time variable in the history file.

        :rtype: str
        """
        return self.__time_eqv

    def get_timebnds_var_name(self):
        """
        Returns the name of the time-bounds variable, or ``None`` if there is none.

        :rtype: str or None
        """
        return self.__time_bnds_eqv

    def __decode_dates(self):
        """
        Converts the cached raw time (and bounds) values to CFTime objects.

        Idempotent, and needs only the values cached in ``__init__``, so the
        source file does not have to still be open.
        """
        if self.__cftime_vals is None:
            self.__cftime_vals = num2date(self.__time_vals, units=self.__time_units, calendar=self.__time_calendar)
        if self.__time_bounds_vals is not None and self.__cftime_bounds_vals is None:
            self.__cftime_bounds_vals = num2date(
                self.__time_bounds_vals, units=self.__time_bounds_units, calendar=self.__time_bounds_calendar
            )

    def __check_time_bounds_loaded(self):
        if self.__time_bnds_eqv and not self.__load_time_bounds:
            raise RuntimeError(
                f"Time-bounds data was not loaded for this netCDFMeta (load_time_bounds=False), "
                f"but '{self.__time_bnds_eqv}' exists in the file. Path: {self.__path}"
            )

    def get_cftime_bounds(self):
        """
        Returns the time-bounds array as CFTime pairs, or ``None`` if the file
        has no time-bounds variable.

        :rtype: numpy.ndarray or None
        :raises RuntimeError: If constructed with ``load_time_bounds=False`` and
            the file actually has a time-bounds variable.
        """
        self.__check_time_bounds_loaded()
        if self.__time_bounds_vals is not None and self.__cftime_bounds_vals is None:
            self.__decode_dates()
        return self.__cftime_bounds_vals

    def get_float_time_bounds(self):
        """
        Returns the time-bounds array as raw float pairs, or ``None`` if the file
        has no time-bounds variable.

        :rtype: numpy.ndarray or None
        :raises RuntimeError: If constructed with ``load_time_bounds=False`` and
            the file actually has a time-bounds variable.
        """
        self.__check_time_bounds_loaded()
        return self.__time_bounds_vals

    def get_float_times(self):
        """
        Returns the raw float time values read from the time variable.

        :rtype: numpy.ndarray
        """
        return self.__time_vals

    def get_cftimes(self):
        """
        Returns the time values as CFTime objects, one per time step.

        :rtype: numpy.ndarray
        """
        if self.__cftime_vals is None:
            self.__decode_dates()
        return self.__cftime_vals

    def get_time_units(self):
        """
        Returns the ``units`` attribute of the time variable.

        :rtype: str
        """
        return self.__time_units

    def get_time_calendar(self):
        """
        Returns the ``calendar`` attribute of the time variable.

        :rtype: str
        """
        return self.__time_calendar

    def decode_time_values(self, values):
        """
        Decodes raw time values into CFTime objects using this file's time
        reference (``units`` and ``calendar``).

        :param values: Raw time value(s) expressed in this file's time units.
        :type values: numpy.ndarray or float
        :returns: Decoded CFTime object(s) matching the input's shape.
        :rtype: numpy.ndarray or cftime.datetime
        """
        return num2date(values, units=self.__time_units, calendar=self.__time_calendar)

    def get_time_bounds_units(self):
        """
        Returns the ``units`` attribute of the time-bounds variable (falling back
        to the time variable's at load time), or ``None`` if the file has none.

        :rtype: str or None
        """
        return self.__time_bounds_units

    def get_time_bounds_calendar(self):
        """
        Returns the ``calendar`` attribute of the time-bounds variable (falling
        back to the time variable's at load time), or ``None`` if the file has none.

        :rtype: str or None
        """
        return self.__time_bounds_calendar

    def decode_time_bounds_values(self, values):
        """
        Decodes raw time-bounds values into CFTime objects using the time-bounds
        variable's reference (``units`` and ``calendar``).

        :param values: Raw time-bounds value(s) expressed in the bounds units.
        :type values: numpy.ndarray or float
        :returns: Decoded CFTime object(s) matching the input's shape.
        :rtype: numpy.ndarray or cftime.datetime
        :raises RuntimeError: If the file has no time-bounds variable, or it was
            not loaded (``load_time_bounds=False``).
        """
        if self.__time_bnds_eqv is None:
            raise RuntimeError(
                f"No time-bounds variable exists to take a time reference from. Path: {self.__path}"
            )
        self.__check_time_bounds_loaded()
        return num2date(values, units=self.__time_bounds_units, calendar=self.__time_bounds_calendar)

    def get_variables(self):
        """
        Returns the names of every variable in the history file.

        :rtype: list[str]
        """
        return self.__var_names

    def get_primary_variables(self):
        """
        Returns the names of the primary variables (see :func:`is_var_secondary`).

        :rtype: list[str]
        """
        return self.__primary_var_names

    def get_secondary_variables(self):
        """
        Returns the names of the secondary variables (see :func:`is_var_secondary`).

        :rtype: list[str]
        """
        return self.__secondary_var_names

    def get_variable_dims(self, variable):
        """
        Returns the dimension names of the given variable.

        :param variable: Name of the variable to look up.
        :type variable: str
        :rtype: tuple[str]
        """
        return self.__variable_dims[variable]

    def get_variable_shapes(self, variable):
        """
        Returns the shape of the given variable in this file.

        :param variable: Name of the variable to look up.
        :type variable: str
        :rtype: tuple[int]
        """
        return self.__variable_shapes[variable]

    def get_variable_dtype(self, variable):
        """
        Returns the NumPy dtype of the given variable.

        :param variable: Name of the variable to look up.
        :type variable: str
        :rtype: numpy.dtype
        """
        return self.__variable_dtypes[variable]

    def get_variable_attrs(self, variable):
        """
        Returns the attribute dictionary of the given variable.

        :param variable: Name of the variable to look up.
        :type variable: str
        :rtype: dict
        :raises RuntimeError: If constructed with ``load_variable_attrs=False``.
        """
        if not self.__load_variable_attrs:
            raise RuntimeError(
                f"Variable attributes were not loaded for this netCDFMeta "
                f"(load_variable_attrs=False). Path: {self.__path}"
            )
        return self.__variable_attrs[variable]

    def get_attributes(self):
        """
        Returns the global attributes cached from the history file.

        :rtype: dict
        """
        return self.__attrs

    def is_valid(self):
        """
        Returns whether this history file is usable for time series generation.

        A file is invalid if it has no usable time coordinate, holds no variables,
        or carries a ``gents_version`` attribute (marking it as GenTS output
        rather than raw model output).

        :rtype: bool
        """
        if self.get_float_time_bounds() is None and self.get_float_times() is None:
            return False
        if len(self.get_primary_variables()) + len(self.get_secondary_variables()) == 0:
            return False
        if "gents_version" in self.get_attributes():
            return False
        return True

    def get_dim_bounds(self):
        """
        Returns ``{dimension: [min]}`` or ``{dimension: [min, max]}`` for every
        dimension that has a coordinate variable.

        Used by :func:`~gents.hfcollection.merge_fragmented_groups` to match the
        spatial extent of tiled files.

        :rtype: dict
        :raises RuntimeError: If constructed with ``compute_dim_bounds=False``.
        """
        if not self.__compute_dim_bounds:
            raise RuntimeError(
                f"Dimension bounds were not computed for this netCDFMeta "
                f"(compute_dim_bounds=False). Path: {self.__path}"
            )
        return self.__dim_bounds

    def get_stat(self):
        """
        Returns the file's size and modification time as of when this metadata was
        read, so cached metadata can be matched to the file it describes.

        :returns: ``{"size": bytes, "mtime_ns": int}``, or ``None`` if the file could not be stat-ed.
        :rtype: dict or None
        """
        return self.__stat

    def to_dict(self):
        """
        Returns the minimal state needed to rebuild this object, as JSON-serialisable data.

        Cached CFTime values and the primary/secondary split are left out: both
        are recomputed by :meth:`from_dict`.

        :rtype: dict
        """
        return {
            "path": str(self.__path),
            "stat": self.__stat,
            "attrs": _encode(self.__attrs),
            "time_name": self.__time_eqv,
            "time_vals": self.__time_vals.tolist(),
            "time_units": self.__time_units,
            "time_calendar": self.__time_calendar,
            "time_bnds_name": self.__time_bnds_eqv,
            "time_bnds_vals": None if self.__time_bounds_vals is None else self.__time_bounds_vals.tolist(),
            "time_bnds_units": self.__time_bounds_units,
            "time_bnds_calendar": self.__time_bounds_calendar,
            "load_time_bounds": self.__load_time_bounds,
            "variables": {
                name: {
                    "shape": list(self.__variable_shapes[name]),
                    "dims": list(self.__variable_dims[name]),
                    "dtype": "str" if self.__variable_dtypes[name] is str
                             else np.dtype(self.__variable_dtypes[name]).str,
                    "attrs": _encode(self.__variable_attrs[name]) if self.__load_variable_attrs else None,
                }
                for name in self.__var_names
            },
            "dim_bounds": _encode(self.__dim_bounds) if self.__compute_dim_bounds else None,
        }

    @classmethod
    def from_dict(cls, state, shared=None):
        """
        Rebuilds a ``netCDFMeta`` from :meth:`to_dict` output without opening a file.

        Dates are left undecoded, as with ``decode_dates=False``.

        :param state: Dictionary produced by :meth:`to_dict`, possibly via JSON.
        :type state: dict
        :param shared: Cache for rebuilding many objects whose ``state["variables"]``
            is the same dictionary object, as a manifest's entries are. The variable
            tables are built once and shared by all of them, so treat what the
            variable getters return as read-only. ``None`` builds a private copy.
        :type shared: dict or None
        :rtype: netCDFMeta
        """
        meta = cls.__new__(cls)
        meta.__path = state["path"]
        meta.__stat = state["stat"]
        meta.__attrs = _decode(state["attrs"])
        meta.__time_eqv = state["time_name"]
        meta.__time_vals = np.asarray(state["time_vals"], dtype=np.float64)
        meta.__time_units = state["time_units"]
        meta.__time_calendar = state["time_calendar"]
        meta.__time_bnds_eqv = state["time_bnds_name"]
        meta.__time_bounds_vals = (
            None if state["time_bnds_vals"] is None else np.asarray(state["time_bnds_vals"], dtype=np.float64)
        )
        meta.__time_bounds_units = state["time_bnds_units"]
        meta.__time_bounds_calendar = state["time_bnds_calendar"]
        meta.__load_time_bounds = state["load_time_bounds"]
        meta.__cftime_vals = None
        meta.__cftime_bounds_vals = None
        meta.__compute_dim_bounds = state["dim_bounds"] is not None
        meta.__dim_bounds = _decode(state["dim_bounds"]) if meta.__compute_dim_bounds else {}

        variables = state["variables"]
        if shared is None:
            shared = {}
        if id(variables) not in shared:
            shared[id(variables)] = _build_variable_tables(variables)
        (
            meta.__load_variable_attrs,
            meta.__var_names,
            meta.__primary_var_names,
            meta.__secondary_var_names,
            meta.__variable_shapes,
            meta.__variable_dims,
            meta.__variable_dtypes,
            meta.__variable_attrs,
        ) = shared[id(variables)]
        return meta


def get_meta_from_path(path: str):
    """
    Opens a netCDF file and returns a :class:`netCDFMeta` built from it.

    Picklable factory, so metadata can be read inside ``ProcessPoolExecutor``
    workers. Dates are left undecoded and per-variable attributes unread; the
    pipeline needs neither from collection metadata.

    :param path: Path to the netCDF history file.
    :type path: str
    :returns: Metadata object populated from the file.
    :rtype: netCDFMeta
    :raises Exception: Re-raises construction errors with the path appended.
    """
    ds_meta = None
    try:
        with GenTSDataStore(path, 'r') as ds:
            ds_meta = netCDFMeta(ds, path, decode_dates=False, load_variable_attrs=False)
    except Exception as e:
        raise type(e)(f"{e} Path: {path}") from e

    return ds_meta


def read_meta_map(paths, factory, num_processes=1, raise_errors=False, show_progress=True):
    """
    Reads the metadata of many files, in parallel when ``num_processes > 1``.

    Shared by the collections' ``pull_metadata``. A file that fails to load is
    logged and left out of the result, so the caller's entry for it stays
    unloaded.

    :param paths: Files to read.
    :type paths: list[pathlib.Path]
    :param factory: Picklable callable mapping a path to its metadata, such as
        :func:`get_meta_from_path`.
    :type factory: callable
    :param num_processes: Worker processes; ``1`` reads serially in-process.
    :type num_processes: int
    :param raise_errors: Raise the first per-file failure instead of logging it.
    :type raise_errors: bool
    :param show_progress: If ``False``, suppress the stdout progress bar.
    :type show_progress: bool
    :returns: ``{path: metadata}`` for every file that loaded.
    :rtype: dict
    """
    loaded = {}
    prog_bar = ProgressBar(total=len(paths), label="Pulling Metadata", quiet=not show_progress)
    if num_processes > 1:
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            futures = {executor.submit(factory, path): path for path in paths}
            for future in as_completed(futures):
                path = futures[future]
                try:
                    loaded[path] = future.result()
                except Exception as exc:
                    logger.warning("Failed to load metadata for %s: %s", path, exc, exc_info=True)
                    if raise_errors:
                        raise
                finally:
                    prog_bar.step()
    else:
        for path in paths:
            try:
                loaded[path] = factory(path)
            except Exception as exc:
                logger.warning("Failed to load metadata for %s: %s", path, exc, exc_info=True)
                if raise_errors:
                    raise
            finally:
                prog_bar.step()
    return loaded


def split_valid(meta_map, is_valid_fn):
    """
    Splits a ``{path: metadata}`` map into usable and unusable entries.

    An entry is unusable if its metadata is ``None`` (never loaded, or the read
    failed) or ``is_valid_fn`` rejects it. Warns about each unusable entry.

    :param meta_map: Metadata by path; not modified.
    :type meta_map: dict
    :param is_valid_fn: Predicate taking one metadata object, such as
        :meth:`netCDFMeta.is_valid`.
    :type is_valid_fn: callable
    :returns: ``(kept, removed)`` dicts of ``{path: metadata}``.
    :rtype: tuple[dict, dict]
    """
    kept = {}
    removed = {}
    for path, meta in meta_map.items():
        if meta is not None and is_valid_fn(meta):
            kept[path] = meta
        else:
            removed[path] = meta
            logger.warning("Could not pull valid/complete metadata for '%s'.", path)
    return kept, removed


def save_meta_manifest(meta_map, manifest_path, kind):
    """
    Writes a ``{path: netCDFMeta}`` map to a single gzip-compressed JSON file.

    Each entry carries the file stat taken when its metadata was read (see
    :meth:`netCDFMeta.get_stat`), so :func:`load_meta_manifest` can tell later
    whether the file has changed. Entries that are ``None`` (not pulled) or have no
    stat are skipped. The write goes to a temporary file first, so an interrupted
    run never leaves a truncated manifest.

    To keep the file small, what files share is stored once. Files with the same
    variable table (names, dims, dtypes, shapes) refer to one ``tables`` entry,
    which also holds the global attributes common to all of them; each entry keeps
    only its own differing attributes. The path is the entry's key, not repeated.
    Inspect with ``zcat <manifest> | jq``.

    The manifest is labelled with ``kind`` because the history-file and time-series
    factories keep different metadata (see :func:`load_meta_manifest`).

    :param meta_map: Metadata by path, such as ``dict(hf_collection.items())``.
    :type meta_map: dict
    :param manifest_path: Where to write the manifest.
    :type manifest_path: str or pathlib.Path
    :param kind: Label for what the metadata is, ``"hf"`` or ``"ts"``.
    :type kind: str
    """
    entries = {}
    table_ids = {}  # serialised variable table -> table id
    members = {}  # table id -> entries using it
    tables = {}
    for path, meta in meta_map.items():
        if meta is None:
            continue
        if meta.get_stat() is None:
            logger.warning("Not adding '%s' to the manifest: no file stat was recorded.", path)
            continue
        entry = meta.to_dict()
        del entry["path"]
        variables = entry.pop("variables")
        table_key = json.dumps(variables)
        if table_key not in table_ids:
            table_ids[table_key] = str(len(table_ids))
            tables[table_ids[table_key]] = {"variables": variables}
            members[table_ids[table_key]] = []
        entry["table"] = table_ids[table_key]
        members[entry["table"]].append(entry)
        entries[str(path)] = entry

    for table_id, group in members.items():
        common = {
            key: value for key, value in group[0]["attrs"].items()
            if all(key in other["attrs"] and other["attrs"][key] == value for other in group)
        }
        tables[table_id]["attrs"] = common
        for entry in group:
            entry["attrs"] = {key: value for key, value in entry["attrs"].items() if key not in common}

    tmp_path = f"{manifest_path}.tmp"
    with gzip.open(tmp_path, "wt", encoding="utf-8", compresslevel=MANIFEST_COMPRESSION_LEVEL) as manifest_file:
        json.dump({"version": MANIFEST_VERSION, "kind": kind, "tables": tables, "entries": entries}, manifest_file)
    os.replace(tmp_path, manifest_path)
    logger.info("Wrote %s entries to manifest '%s'.", len(entries), manifest_path)


def load_meta_manifest(manifest_path, kind):
    """
    Reads a manifest written by :func:`save_meta_manifest`, keeping only entries
    that are still current.

    An entry is current if its file still exists with the same size and
    modification time as when its metadata was read. A missing, unreadable (including
    not gzip) or wrong-version manifest is not an error: it gives an empty result, so
    the caller falls back to reading headers.

    A manifest of a different ``kind`` is an error, since the two kinds are not
    interchangeable and silently replacing one would hide a misconfigured path.

    Metadata from files with the same variable table shares its variable tables, so
    treat what the variable getters return as read-only.

    :param manifest_path: Manifest file to read.
    :type manifest_path: str or pathlib.Path
    :param kind: The kind the caller needs, ``"hf"`` or ``"ts"``.
    :type kind: str
    :raises ValueError: If the manifest was written with a different (or no) kind.
    :returns: ``{path: netCDFMeta}`` for each current entry.
    :rtype: dict[pathlib.Path, netCDFMeta]
    """
    try:
        with gzip.open(manifest_path, "rt", encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        if manifest["version"] != MANIFEST_VERSION:
            logger.info("Ignoring manifest '%s': version %s is not %s.", manifest_path, manifest["version"],
                        MANIFEST_VERSION)
            return {}
        manifest_kind = manifest.get("kind")
        tables = manifest["tables"]
        entries = manifest["entries"]
    except (OSError, EOFError, ValueError, KeyError, TypeError, AttributeError) as exc:
        logger.info("No usable manifest at '%s': %s", manifest_path, exc)
        return {}

    if manifest_kind != kind:
        raise ValueError(
            f"Manifest '{manifest_path}' holds '{manifest_kind}' metadata but '{kind}' was requested. "
            "Use a separate manifest file for each."
        )

    current = {}
    shared = {}
    for path, entry in entries.items():
        try:
            if entry["stat"] is not None and entry["stat"] == get_file_stat(path):
                table = tables[entry["table"]]
                state = {**entry, "path": path, "variables": table["variables"],
                         "attrs": {**table["attrs"], **entry["attrs"]}}
                current[Path(path)] = netCDFMeta.from_dict(state, shared)
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Skipping manifest entry for '%s': %s", path, exc)
    return current
