#!/usr/bin/env python
"""
Per-file netCDF metadata: variable classification, cached header contents, and bulk header reads.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
import logging
from collections.abc import Sequence
from types import SimpleNamespace
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from cftime import num2date

from gents.datastore import GenTSDataStore
from gents.utils import ProgressBar

logger = logging.getLogger(__name__)


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

    def to_dict(self):
        """
        Returns the minimal state needed to rebuild this object, as JSON-serialisable data.

        Cached CFTime values and the primary/secondary split are left out: both
        are recomputed by :meth:`from_dict`.

        :rtype: dict
        """
        return {
            "path": str(self.__path),
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
                    "dtype": "str" if self.__variable_dtypes[name] is str else np.dtype(self.__variable_dtypes[name]).str,
                    "attrs": _encode(self.__variable_attrs[name]) if self.__load_variable_attrs else None,
                }
                for name in self.__var_names
            },
            "dim_bounds": _encode(self.__dim_bounds) if self.__compute_dim_bounds else None,
        }

    @classmethod
    def from_dict(cls, state):
        """
        Rebuilds a ``netCDFMeta`` from :meth:`to_dict` output without opening a file.

        Dates are left undecoded, as with ``decode_dates=False``.

        :param state: Dictionary produced by :meth:`to_dict`, possibly via JSON.
        :type state: dict
        :rtype: netCDFMeta
        """
        meta = cls.__new__(cls)
        meta.__path = state["path"]
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

        variables = state["variables"]
        meta.__load_variable_attrs = all(var["attrs"] is not None for var in variables.values())
        meta.__compute_dim_bounds = state["dim_bounds"] is not None
        meta.__dim_bounds = _decode(state["dim_bounds"]) if meta.__compute_dim_bounds else {}

        meta.__var_names = list(variables)
        meta.__primary_var_names = []
        meta.__secondary_var_names = []
        meta.__variable_shapes = {}
        meta.__variable_dims = {}
        meta.__variable_dtypes = {}
        meta.__variable_attrs = {}
        for name, var in variables.items():
            dims = tuple(var["dims"])
            if is_var_secondary(SimpleNamespace(name=name, dimensions=dims)):
                meta.__secondary_var_names.append(name)
            else:
                meta.__primary_var_names.append(name)
            meta.__variable_shapes[name] = tuple(var["shape"])
            meta.__variable_dims[name] = dims
            meta.__variable_dtypes[name] = str if var["dtype"] == "str" else np.dtype(var["dtype"])
            if meta.__load_variable_attrs:
                meta.__variable_attrs[name] = _decode(var["attrs"])
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
