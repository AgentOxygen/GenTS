#!/usr/bin/env python
"""
Per-file netCDF metadata: variable classification and cached header contents.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
import numpy as np
from cftime import num2date
from gents.datastore import GenTSDataStore


def is_var_secondary(variable,
                     secondary_vars: list = ["time_bnds", "time_bnd", "time_bounds", "time_bound"],
                     secondary_dims: list = ["nbnd", "chars", "string_length", "hist_interval"],
                     max_num_dims: int = 1,
                     primary_dims: list = ["time"]) -> bool:
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
    :type secondary_vars: list
    :param secondary_dims: Dimension names whose presence makes a variable secondary.
    :type secondary_dims: list
    :param max_num_dims: Dimension count at or below which a variable is secondary.
    :type max_num_dims: int
    :param primary_dims: Dimension names whose presence keeps a variable primary.
    :type primary_dims: list
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
        self.__time_vals = ds[time_eqv][:]

        if len(self.__time_vals.shape) > 1:
            self.__time_vals = np.squeeze(self.__time_vals)
        elif len(self.__time_vals.shape) == 0:
            self.__time_vals = np.array([self.__time_vals])

        if 'calendar' not in ds[time_eqv].ncattrs() or 'units' not in ds[time_eqv].ncattrs():
            raise AttributeError(f"Unable to pull 'calendar' and/or 'units' attributes from '{time_eqv}' time-equivalent variable. Path: {self.__path}")

        self.__time_units = ds[time_eqv].units
        self.__time_calendar = ds[time_eqv].calendar

        self.__time_bounds_vals = None
        self.__cftime_bounds_vals = None
        self.__time_bounds_units = None
        self.__time_bounds_calendar = None

        if time_bnds_eqv and load_time_bounds:
            self.__time_bounds_vals = ds[time_bnds_eqv][:]

            if len(self.__time_bounds_vals.shape) > 2:
                self.__time_bounds_vals = np.squeeze(self.__time_bounds_vals)
            elif len(self.__time_bounds_vals.shape) == 1:
                self.__time_bounds_vals = np.array([self.__time_bounds_vals])
            elif len(self.__time_bounds_vals.shape) == 0:
                raise ValueError(f"Found a 'time_bounds' equivalent variable, but it was a single value. It must have two values (one for each boundary). Path: {self.__path}")

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
        if self.get_cftime_bounds() is None and self.get_cftimes() is None:
            return False
        elif len(self.get_primary_variables()) + len(self.get_secondary_variables()) == 0:
            return False
        elif "gents_version" in self.get_attributes():
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


def get_meta_from_path(path: str):
    """
    Opens a netCDF file and returns a :class:`netCDFMeta` built from it.

    Picklable factory, so metadata can be read inside ``ProcessPoolExecutor``
    workers.

    :param path: Path to the netCDF history file.
    :type path: str
    :returns: Metadata object populated from the file.
    :rtype: netCDFMeta
    :raises Exception: Re-raises construction errors with the path appended.
    """
    ds_meta = None
    try:
        with GenTSDataStore(path, 'r') as ds:
            ds_meta = netCDFMeta(ds, path)
    except Exception as e:
        raise type(e)(f"{e} Path: {path}") from e

    return ds_meta