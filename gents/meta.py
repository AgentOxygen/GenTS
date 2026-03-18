#!/usr/bin/env python
"""
meta.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 07/03/25
"""
import netCDF4
import numpy as np
from cftime import num2date


def is_var_secondary(variable: netCDF4._netCDF4._Variable,
                     secondary_vars: list = ["time_bnds", "time_bnd", "time_bounds", "time_bound"],
                     secondary_dims: list = ["nbnd", "chars", "string_length", "hist_interval"],
                     max_num_dims: int = 1,
                     primary_dims: list = ["time"]) -> bool:
    """
    Classifies a netCDF variable as secondary or primary.

    Secondary variables (e.g. coordinate and auxiliary fields such as ``time``,
    ``time_bnds``, ``lat``, ``lon``) are written unchanged into every time-series
    output file.  Primary variables (multi-dimensional, time-varying scientific
    fields) each warrant their own time-series output file.

    Rules are evaluated in order:

    1. Variable name is in ``secondary_vars`` → secondary.
    2. Any dimension name is in ``secondary_dims`` → secondary.
    3. Variable has more than ``max_num_dims`` dimensions and none are in
       ``primary_dims`` → secondary.
    4. Otherwise the variable is primary (has a ``time`` dimension and more
       than one dimension total).

    :param variable: netCDF4 variable object to classify.
    :type variable: netCDF4._netCDF4.Variable
    :param secondary_vars: Variable names that are unconditionally secondary.
        Defaults to ``['time_bnds', 'time_bnd', 'time_bounds', 'time_bound']``.
    :type secondary_vars: list
    :param secondary_dims: Dimension names whose presence makes a variable secondary.
        Defaults to ``['nbnd', 'chars', 'string_length', 'hist_interval']``.
    :type secondary_dims: list
    :param max_num_dims: Maximum number of dimensions a variable may have before
        the ``primary_dims`` check is applied. Defaults to ``1``.
    :type max_num_dims: int
    :param primary_dims: Dimension names whose presence keeps a variable primary.
        Defaults to ``['time']``.
    :type primary_dims: list
    :returns: ``True`` if the variable is secondary, ``False`` if primary.
    :rtype: bool
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


def get_attributes(dataset):
    """
    Extracts all attributes from a netCDF4 dataset or variable into a dictionary.

    Handles the slight API difference between ``MFDataset`` objects (which require
    ``__getattribute__``) and standard ``Dataset`` or ``Variable`` objects (which
    use ``__getattr__``).

    :param dataset: A ``netCDF4.Dataset``, ``netCDF4.MFDataset``, or
        ``netCDF4.Variable`` object from which to read attributes.
    :returns: Dictionary mapping attribute names to their values.
    :rtype: dict
    """
    attrs = {}
    if type(dataset) is netCDF4._netCDF4.MFDataset:
        for key in dataset.ncattrs():
            attrs[key] = dataset.__getattribute__(key)
    else:
        for key in dataset.ncattrs():
            attrs[key] = dataset.__getattr__(key)
    return attrs


def get_time_variables_names(ds):
    """
    Locates the time and time-bounds variable names in a netCDF dataset.

    Performs a case-insensitive scan of all variable names and returns the
    canonical name of the ``time`` variable and, if present, the name of the
    corresponding time-bounds variable (``time_bnds``, ``time_bnd``,
    ``time_bounds``, or ``time_bound``).

    :param ds: Open netCDF4 dataset to inspect.
    :type ds: netCDF4.Dataset
    :returns: Tuple of ``(time_name, time_bounds_name)``. Either element is
        ``None`` if the corresponding variable is not found.
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
    Stores metadata extracted from a single netCDF history file.

    Caches time values (as raw floats and as CFTime objects), optional
    time-bounds values, global file attributes, variable lists partitioned into
    primary vs. secondary sets, and per-dimension coordinate bounds.  Instances
    are constructed by :func:`get_meta_from_path` and consumed throughout
    :mod:`gents.hfcollection`.
    """

    def __init__(self, ds: netCDF4.Dataset, path: str):
        """
        Reads and caches metadata from an open netCDF4 dataset.

        Performs the following steps:

        1. Reads global attributes via :func:`get_attributes`.
        2. Locates the time and time-bounds variables via
           :func:`get_time_variables_names`.
        3. Reads and normalises time values (handles scalar, 1-D, and
           higher-dimensional arrays via ``numpy.squeeze``).
        4. Converts float times to CFTime objects via ``cftime.num2date``.
        5. If a time-bounds variable exists, reads and converts it (falling back
           to the time variable's units/calendar if the bounds variable lacks them).
        6. Classifies every variable as primary or secondary via
           :func:`is_var_secondary`.
        7. Records coordinate bounds for each dimension that has an associated
           coordinate variable.

        :param ds: Open netCDF4 dataset for the history file.
        :type ds: netCDF4.Dataset
        :param path: File-system path to the history file (stored for later retrieval).
        :type path: str
        :raises ValueError: If no time-equivalent variable is found, or if the
            time-bounds variable is a scalar.
        :raises AttributeError: If the time variable lacks ``units`` or ``calendar``
            attributes.
        """
        self.__time_vals = None
        self.__cftime_vals = None

        self.__attrs = get_attributes(ds)
        self.__path = path

        time_eqv, time_bnds_eqv = get_time_variables_names(ds)
        
        if time_eqv is None:
            raise ValueError(f"No equivalent time variable found to concatenate over. Path: {self.__path}")

        self.__time_vals = ds[time_eqv][:]

        if len(self.__time_vals.shape) > 1:
            self.__time_vals = np.squeeze(self.__time_vals)
        elif len(self.__time_vals.shape) == 0:
            self.__time_vals = np.array([self.__time_vals])

        if 'calendar' not in ds[time_eqv].ncattrs() or 'units' not in ds[time_eqv].ncattrs():
            raise AttributeError(f"Unable to pull 'calendar' and/or 'units' attributes from '{time_eqv}' time-equivalent variable. Path: {self.__path}")

        self.__cftime_vals = num2date(self.__time_vals, units=ds[time_eqv].units, calendar=ds[time_eqv].calendar)

        self.__time_bounds_vals = None
        self.__cftime_bounds_vals = None
        
        if time_bnds_eqv:
            self.__time_bounds_vals = ds[time_bnds_eqv][:]

            if len(self.__time_bounds_vals.shape) > 2:
                self.__time_bounds_vals = np.squeeze(self.__time_bounds_vals)
            elif len(self.__time_bounds_vals.shape) == 1:
                self.__time_bounds_vals = np.array([self.__time_bounds_vals])
            elif len(self.__time_bounds_vals.shape) == 0:
                raise ValueError(f"Found a 'time_bounds' equivalent variable, but it was a single value. It must have two values (one for each boundary). Path: {self.__path}")

            try:
                self.__cftime_bounds_vals = num2date(self.__time_bounds_vals, units=ds[time_bnds_eqv].units, calendar=ds[time_bnds_eqv].calendar)
            except AttributeError:
                self.__cftime_bounds_vals = num2date(self.__time_bounds_vals, units=ds[time_eqv].units, calendar=ds[time_eqv].calendar)
        self.__var_names = list(ds.variables)
        self.__primary_var_names = []
        self.__secondary_var_names = []
        
        for variable in ds.variables:
            if is_var_secondary(ds[variable]):
                self.__secondary_var_names.append(variable)
            else:
                self.__primary_var_names.append(variable)

        self.__dim_bounds = {}

        for dim_variable in ds.dimensions:
            if dim_variable in ds.variables:
                dim_data = ds[dim_variable][:]
                if dim_data.shape[0] >= 2:
                    self.__dim_bounds[dim_variable] = [np.min(dim_data), np.max(dim_data)]
                else:
                    self.__dim_bounds[dim_variable] = [np.min(dim_data)]

    def get_path(self):
        """
        Returns the file-system path of the history file this object was built from.

        :returns: Path to the source history file.
        :rtype: str
        """
        return self.__path
    
    def get_cftime_bounds(self):
        """
        Returns the time-bounds array as CFTime objects.

        :returns: Array of CFTime bound pairs, or ``None`` if the history file
            contains no time-bounds variable.
        :rtype: numpy.ndarray or None
        """
        return self.__cftime_bounds_vals

    def get_float_time_bounds(self):
        """
        Returns the time-bounds array as raw float values.

        :returns: Array of float time-bound pairs, or ``None`` if the history file
            contains no time-bounds variable.
        :rtype: numpy.ndarray or None
        """
        return self.__time_bounds_vals

    def get_float_times(self):
        """
        Returns the raw float time values read from the ``time`` variable.

        :returns: 1-D array of float time values.
        :rtype: numpy.ndarray
        """
        return self.__time_vals

    def get_cftimes(self):
        """
        Returns the time values converted to CFTime objects.

        :returns: Array of CFTime datetime objects corresponding to each time step.
        :rtype: numpy.ndarray
        """
        return self.__cftime_vals

    def get_variables(self):
        """
        Returns the full list of variable names present in the history file.

        :returns: List of all variable name strings.
        :rtype: list
        """
        return self.__var_names

    def get_primary_variables(self):
        """
        Returns the names of primary variables in the history file.

        Primary variables are multi-dimensional, time-varying scientific fields
        that each warrant their own time-series output file.

        :returns: List of primary variable name strings.
        :rtype: list
        """
        return self.__primary_var_names

    def get_secondary_variables(self):
        """
        Returns the names of secondary variables in the history file.

        Secondary variables are coordinate and auxiliary fields (e.g. ``time``,
        ``time_bnds``, ``lat``, ``lon``) that are written unchanged into every
        time-series output file.

        :returns: List of secondary variable name strings.
        :rtype: list
        """
        return self.__secondary_var_names

    def get_attributes(self):
        """
        Returns the global attributes dictionary cached from the history file.

        :returns: Dictionary mapping global attribute names to their values.
        :rtype: dict
        """
        return self.__attrs

    def is_valid(self):
        """
        Returns whether this history file is usable for time-series generation.

        A file is considered invalid if any of the following are true:

        - Both ``get_cftime_bounds()`` and ``get_cftimes()`` are ``None``
          (no usable time coordinate).
        - The file contains zero primary and zero secondary variables.
        - A ``gents_version`` global attribute is present (the file is already
          a GenTS-generated time-series output, not a raw history file).

        :returns: ``True`` if the file is valid for processing, ``False`` otherwise.
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
        Returns coordinate bounds for each dimension in the history file.

        For each dimension that has an associated coordinate variable, maps the
        dimension name to a list containing its minimum value (single-element list
        for a scalar coordinate) or ``[min_value, max_value]`` for a range.  Used
        by :func:`~gents.hfcollection.merge_fragmented_groups` to identify spatial
        extent when merging tiled files.

        :returns: Dictionary mapping dimension names to their coordinate bound lists.
        :rtype: dict
        """
        return self.__dim_bounds

def get_meta_from_path(path: str):
    """
    Opens a netCDF file, constructs a :class:`netCDFMeta` object, and returns it.

    Serves as a picklable factory wrapper around :class:`netCDFMeta` so that
    instances can be created inside ``ProcessPoolExecutor`` worker processes.
    Any exception raised during construction is re-raised with the file path
    appended to the message for easier debugging.

    :param path: Path to the netCDF history file.
    :type path: str
    :returns: Metadata object populated from the specified file.
    :rtype: netCDFMeta
    :raises Exception: Re-raises any exception from ``netCDFMeta.__init__``
        with the file path appended to the message.
    """
    ds_meta = None
    try:
        with netCDF4.Dataset(path, 'r') as ds:
            ds_meta = netCDFMeta(ds, path)
    except Exception as e:
        raise type(e)(f"{e} Path: {path}") from e

    return ds_meta