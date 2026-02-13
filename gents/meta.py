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
from gents.utils import LOG_LEVEL_IO_WARNING
import logging

logger = logging.getLogger(__name__)

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


def get_attributes(dataset):
    """
    Builds Python dictionary of attributes from netCDF4 dataset and variable classes
    
    :param dataset: netCDF4 dataset or variable class object
    :return: Dictionary containing attributes.
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
    """Stores and provides interface for accessing necessary metadata for individual history files."""
    def __init__(self, ds: netCDF4.Dataset):
        """
        :param ds: netCDF dataset read of history file (not this is not the path).
        """
        self.__time_vals = None
        self.__cftime_vals = None

        time_eqv, time_bnds_eqv = get_time_variables_names(ds)
        
        if time_eqv is None:
            raise ValueError(f"No equivalent time variable found to concatenate over.")

        try:
            self.__time_vals = ds[time_eqv][:]

            if len(self.__time_vals.shape) > 1:
                self.__time_vals = np.squeeze(self.__time_vals)
            elif len(self.__time_vals.shape) == 0:
                self.__time_vals = np.array([self.__time_vals])

            self.__cftime_vals = num2date(self.__time_vals, units=ds[time_eqv].units, calendar=ds[time_eqv].calendar)
        except AttributeError:
            logger.log(LOG_LEVEL_IO_WARNING, f"Unable to pull 'calendar' and/or 'units' attributes from 'time' variable.")

        self.__time_bounds_vals = None
        self.__cftime_bounds_vals = None
        
        if time_bnds_eqv:
            self.__time_bounds_vals = ds[time_bnds_eqv][:]

            if len(self.__time_bounds_vals.shape) > 2:
                self.__time_bounds_vals = np.squeeze(self.__time_bounds_vals)
            elif len(self.__time_bounds_vals.shape) == 1:
                self.__time_bounds_vals = np.array([self.__time_bounds_vals])
            elif len(self.__time_bounds_vals.shape) == 0:
                raise ValueError(f"Found a 'time_bounds' equivalent variable, but it was a single value. It must have two values (one for each boundary).")

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
                
        self.__attrs = get_attributes(ds)
        self.__path = ds.filepath()

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
        :return: Get path to history file represented by this metadata class.
        """
        return self.__path
    
    def get_cftime_bounds(self):
        """
        :return: Get time bounds variable as CFTime objects.
        """
        return self.__cftime_bounds_vals

    def get_float_time_bounds(self):
        """
        :return: Get time bounds variable as floats.
        """
        return self.__time_bounds_vals

    def get_float_times(self):
        """
        :return: Get array of floats from time dimension.
        """
        return self.__time_vals

    def get_cftimes(self):
        """
        :return: Get array of CFTime objects from time dimension.
        """
        return self.__cftime_vals

    def get_variables(self):
        """
        :return: Get array of CFTime objects
        """
        return self.__var_names

    def get_primary_variables(self):
        """
        :return: Get primary variables derived from this history file.
        """
        return self.__primary_var_names

    def get_secondary_variables(self):
        """
        :return: Get secondary variables derived from this history file.
        """
        return self.__secondary_var_names

    def get_attributes(self):
        """
        :return: Get all attributes from this history file.
        """
        return self.__attrs

    def is_valid(self):
        """
        :return: Whether or not all necessary information is available for GenTS to create a time series.
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
        :return: Dictionary containing the bounds for each dimension coordinate variable.
        """
        return self.__dim_bounds

def get_meta_from_path(path: str):
    """
    The netCDFMeta class only accepts netCDF4.Dataset objects as input to manage I/O errors.
    This function serves as a wrapper to obtain metadata.
    
    :param path: Path to netCDF file.
    :return: netCDF4 Dataset read of the history file.
    """
    ds_meta = None
    with netCDF4.Dataset(path, 'r') as ds:
        ds_meta = netCDFMeta(ds)

    return ds_meta