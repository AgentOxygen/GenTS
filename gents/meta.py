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


class netCDFMeta:
    """Stores and provides interface for accessing necessary metadata for individual history files."""
    def __init__(self, ds: netCDF4.Dataset):
        """
        :param ds: netCDF dataset read of history file (not this is not the path).
        """
        try:
            if 'time' in ds.variables:
                self.__time_vals = num2date(ds['time'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            else:
                self.__time_vals = None
        except AttributeError:
            self.__time_vals = None

        self.__time_bounds_vals = None
        try:
            if 'time_bnds' in ds.variables:
                self.__time_bounds_vals = num2date(ds['time_bnds'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bnd' in ds.variables:
                self.__time_bounds_vals = num2date(ds['time_bnd'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bounds' in ds.variables:
                self.__time_bounds_vals = num2date(ds['time_bounds'][:], units=ds["time"].units, calendar=ds["time"].calendar)
            elif 'time_bound' in ds.variables:
                self.__time_bounds_vals = num2date(ds['time_bound'][:], units=ds["time"].units, calendar=ds["time"].calendar)
        except AttributeError:
            self.__time_bounds_vals = None

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

    def get_path(self):
        """
        :return: Get path to history file represented by this metadata class.
        """
        return self.__path
    
    def get_cftime_bounds(self):
        """
        :return: Get time bounds variable as CFTime objects.
        """
        return self.__time_bounds_vals

    def get_cftimes(self):
        """
        :return: Get array of CFTime objects from time dimension.
        """
        return self.__time_vals

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
        if self.get_cftime_bounds() is None:
            return False
        elif len(self.get_primary_variables()) == 0:
            return False
        elif "gents_version" in self.get_attributes():
            return False
        return True

def get_meta_from_path(path: str):
    """
    The netCDFMeta class only accepts netCDF4.Dataset objects as input to manage I/O errors.
    This function serves as a wrapper to obtain metadata.
    
    :param path: Path to netCDF file.
    :return: netCDF4 Dataset read of the history file.
    """
    ds_meta = None
    with netCDF4.Dataset(path, 'r') as ds:
        if "time" in ds.variables:
            ds_meta = netCDFMeta(ds)

    return ds_meta