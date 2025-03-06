import netCDF4
from cftime import num2date


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
    def __init__(self, ds: netCDF4.Dataset):
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
        self.__attrs = get_attributes(ds)
        self.__path = ds.filepath()

    def get_path(self):
        return self.__path
    
    def get_cftime_bounds(self):
        return self.__time_bounds_vals

    def get_cftimes(self):
        return self.__time_vals

    def get_variables(self):
        return self.__var_names

    def get_attributes(self):
        return self.__attrs


def get_meta_from_path(path):
    ds_meta = None
    with netCDF4.Dataset(path, 'r') as ds:
        if "time" in ds.variables:
            ds_meta = netCDFMeta(ds)
    return ds_meta