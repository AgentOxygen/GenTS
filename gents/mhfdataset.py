from netCDF4 import Dataset
from pathlib import Path
from gents.meta import get_attributes, get_time_variables_names
from cftime import num2date
import numpy as np


def get_concat_coords(hf_datasets):
    dim_coords = {}

    for ds in hf_datasets:
        for dim in ds.dimensions:
            if dim in ds.variables and dim in dim_coords:
                dim_coords[dim] = np.unique(np.concat([ds[dim][:], dim_coords[dim]]))
            elif dim in ds.variables:
                dim_coords[dim] = ds[dim][:]
            else:
                dim_coords[dim] = np.arange(ds.dimensions[dim].size)

    return dim_coords


def get_timestamp_str(times):
    """
    Creates timestamp string to describe time range for netCDF dataset

    :param times: Time values for netCDF dataset in integer form with units and calendar attributes.
    :return: String containing appropriate timestamp.
    """
    calendar = times.calendar
    units = times.units
    data = np.sort(times[:])
    
    start_t = num2date(data[0], units=units, calendar=calendar)
    if times.shape[0] == 1:
        return start_t.strftime("%Y%m%d%H")
    else:
        dt = num2date(data[1], units=units, calendar=calendar) - start_t
        minutes = dt.total_seconds() / 60
        hours = minutes / 60
        days = hours / 24
        months = days / 30
    
        if minutes < 1:
            time_format = "%Y%m%d%H%M%S"
        elif hours < 1:
            time_format = "%Y%m%d%H"
        elif days < 1:
            time_format = "%Y%m%d"
        elif months < 1:
            time_format = "%Y%m"
        else:
            time_format = "%Y"
        
        end_t = num2date(data[-1], units=units, calendar=calendar)
        return f"{start_t.strftime(time_format)}-{end_t.strftime(time_format)}"


class MHFDataset:
    def __init__(self, hf_paths):
        self.__hf_files = [Path(path) for path in hf_paths]
        self.__hf_datasets = None
        self.__time_mapping = {}
        self.__data_coords = None

        with Dataset(self.__hf_files[0], 'r') as ds:
            self.__time_name, self.time_bnds_name = get_time_variables_names(ds)

    def open(self):
        if self.__hf_datasets is None:
            self.__hf_datasets = [Dataset(path, 'r') for path in self.__hf_files]
            for hf_index in range(len(self.__hf_datasets)):
                time_vals = np.squeeze(self.__hf_datasets[hf_index][self.__time_name][:])
                if len(time_vals.shape) == 0:
                    time_vals = [float(time_vals)]
                
                
                for time in time_vals:
                    time = float(time)
                    if time in self.__time_mapping:
                        self.__time_mapping[time].append(hf_index)
                    else:
                        self.__time_mapping[time] = [hf_index]
            if not self.is_time_consistent():
                raise Exception("Fragmentation is not consistent over time.")

    def close(self):
        for ds in self.__hf_datasets:
            ds.close()

    def get_time_vals(self):
        return list(self.__time_mapping.keys())

    def get_timestamp_string(self):
        return get_timestamp_str(self.__hf_datasets[0][self.__time_name])

    def is_time_consistent(self):
        n_time_files = len(self.__time_mapping[self.get_time_vals()[0]])
        for time in self.__time_mapping:
            if len(self.__time_mapping[time]) != n_time_files:
                return False
        return True

    def is_fragmented(self):
        init_time = self.get_time_vals()[0]
        if len(self.__time_mapping[init_time]) > 1:
            return True
        return False

    def get_var_dimensions(self, var_name):
        init_ds = self.__hf_datasets[0][var_name]
        return list(init_ds.dimensions)

    def get_var_dtype(self, var_name):
        return self.__hf_datasets[0][var_name].dtype

    def get_var_attrs(self, var_name):
        return get_attributes(self.__hf_datasets[0][var_name])

    def __check_coord_map(self):
        if self.__data_coords is None:
            if self.is_fragmented():
                self.__data_coords = get_concat_coords(self.__hf_datasets)
            else:
                self.__data_coords = {}
                init_ds = self.__hf_datasets[0]
                for dim in init_ds.dimensions:
                    if dim in init_ds.variables:
                        self.__data_coords[dim] = init_ds[dim][:]
                    else:
                        self.__data_coords[dim] = np.arange(init_ds.dimensions[dim].size)
                self.__data_coords[self.__time_name] = self.get_time_vals()

    def get_var_data_shape(self, var_name):
        self.__check_coord_map()
        if var_name in self.__data_coords:
            return [len(self.__data_coords[var_name])]
        else:
            init_ds = self.__hf_datasets[0][var_name]
            dim_shape = []
            if self.__time_name in init_ds.dimensions:
                dim_shape.append(len(self.get_time_vals()))
            dim_shape += [len(self.__data_coords[dim]) for dim in init_ds.dimensions if dim != self.__time_name]
            return dim_shape

    def get_var_vals(self, var_name, time_index_start=0, time_index_end=-1):
        self.__check_coord_map()
        if var_name in self.__data_coords:
            return self.__data_coords[var_name]

        if "time" not in self.get_var_dimensions(var_name):
            return self.__hf_datasets[0][var_name][:]

        time_vals = self.get_time_vals()[time_index_start:time_index_end]
        data_shape = self.get_var_data_shape(var_name)
        data_shape[0] = len(time_vals)

        var_vals = np.empty(data_shape, dtype=self.__hf_datasets[0][var_name].dtype)
        if not self.is_fragmented():
            for index, time_val in enumerate(time_vals):
                hf_data = self.__hf_datasets[self.__time_mapping[time_val][0]]
                if hf_data[self.__time_name].shape[0] > 1:
                    sub_t_index = np.where(hf_data[self.__time_name][:] == time_val)[0]
                    var_vals[index] = hf_data[var_name][:][sub_t_index]
                else:
                    var_vals[index] = hf_data[var_name][:]
        else:
            for time_index, time_val in enumerate(time_vals):
                for hf_index in self.__time_mapping[time_val]:
                    hf_data = self.__hf_datasets[hf_index]
                    if self.__time_name in hf_data[var_name].dimensions and hf_data[self.__time_name].shape[0] > 1:
                        sub_t_index = np.where(hf_data[self.__time_name][:] == time_val)[0]
                        hf_data_fragment = hf_data[var_name][:][sub_t_index]
                    else:
                        hf_data_fragment = hf_data[var_name][:]
                    
                    index_ranges = []
                    for dim_index, dim in enumerate(hf_data[var_name].dimensions):
                        if dim == self.__time_name:
                            index_ranges.append(time_index)
                        elif dim in hf_data.variables:
                            dim_vals = hf_data[dim][:]
                            lower_index = np.where(np.min(dim_vals) == self.__data_coords[dim])[0][0]
                            upper_index = np.where(np.max(dim_vals) == self.__data_coords[dim])[0][0]
                            if lower_index == upper_index:
                                index_ranges.append(lower_index)
                            else:
                                index_ranges.append(slice(lower_index, upper_index+1))
                        elif var_vals.shape[dim_index] == 1:
                            index_ranges.append(0)
                        else:
                            index_ranges.append(slice(0, var_vals.shape[dim_index]))
                    
                    var_vals[tuple(index_ranges)] = np.squeeze(hf_data_fragment)
        return var_vals

    def get_global_attrs(self):
        assert self.__hf_datasets is not None

        agg_attrs = {}
        for ds in self.__hf_datasets:
            agg_attrs |= get_attributes(ds)
        return agg_attrs

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __contains__(self, key):
        return key in self.__hf_files

    def __iter__(self):
        return iter(self.__hf_datasets)

    def __len__(self):
        return len(self.__hf_datasets)
    
    def __getitem__(self, item):
        return self.__hf_datasets[item]