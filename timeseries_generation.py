import numpy as np
from pathlib import Path
from dask import delayed
import netCDF4
import cftime
from time import time
from os.path import isfile, getsize
from os import remove
import dask.distributed


def generateReflectiveOutputDirectory(input_head, output_head, parent_dir, swaps={}):
    raw_sub_dir = str(parent_dir).split(str(input_head))[-1]
    raw_sub_dir_parsed = raw_sub_dir.split("/")
    for key in swaps:
        for index in range(len(raw_sub_dir_parsed)):
            if raw_sub_dir_parsed[index] == key:
                raw_sub_dir_parsed[index] = swaps[key]
                break
    output_dir = str(output_head) + "/"
    for dir in raw_sub_dir_parsed:
        output_dir += dir + "/"
    return output_dir


def solveForGroupsLeftToRight(file_names, delimiter="."):
    varying_parsed_names = [str(name).split(delimiter) for name in file_names]

    lengths = {}
    for parsed in varying_parsed_names:
        if len(parsed) in lengths:
            lengths[len(parsed)].append(parsed)
        else:
            lengths[len(parsed)] = [parsed]

    groups = []
    for l in lengths:
        parsed_names = np.array(lengths[l])
        unique_strs = []
        for i in range(parsed_names.shape[1]):
            unique_strs.append(np.unique(parsed_names[:, i]))
        common_str = ""
        for index in range(len(unique_strs)):
            if len(unique_strs[index]) == 1:
                if unique_strs[index][0] != "nc":
                    common_str += unique_strs[index][0] + delimiter
            else:
                break
        if len(unique_strs[index]) != len(parsed_names):
            for group in unique_strs[index]:
                groups.append(common_str + group + delimiter)
        else:
            groups.append(common_str)
    return groups


def generateTimeSeriesGroupPaths(paths, input_head_dir, output_head_dir, dir_name_swaps={}):
    timeseries_groups = {}
    parent_directories = {}
    for path in paths:
        if path.parent in parent_directories:
            parent_directories[path.parent].append(path)
        else:
            parent_directories[path.parent] = [path]

    for parent in parent_directories:
        groups = solveForGroupsLeftToRight([path.name for path in parent_directories[parent]])
        conflicts = {}
        for group in groups:
            for comparable_group in groups:
                if group != comparable_group and group in comparable_group:
                    if group in conflicts:
                        conflicts[group].append(comparable_group)
                    else:
                        conflicts[group] = [comparable_group]
        group_to_paths = {group: [] for group in groups}
        for path in parent_directories[parent]:
            for group in groups:
                if group in str(path.name):
                    if group in conflicts:
                        conflict = False
                        for conflict_group in conflicts[group]:
                            if conflict_group in str(path.name):
                                conflict = True
                                break
                        if not conflict:
                            group_to_paths[group].append(path)
                    else:
                        group_to_paths[group].append(path)

        group_output_dir = generateReflectiveOutputDirectory(input_head_dir, output_head_dir, parent, swaps=dir_name_swaps)
        for group in group_to_paths:
            timeseries_groups[Path(group_output_dir + "/" + group)] = group_to_paths[group]
    return timeseries_groups


def isVariableAuxiliary(variable_meta):
    dim_coords = np.unique(variable_meta["dimensions"])

    primary_tags = ["time"]
    aux_tags = ["nbnd", "chars", "string_length", "hist_interval"]
    for tag in aux_tags:
        if tag in dim_coords:
            return True

    if len(dim_coords) > 1:
        for tag in primary_tags:
            if tag in dim_coords:
                return False

    return True


def getHistoryFileMetaData(hs_file_path):
    meta = {}
    ds = netCDF4.Dataset(hs_file_path, mode="r")

    meta["file_size"] = getsize(hs_file_path)
    meta["variables"] = list(ds.variables)
    meta["global_attrs"] = {key: getattr(ds, key) for key in ds.ncattrs()}
    meta["variable_meta"] = {}
    if "time" in meta["variables"]:
        meta["time"] = cftime.num2date(ds["time"][:], units=ds["time"].units, calendar=ds["time"].calendar)
    else:
        meta["time"] = None

    for variable in meta["variables"]:
        meta["variable_meta"][variable] = {}
        if type(ds[variable]) is netCDF4._netCDF4._Variable:
            for key in ds[variable].ncattrs():
                meta["variable_meta"][variable][key] = ds[variable].__getattr__(key)
        else:
            for key in ds[variable].ncattrs():
                meta["variable_meta"][variable][key] = ds[variable].getncattr(key)

        meta["variable_meta"][variable]["dimensions"] = list(ds[variable].dimensions)
        meta["variable_meta"][variable]["data_type"] = ds[variable].dtype
        meta["variable_meta"][variable]["shape"] = ds[variable].shape

    meta["primary_variables"] = []
    meta["auxiliary_variables"] = []
    for variable in meta["variable_meta"]:
        if isVariableAuxiliary(meta["variable_meta"][variable]):
            meta["auxiliary_variables"].append(variable)
        else:
            meta["primary_variables"].append(variable)
    ds.close()
    return meta


def getYearSlices(years, slice_length):
    slices = []
    last_slice_yr = years[0]
    for index in range(len(years)):
        if years[index] % slice_length == 0 and years[index] != last_slice_yr:
            if len(slices) == 0:
                slices.append((0, index))
            else:
                slices.append((slices[-1][1], index))
            last_slice_yr = years[index]
    if len(slices) == 0:
        slices.append((0, index + 1))
    elif slices[-1][1] != index + 1:
        slices.append((slices[-1][1], index + 1))
    return slices


def generateTimeSeries(output_template, hf_paths, metadata, time_str_format, overwrite=False, debug_timing=False):
    debug_start_time = time()
    output_template.parent.mkdir(parents=True, exist_ok=True)

    auxiliary_ds = netCDF4.MFDataset(hf_paths, aggdim="time", exclude=metadata["primary_variables"])
    auxiliary_ds.set_auto_mask(False)
    auxiliary_ds.set_auto_scale(False)
    auxiliary_ds.set_always_mask(False)

    time_start_str = cftime.num2date(auxiliary_ds["time"][0],
                                     units=auxiliary_ds["time"].units,
                                     calendar=auxiliary_ds["time"].calendar).strftime(time_str_format)
    time_end_str = cftime.num2date(auxiliary_ds["time"][-1],
                                   units=auxiliary_ds["time"].units,
                                   calendar=auxiliary_ds["time"].calendar).strftime(time_str_format)

    auxiliary_variable_data = {}
    for auxiliary_var in metadata["auxiliary_variables"]:
        attrs = {}
        for key in auxiliary_ds[auxiliary_var].ncattrs():
            if type(auxiliary_ds[auxiliary_var]) is netCDF4._netCDF4._Variable:
                attrs[key] = auxiliary_ds[auxiliary_var].__getattr__(key)
            else:
                attrs[key] = auxiliary_ds[auxiliary_var].getncattr(key)
        auxiliary_variable_data[auxiliary_var] = {
            "attrs": attrs,
            "dimensions": auxiliary_ds[auxiliary_var].dimensions,
            "shape": auxiliary_ds[auxiliary_var].shape,
            "dtype": auxiliary_ds[auxiliary_var].dtype,
            "data": auxiliary_ds[auxiliary_var][:],
        }
    auxiliary_ds.close()

    primary_ds = netCDF4.MFDataset(hf_paths, aggdim="time", exclude=metadata["auxiliary_variables"])
    primary_ds.set_auto_mask(False)
    primary_ds.set_auto_scale(False)
    primary_ds.set_always_mask(False)

    ts_paths = []
    for primary_var in metadata["primary_variables"]:
        ts_path = output_template.parent / f"{output_template.name}{primary_var}.{time_start_str}.{time_end_str}.nc"
        ts_paths.append(ts_path)
        if isfile(ts_path) and not overwrite:
            continue
        elif isfile(ts_path) and overwrite:
            remove(ts_path)
        ts_ds = netCDF4.Dataset(ts_path, mode="w")
        for dim_index, dim in enumerate(primary_ds[primary_var].dimensions):
            if dim not in ts_ds.dimensions:
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, primary_ds[primary_var].shape[dim_index])

        var_data = ts_ds.createVariable(primary_var,
                                        primary_ds[primary_var].dtype,
                                        primary_ds[primary_var].dimensions)
        var_data.set_auto_mask(False)
        var_data.set_auto_scale(False)
        var_data.set_always_mask(False)

        attrs = {}
        if type(primary_ds[primary_var]) is netCDF4._netCDF4._Variable:
            for key in primary_ds[primary_var].ncattrs():
                attrs[key] = primary_ds[primary_var].__getattr__(key)
        else:
            for key in primary_ds[primary_var].ncattrs():
                attrs[key] = primary_ds[primary_var].getncattr(key)

        ts_ds[primary_var].setncatts(attrs)


        time_chunk_size = 1
        if len(primary_ds[primary_var].shape) > 0 and "time" in primary_ds[primary_var].dimensions:
            for i in range(0, primary_ds[primary_var].shape[0], time_chunk_size):
                if i + time_chunk_size > primary_ds[primary_var].shape[0]:
                    time_chunk_size = primary_ds[primary_var].shape[0] - i
                var_data[i:i + time_chunk_size] = primary_ds[primary_var][i:i + time_chunk_size]
        else:
            var_data[:] = primary_ds[primary_var][:]

        for auxiliary_var in auxiliary_variable_data:
            for dim_index, dim in enumerate(auxiliary_variable_data[auxiliary_var]["dimensions"]):
                if dim not in ts_ds.dimensions:
                    ts_ds.createDimension(dim, auxiliary_variable_data[auxiliary_var]["shape"][dim_index])
            aux_var_data = ts_ds.createVariable(auxiliary_var,
                                                auxiliary_variable_data[auxiliary_var]["dtype"],
                                                auxiliary_variable_data[auxiliary_var]["dimensions"])
            aux_var_data.set_auto_mask(False)
            aux_var_data.set_auto_scale(False)
            aux_var_data.set_always_mask(False)
            ts_ds[auxiliary_var].setncatts(auxiliary_variable_data[auxiliary_var]["attrs"])

            aux_var_data[:] = auxiliary_variable_data[auxiliary_var]["data"]

        ts_ds.setncatts({key: getattr(primary_ds, key) for key in primary_ds.ncattrs()})
        ts_ds.close()

    if debug_timing:
        return (time() - debug_start_time, ts_paths)
    else:
        return ts_paths


class ModelOutputDatabase:
    def __init__(self, hf_head_dir, ts_head_dir, dir_name_swaps={}, file_exclusions=[], dir_exclusions=["rest", "logs"]):
        self.__hf_head_dir = Path(hf_head_dir)
        self.__ts_head_dir = Path(ts_head_dir)
        self.__total_size = 0

        self.__history_file_paths = []
        for path in sorted(self.__hf_head_dir.rglob("*.nc")):
            exclude = False
            for exclusion in file_exclusions:
                if exclusion in path.name:
                    exclude = True

            directory_names = [directory.name for directory in sorted(path.parents)]

            for exclusion in dir_exclusions:
                if exclusion in directory_names:
                    exclude = True

            if not exclude:
                self.__history_file_paths.append(path)
        self.__timeseries_group_paths = generateTimeSeriesGroupPaths(self.__history_file_paths, hf_head_dir, ts_head_dir, dir_name_swaps=dir_name_swaps)

    def getHistoryFileMetaData(self, history_file_path):
        return self.__history_file_metas[history_file_path]

    def getTotalFileSize(self):
        return self.__total_size

    def getTimeSeriesGroups(self):
        return self.__timeseries_group_paths

    def getHistoryFilePaths(self):
        return self.__history_file_paths

    def getTimeStepHours(self, hf_paths):
        times = []
        for path in hf_paths:
            hf_time = self.__history_file_metas[path]["time"]
            if len(hf_time) == 0:
                return 0
            elif len(hf_time) == 1:
                times.append(hf_time[0])
            else:
                times.append(hf_time[0])
                times.append(hf_time[1])
                break

        times.sort()
        if len(times) == 1:
            return 0
        else:
            return (times[1] - times[0]).total_seconds() / 60 / 60

    def getTimeStepStr(self, hf_paths):
        if "time_period_freq" in self.__history_file_metas[hf_paths[0]]["global_attrs"]:
            return self.__history_file_metas[hf_paths[0]]["global_attrs"]["time_period_freq"]

        dt_hrs = self.getTimeStepHours(hf_paths)
        if dt_hrs >= 24*364:
            return f"year_{int(np.ceil(dt_hrs / (24*365)))}"
        elif dt_hrs >= 24*28:
            return f"month_{int(np.ceil(dt_hrs / (24*31)))}"
        elif dt_hrs >= 24:
            return f"day_{int(np.ceil(dt_hrs / (24)))}"
        else:
            return f"hour_{int(np.ceil(dt_hrs))}"

    def getTimeStepStrFormat(self, timestep_str):
        if "hour" in timestep_str:
            return "%Y-%m-%d-%H"
        elif "day" in timestep_str:
            return "%Y-%m-%d"
        elif "month" in timestep_str:
            return "%Y-%m"
        elif "year" in timestep_str:
            return "%Y"
        else:
            return "%Y-%m-%d-%H"

    def build(self, client=None):
        if client is None:
            client = dask.distributed.client._get_global_client()

        self.__history_file_metas = {}
        if client is None:
            for path in self.__history_file_paths:
                self.__history_file_metas[path] = getHistoryFileMetaData(path)
        else:
            metas = dask.compute(*[delayed(getHistoryFileMetaData)(path) for path in self.__history_file_paths])
            for index, path in enumerate(self.__history_file_paths):
                self.__history_file_metas[path] = metas[index]

        new_timeseries_group_paths = {}
        for path_template in self.__timeseries_group_paths:
            hs_file_paths = self.__timeseries_group_paths[path_template]
            timestep_str = self.getTimeStepStr(hs_file_paths)
            new_path_template = (path_template.parent / timestep_str) / path_template.name
            new_timeseries_group_paths[new_path_template] = hs_file_paths
        self.__timeseries_group_paths = new_timeseries_group_paths

        for meta in metas:
            self.__total_size += meta["file_size"]

    def run(self, client=None, timeseries_year_length=10, overwrite=False, serial=False):
        if client is None:
            client = dask.distributed.client._get_global_client()

        delayed_ts_gen_funcs = []
        futures = []
        for output_template in self.getTimeSeriesGroups():
            hf_paths = self.getTimeSeriesGroups()[output_template]
            years = [self.getHistoryFileMetaData(hf_path)["time"][0].year for hf_path in hf_paths]
            for start_index, end_index in getYearSlices(years, timeseries_year_length):
                slice_paths = hf_paths[start_index:end_index]
                metadata = self.getHistoryFileMetaData(slice_paths[0])
                time_str_format = self.getTimeStepStrFormat(self.getTimeStepStr(slice_paths))
                for primary_variable in metadata["primary_variables"]:
                    slice_metadata = metadata
                    slice_metadata["time"] = None
                    slice_metadata["primary_variables"] = [primary_variable]
                    futures.append(client.submit(generateTimeSeries, output_template, slice_paths, slice_metadata, time_str_format, overwrite))
        ts_paths = []
        if client is None or serial:
            for func in delayed_ts_gen_funcs:
                ts_paths.append(func.compute())
        else:
            ts_paths = client.gather(futures)
        return ts_paths