import numpy as np
from pathlib import Path
from dask import delayed
import netCDF4
import cftime
from os.path import isfile, getsize
from os import remove, listdir
from shutil import move
from time import time
import dask.distributed
from os import rmdir


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


def expandDataset(input_ds_path, output_ds_dir, suffix="", overwrite=False, local_storage=None):
    ds = netCDF4.Dataset(input_ds_path, mode="r")
    ds.set_auto_mask(False)
    ds.set_auto_scale(False)
    ds.set_always_mask(False)

    global_attrs = {key: getattr(ds, key) for key in ds.ncattrs()}
    output_paths = []
    local_paths = []

    for target_variable in ds.variables:
        if target_variable == "time":
            continue

        output_path = output_ds_dir / target_variable / f"{target_variable}{suffix}.nc"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_paths.append(output_path)
        if isfile(output_path):
            if overwrite:
                remove(output_path)
            else:
                continue

        if local_storage is None:
            ts_ds = netCDF4.Dataset(output_path, format="NETCDF3_CLASSIC", mode="w")
        else:
            local_path = local_storage / f"{target_variable}{suffix}.nc"
            ts_ds = netCDF4.Dataset(local_path, format="NETCDF3_CLASSIC", mode="w")
            local_paths.append((local_path, output_path))

        ts_ds.setncatts(global_attrs)

        for variable in ["time", target_variable]:
            for dim_index, dim in enumerate(ds[variable].dimensions):
                if dim not in ts_ds.dimensions:
                    if dim == "time":
                        ts_ds.createDimension(dim, None)
                    else:
                        ts_ds.createDimension(dim, ds[variable].shape[dim_index])
            var_data = ts_ds.createVariable(variable, ds[variable].dtype, ds[variable].dimensions)
            var_data.set_auto_mask(False)
            var_data.set_auto_scale(False)
            var_data.set_always_mask(False)

            attrs = {}
            for key in ds[variable].ncattrs():
                if type(ds[variable]) is netCDF4._netCDF4._Variable:
                    attrs[key] = ds[variable].__getattr__(key)
                else:
                    attrs[key] = ds[variable].getncattr(key)
            ts_ds[variable].setncatts(attrs)

            # if len(ds[variable].shape) > 0:
            #     time_chunk_size = 1
            #     for i in range(0, ds[variable].shape[0], time_chunk_size):
            #         if i + time_chunk_size > ds[variable].shape[0]:
            #             time_chunk_size = ds[variable].shape[0] - i
            #         var_data[i:i + time_chunk_size] = ds[variable][i:i + time_chunk_size]
            if len(ds[variable].shape) > 0:
                var_data[:] = ds[variable][:]
        ts_ds.close()

    if local_storage is not None:
        for local_path, output_path in local_paths:
            move(local_path, output_path)
            if isfile(local_path):
                remove(local_path)
    return output_paths


def concatenateSingleVariableDatasets(timestep_dataset_paths, output_template, time_str_format, overwrite=False, delete_timesteps=True):
    output_template.parent.mkdir(parents=True, exist_ok=True)

    concat_ds = netCDF4.MFDataset(timestep_dataset_paths)
    concat_ds.set_auto_mask(False)
    concat_ds.set_auto_scale(False)
    concat_ds.set_always_mask(False)

    variables = list(concat_ds.variables)
    time_start_str = cftime.num2date(concat_ds["time"][0], units=concat_ds["time"].units, calendar=concat_ds["time"].calendar).strftime(time_str_format)
    time_end_str = cftime.num2date(concat_ds["time"][-1], units=concat_ds["time"].units, calendar=concat_ds["time"].calendar).strftime(time_str_format)

    target_variable = "time"
    for variable in variables:
        if variable != "time":
            target_variable = variable
            break

    output_dataset_path = output_template.parent / f"{output_template.name}{target_variable}.{time_start_str}.{time_end_str}.nc"
    if isfile(output_dataset_path):
        if overwrite:
            remove(output_dataset_path)
        else:
            return (target_variable, output_dataset_path)

    ts_ds = netCDF4.Dataset(output_dataset_path, mode="w")
    ts_ds.setncatts({key: getattr(concat_ds, key) for key in concat_ds.ncattrs()})

    for variable in variables:
        for dim_index, dim in enumerate(concat_ds[variable].dimensions):
            if dim not in ts_ds.dimensions:
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, concat_ds[variable].shape[dim_index])

        var_data = ts_ds.createVariable(variable, concat_ds[variable].dtype, concat_ds[variable].dimensions)

        var_data.set_auto_mask(False)
        var_data.set_auto_scale(False)
        var_data.set_always_mask(False)

        attrs = {}
        if type(concat_ds[variable]) is netCDF4._netCDF4._Variable:
            for key in concat_ds[variable].ncattrs():
                attrs[key] = concat_ds[variable].__getattr__(key)
        else:
            for key in concat_ds[variable].ncattrs():
                attrs[key] = concat_ds[variable].getncattr(key)

        ts_ds[variable].setncatts(attrs)

        time_chunk_size = 1
        if len(concat_ds[variable].shape) > 0:
            for i in range(0, concat_ds[variable].shape[0], time_chunk_size):
                if i + time_chunk_size > concat_ds[variable].shape[0]:
                    time_chunk_size = concat_ds[variable].shape[0] - i
                var_data[i:i + time_chunk_size] = concat_ds[variable][i:i + time_chunk_size]
    ts_ds.close()
    concat_ds.close()

    if delete_timesteps:
        for path in timestep_dataset_paths:
            remove(path)

    return (target_variable, output_dataset_path)


def addAuxiliaryVariables(auxiliary_paths, primary_paths, delete_auxiliary_concats=True):
    aux_dims = []
    aux_dim_shapes = []

    aux_variables = []
    aux_variable_dims = []
    aux_dtypes = []
    aux_attrs = []
    aux_data = []
    for path in auxiliary_paths:
        aux_ds = netCDF4.Dataset(path, mode="r")
        aux_ds.set_auto_mask(False)
        aux_ds.set_auto_scale(False)
        aux_ds.set_always_mask(False)

        for variable in aux_ds.variables:
            if variable == "time":
                continue

            for dim_index, dim in enumerate(aux_ds[variable].dimensions):
                if dim not in aux_dims and dim != "time":
                    aux_dims.append(dim)
                    aux_dim_shapes.append(aux_ds[variable].shape[dim_index])

        if variable not in aux_variables:
            aux_variables.append(variable)
            aux_variable_dims.append(aux_ds[variable].dimensions)
            aux_dtypes.append(aux_ds[variable].dtype)

            attrs = {}
            if type(aux_ds[variable]) is netCDF4._netCDF4._Variable:
                for key in aux_ds[variable].ncattrs():
                    attrs[key] = aux_ds[variable].__getattr__(key)
            else:
                for key in aux_ds[variable].ncattrs():
                    attrs[key] = aux_ds[variable].getncattr(key)
            aux_attrs.append(attrs)
            aux_data.append(aux_ds[variable][:])
        aux_ds.close()

    for path in primary_paths:
        primary_ds = netCDF4.Dataset(path, mode="a")
        for dim_index, dim in enumerate(aux_dims):
            if dim not in primary_ds.dimensions:
                primary_ds.createDimension(dim, aux_dim_shapes[dim_index])

        for var_index, variable in enumerate(aux_variables):
            if variable not in primary_ds.variables:
                var_data = primary_ds.createVariable(variable, aux_dtypes[var_index], aux_variable_dims[var_index])

                var_data.set_auto_mask(False)
                var_data.set_auto_scale(False)
                var_data.set_always_mask(False)
                primary_ds[variable].setncatts(aux_attrs[var_index])
                var_data[:] = aux_data[var_index]
        primary_ds.close()

    if delete_auxiliary_concats:
        for path in auxiliary_paths:
            remove(path)
    return primary_paths


class ModelOutputDatabase:
    def __init__(self, hf_head_dir, ts_head_dir, dir_name_swaps={}, file_exclusions=[], dir_exclusions=["rest", "logs"], local_storage=None):
        self.__hf_head_dir = Path(hf_head_dir)
        self.__ts_head_dir = Path(ts_head_dir)
        if local_storage is None:
            self.__local_storage = None
        else:
            self.__local_storage = Path(local_storage)
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
        if dt_hrs >= 24*365:
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

    def run_serial(self):
        pass

    def run(self, client=None, overwrite_expands=False):
        if client is None:
            client = dask.distributed.client._get_global_client()

        if client is None:
            self.run_serial()
            return None

        hf_paths = []
        timestep_output_dirs = []
        suffixes = []
        overwrites = []
        local_storages = []
        for template_path in self.__timeseries_group_paths:
            timestep_strfrmt = self.getTimeStepStrFormat(template_path.parent.name)
            for hf_path in self.__timeseries_group_paths[template_path]:
                hf_paths.append(hf_path)
                overwrites.append(overwrite_expands)
                local_storages.append(self.__local_storage)
                timestep_output_dirs.append(template_path)
                time = self.getHistoryFileMetaData(hf_path)["time"][0]
                if time is None:
                    suffixes.append(".None")
                else:
                    suffixes.append("." + time.strftime(timestep_strfrmt))
        expand_futures = client.map(expandDataset, hf_paths, timestep_output_dirs, suffixes, overwrites, local_storages)
        expanded_paths_grouped = client.gather(expand_futures)
        # expanded_paths_grouped = []
        # for future in expand_futures:
        #     expanded_paths_grouped.append(future.result())
        #     future.release()

        expanded_to_hf_map = {}
        timestep_variable_map = {}

        for timestep_dir in timestep_output_dirs:
            timestep_variable_map[timestep_dir] = {}

        for index, group in enumerate(expanded_paths_grouped):
            for path in group:
                expanded_to_hf_map[path] = hf_paths[index]
                timestep_dir = path.parent.parent
                if path.parent not in timestep_variable_map[timestep_dir]:
                    timestep_variable_map[timestep_dir][path.parent] = [path]
                else:
                    timestep_variable_map[timestep_dir][path.parent].append(path)

        concat_output_templates = []
        concat_input_sets = []
        concat_timestep_st_formats = []
        overwrites = []
        for output_template in timestep_variable_map:
            for variable_path in timestep_variable_map[output_template]:
                concat_output_templates.append(output_template)
                concat_input_sets.append(timestep_variable_map[output_template][variable_path])
                concat_timestep_st_formats.append(self.getTimeStepStrFormat(variable_path.parent.parent.name))
                overwrites.append(overwrite_expands)

        concat_futures = client.map(concatenateSingleVariableDatasets,
                                    concat_input_sets,
                                    concat_output_templates,
                                    concat_timestep_st_formats,
                                    overwrites)
        concatd_tuples = client.gather(concat_futures)

        template_metadata = {}
        for template in self.getTimeSeriesGroups():
            hf_metadata = self.getHistoryFileMetaData(self.getTimeSeriesGroups()[template][0])
            template_metadata[template] = {
                "primary_variables": hf_metadata["primary_variables"],
                "primary_paths": [],
                "auxiliary_paths": []
            }

        for index in range(len(concatd_tuples)):
            variable, concat_path = concatd_tuples[index]
            template = concat_output_templates[index]
            if variable in template_metadata[template]["primary_variables"]:
                template_metadata[template]["primary_paths"].append(concat_path)
            else:
                template_metadata[template]["auxiliary_paths"].append(concat_path)

        auxiliary_path_sets = []
        primary_path_sets = []

        for template in template_metadata:
            auxiliary_path_sets.append(template_metadata[template]["auxiliary_paths"])
            primary_path_sets.append(template_metadata[template]["primary_paths"])

        append_auxiliary_futures = client.map(addAuxiliaryVariables, auxiliary_path_sets, primary_path_sets)
        appended_primary_paths_sets = client.gather(append_auxiliary_futures)

        for path in self.__ts_head_dir.rglob("*"):
            if not isfile(path):
                if len(listdir(path)) == 0:
                    rmdir(path)

        return appended_primary_paths_sets