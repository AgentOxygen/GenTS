import numpy as np
from pathlib import Path
from dask import delayed
from difflib import SequenceMatcher
import netCDF4
import cftime
import subprocess
from os.path import isfile
from os import remove
from time import time


class TimeSeriesOrder:
    def __init__(self, output_path_template, history_file_paths, overwrite=True):
        start = time()
        self.__history_files_paths = history_file_paths
        self.overwrite = overwrite

        print("Gathering future MFDataset metadata...", end="")
        try:
            full_ds = netCDF4.MFDataset(self.__history_files_paths, check=False, aggdim="time")
        except KeyError:
            msg = str(history_file_paths[0].parent) + "/" + str(output_path_template.name) + "*"
            raise KeyError(f"Variables or dimensions are not consistent for all history files under the following directory and prefix: {msg}")
        print(" Done.", end="")
        self.__variables = list(full_ds.variables)
        self.__global_attrs = {key: getattr(full_ds, key) for key in full_ds.ncattrs()}
        self.__primary_variables = []
        self.__primary_variables_dims = []
        self.__primary_variables_attrs = []
        self.__primary_variables_typecodes = []
        self.__primary_variables_shapes = []
        self.__auxiliary_variables = []
        self.__auxiliary_variables_dims = []
        self.__auxiliary_variables_attrs = []
        self.__auxiliary_variables_typecodes = []
        self.__auxiliary_variables_shapes = []
        for var_index, target_variable in enumerate(self.__variables):
            dim_coords = np.unique(list(full_ds[target_variable].dimensions))

            attrs = {}
            if type(full_ds[target_variable]) is netCDF4._netCDF4._Variable:
                for key in full_ds[target_variable].ncattrs():
                    attrs[key] = full_ds[target_variable].__getattr__(key)
            else:
                for key in full_ds[target_variable].ncattrs():
                    attrs[key] = full_ds[target_variable].getncattr(key)

            if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
                self.__primary_variables.append(target_variable)
                self.__primary_variables_dims.append(full_ds[target_variable].dimensions)
                self.__primary_variables_attrs.append(attrs)
                self.__primary_variables_typecodes.append(full_ds[target_variable].dtype)
                self.__primary_variables_shapes.append(full_ds[target_variable].shape)
            else:
                self.__auxiliary_variables.append(target_variable)
                self.__auxiliary_variables_dims.append(full_ds[target_variable].dimensions)
                self.__auxiliary_variables_attrs.append(attrs)
                self.__auxiliary_variables_typecodes.append(full_ds[target_variable].dtype)
                self.__auxiliary_variables_shapes.append(full_ds[target_variable].shape)

        self.__time_units = full_ds["time"].units
        self.__calendar = full_ds["time"].calendar
        self.__time_start = cftime.num2date(full_ds["time"][0], units=self.__time_units, calendar=self.__calendar)
        self.__time_end = cftime.num2date(full_ds["time"][-1], units=self.__time_units, calendar=self.__calendar)
        self.__cftime_step = cftime.num2date(full_ds["time"][1], units=self.__time_units, calendar=self.__calendar) - self.__time_start
        full_ds.close()

        self.__time_step = self.getTimeStep()
        self.__time_str_format = self.getTimeStrFormat(self.__time_step)
        self.__ts_output_tuples = []

        self.__output_path_template = Path(f"{output_path_template.parent}/{self.__time_step}/{output_path_template.name}")
        self.__output_path_template.parent.mkdir(parents=True, exist_ok=True)

        print(f" Output Directory Created: {self.getOutputPathTemplate()} ({round(time() - start, 2)}s)")
        for variable in self.__primary_variables:
            self.__ts_output_tuples.append((self.__auxiliary_variables + [variable], self.__output_path_template, self.__time_start, self.__time_end, self.__history_files_paths))
    
    def getOutputPathTemplate(self):
        return str(self.__output_path_template)

    def getPrimaryVariablesTuples(self):
        return self.__primary_variables, self.__primary_variables_dims, self.__primary_variables_attrs, self.__primary_variables_typecodes, self.__primary_variables_shapes

    def getauxiliaryVariablesTuples(self):
        return self.__auxiliary_variables, self.__auxiliary_variables_dims, self.__auxiliary_variables_attrs, self.__auxiliary_variables_typecodes, self.__auxiliary_variables_shapes

    def getGlobalAttributes(self):
        return self.__global_attrs

    def getIndices(self):
        return self.__time_path_indices

    def getTimeStrings(self):
        return self.__time_start.strftime(self.__time_str_format), self.__time_end.strftime(self.__time_str_format)

    def getCommandStrings(self):
        cmds = []
        for variables, template, start_time, end_time, paths in self.__ts_output_tuples:
            path_str = ""
            for path in paths:
                path_str += f"{str(path)} "

            var_str = ""
            for var_i in variables:
                var_str += f"{var_i},"

            start_time_str = start_time.strftime(self.__time_str_format)
            end_time_str = end_time.strftime(self.__time_str_format)

            cmd = "ncrcat -v " + var_str[:-1] + f" {path_str[:-1]}" + f" -O {template}.{variables[-1]}.{start_time_str}.{end_time_str}.nc"
            cmds.append(cmd)
        return cmds

    def getAllHistoryFilePaths(self):
        return self.__history_files_paths

    def getTimeStrFormat(self, time_step_label):
        if "hour" in time_step_label:
            time_str_format = "%Y-%m-%d-%H"
        elif "day" in time_step_label:
            time_str_format = "%Y-%m-%d"
        elif "month" in time_step_label:
            time_str_format = "%Y-%m"
        else:
            time_str_format = "%Y"
        return time_str_format

    def getTimeStep(self):
        dt_hrs = (self.__cftime_step).total_seconds() / 60 / 60
        if dt_hrs >= 24*365:
            return f"year_{int(np.ceil(dt_hrs / (24*365)))}"
        elif dt_hrs >= 24*28:
            return f"month_{int(np.ceil(dt_hrs / (24*31)))}"
        elif dt_hrs >= 24:
            return f"day_{int(np.ceil(dt_hrs / (24)))}"
        else:
            return f"hour_{int(np.ceil(dt_hrs))}"

    def generateTimeseries(self):
        hist_ds = netCDF4.MFDataset(self.getAllHistoryFilePaths(), aggdim="time")

        global_attrs = self.getGlobalAttributes()
        start_timestr, end_timestr = self.getTimeStrings()

        paths = []
        for var_index in range(len(self.__primary_variables)):
            variable = self.__primary_variables[var_index]
            dims = self.__primary_variables_dims[var_index]
            attrs = self.__primary_variables_attrs[var_index]
            dtype = self.__primary_variables_typecodes[var_index]
            shape = self.__primary_variables_shapes[var_index]

            hist_ds[variable].set_auto_mask(False)
            hist_ds[variable].set_auto_scale(False)
            hist_ds[variable].set_always_mask(False)

            variable_ts_output_path = f"{self.getOutputPathTemplate()}{variable}.{start_timestr}.{end_timestr}.nc"
            if self.overwrite and isfile(variable_ts_output_path):
                remove(variable_ts_output_path)

            paths.append(variable_ts_output_path)
            ts_ds = netCDF4.Dataset(variable_ts_output_path,
                                    mode="w")
            ts_ds.setncatts(global_attrs)

            for dim_index, dim in enumerate(dims):
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, shape[dim_index])

            var_data = ts_ds.createVariable(variable, dtype, dims)

            var_data.set_auto_mask(False)
            var_data.set_auto_scale(False)
            var_data.set_always_mask(False)

            ts_ds[variable].setncatts(attrs)
            # This is the chunk-writing loop, bulk of computation occurs here
            time_chunk_size = 1
            for i in range(0, hist_ds[variable].shape[0], time_chunk_size):
                if i + time_chunk_size > hist_ds[variable].shape[0]:
                    time_chunk_size =  hist_ds[variable].shape[0] - i
                var_data[i:i + time_chunk_size] = hist_ds[variable][i:i + time_chunk_size]

            ts_ds.close()

        aux_variable_data_tmp = {}
        for aux_variable in self.__auxiliary_variables:
            aux_variable_data_tmp[aux_variable] = hist_ds[aux_variable][:]
        hist_ds.close()

        for path in paths:
            var_ds = netCDF4.Dataset(path, mode="a")

            for aux_index, aux_variable in enumerate(self.__auxiliary_variables):
                for dim_index, dim in enumerate(self.__auxiliary_variables_dims[aux_index]):
                    if dim not in var_ds.dimensions:
                        var_ds.createDimension(dim, self.__auxiliary_variables_shapes[aux_index][dim_index])
                aux_var_data = var_ds.createVariable(aux_variable, self.__auxiliary_variables_typecodes[aux_index], self.__auxiliary_variables_dims[aux_index])
                var_ds[aux_variable].setncatts(self.__auxiliary_variables_attrs[aux_index])
                aux_var_data[:] = aux_variable_data_tmp[aux_variable]
            var_ds.close()


class TimeSeriesConfig:

    def getYearFromDataset(path):
        ds = netCDF4.Dataset(path, mode="r")
        if "time" not in ds.dimensions:
            raise KeyError(f"'time' dimension not found in dataset: {path}")
        return [path, cftime.num2date(ds["time"][0], units=ds["time"].units, calendar=ds["time"].calendar).year]
    
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

    
    def __init__(self, input_head_dir, output_head_dir, directory_name_swaps={}, file_name_exclusions=[], directory_name_exclusions=["rest", "logs"]):
        input_head_dir = Path(input_head_dir)
        output_head_dir = Path(output_head_dir)

        self.__netcdf_paths = []
        for path in sorted(input_head_dir.rglob("*.nc")):
            exclude = False
            for exclusion in file_name_exclusions:
                if exclusion in path.name:
                    exclude = True

            directory_names = [directory.name for directory in sorted(path.parents)]

            for exclusion in directory_name_exclusions:
                if exclusion in directory_names:
                    exclude = True

            if not exclude:
                self.__netcdf_paths.append(path)

        parent_directories = {}
        for path in self.__netcdf_paths:
            if path.parent in parent_directories:
                parent_directories[path.parent].append(path)
            else:
                parent_directories[path.parent] = [path]

        self.ts_order_parameters_unchunked = []
        self.ts_order_parameters_chunked = None
        self.__paths_to_years = None
        self.__chunk_size_yrs = None
        for parent in parent_directories:
            groups = TimeSeriesConfig.solveForGroupsLeftToRight([path.name for path in parent_directories[parent]])

            conflicts = {}

            for group in groups:
                for comparable_group in groups:
                    if group != comparable_group and group in comparable_group:
                        if group in conflicts:
                            conflicts[group].append(comparable_group)
                        else:
                            conflicts[group] = [comparable_group]
            
            group_to_history_paths = {group: [] for group in groups}
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
                                group_to_history_paths[group].append(path)
                        else:
                            group_to_history_paths[group].append(path)

            group_output_dir = TimeSeriesConfig.generateReflectiveOutputDirectory(input_head_dir, output_head_dir, parent, swaps=directory_name_swaps)
            for group in group_to_history_paths:
                if len(group_to_history_paths[group]) <= 1:
                    print(f"NOTE: Only one history file detected, skipping: '{group_to_history_paths[group]}'")
                    continue
                    # Maybe write a single timeseries dataset for each variable in this case?
                self.ts_order_parameters_unchunked.append((Path(group_output_dir + "/" + group), group_to_history_paths[group]))

    def mapHistoryFileTimestampYears(self, paths_years):
        self.__paths_to_years = {path: year for path, year in paths_years}

    def getHistoryFileYears(self):
        return [delayed(TimeSeriesConfig.getYearFromDataset)(path) for path in self.__netcdf_paths]
    
    def chunkOutputByYear(self, chunk_size_years):
        self.__chunk_size_yrs = chunk_size_years
        if self.__paths_to_years is None:
            self.mapHistoryFileTimestampYears([TimeSeriesConfig.getYearFromDataset(path) for path in self.__netcdf_paths])

        self.ts_order_parameters_chunked = []
        for output_dir_path, paths in self.ts_order_parameters_unchunked:
            years = [self.__paths_to_years[path] for path in paths]
            
            slices = []
            last_slice_yr = 0
            for index in range(len(years)):
                if years[index] % time_slice_size_yrs == 0 and years[index] != last_slice_yr:
                    if len(slices) == 0:
                        slices.append((0, index))
                    else:
                        slices.append((slices[-1][1], index))
                    last_slice_yr = years[index]
        
            for start, end in slices:
                self.ts_order_parameters_chunked.append((output_dir_path, paths[start:end]))
    
    def getChunkSizeYrs(self):
        return self.__chunk_size_yrs
    
    def getOrders(self):
        self.ts_orders = []
        if self.getChunkSizeYrs() is None:
            for output_dir_path, input_hist_paths in self.ts_order_parameters_unchunked:
                self.ts_orders.append(TimeSeriesOrder(output_dir_path, input_hist_paths))
        else:
            for output_dir_path, input_hist_paths in self.ts_order_parameters_chunked:
                self.ts_orders.append(TimeSeriesOrder(output_dir_path, input_hist_paths))
        
        return self.ts_orders
    
    def generateAllTimeseries(self, orders):
        return [delayed(order.generateTimeseries)() for order in orders]

    def generateAllTimeseriesNCO(self, orders):
        def execute_ncrcat_cmd(cmd):
            return subprocess.run([cmd], shell=True)

        cmd_strs = []
        for order in orders:
            for cmd_str in order.getCommandStrings():
                cmd_strs.append(cmd_str)
        return [delayed(execute_ncrcat_cmd)(cmd_str) for cmd in cmd_strs]

config_gen = TimeSeriesConfig(
    "/glade/derecho/scratch/nanr/archive/b.e21.B1850cmip6.f09_g17.DAMIP-ssp245-vlc.001/",
    "/glade/derecho/scratch/ccummins/ts_output/b.e21.B1850cmip6.f09_g17.DAMIP-ssp245-vlc.001/",
    directory_name_swaps={
        "hist": "proc/tseries"
    },
    file_name_exclusions=[".once.nc"])