import numpy as np
from pathlib import Path
from dask import delayed
from difflib import SequenceMatcher
import netCDF4
import cftime
import subprocess


class TimeSeriesOrder:
    def __init__(self, output_path_template, history_file_paths):
        self.__output_path_template = output_path_template
        self.__history_files_paths = history_file_paths

        full_ds = netCDF4.MFDataset(self.__history_files_paths, check=False, aggdim="time")
        self.__variables = list(full_ds.variables)
        self.__global_attrs = {key: getattr(full_ds, key) for key in full_ds.ncattrs()}
        self.__primary_variables = []
        self.__primary_variables_dims = []
        self.__primary_variables_attrs = []
        self.__primary_variables_typecodes = []
        self.__primary_variables_shapes = []
        self.__auxillary_variables = []
        self.__auxillary_variables_dims = []
        self.__auxillary_variables_attrs = []
        self.__auxillary_variables_typecodes = []
        self.__auxillary_variables_shapes = []
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
                self.__auxillary_variables.append(target_variable)
                self.__auxillary_variables_dims.append(full_ds[target_variable].dimensions)
                self.__auxillary_variables_attrs.append(attrs)
                self.__auxillary_variables_typecodes.append(full_ds[target_variable].dtype)
                self.__auxillary_variables_shapes.append(full_ds[target_variable].shape)

        self.__time_raw = full_ds["time"][:]
        self.__time_units = full_ds["time"].units
        self.__time = cftime.num2date(self.__time_raw, self.__time_units)
        full_ds.close()

        self.__history_file_times = []
        for path in history_file_paths:
            ds = netCDF4.Dataset(path)
            self.__history_file_times.append(ds["time"][:])
            ds.close()

        self.__time_path_indices = self.indexTimestampsToPaths(self.__history_files_paths, self.__time_raw)
        self.__time_step = self.getTimeStep()
        self.__time_str_format = self.getTimeStrFormat(self.__time_step)
        self.__history_file_groupings = []
        self.__ts_output_tuples = []
        self.__chunk_year_length = None
        self.generateOutputs()

    def getOutputPathTemplate(self):
        return self.__output_path_template

    def getPrimaryVariablesTuples(self):
        return self.__primary_variables, self.__primary_variables_dims, self.__primary_variables_attrs, self.__primary_variables_typecodes, self.__primary_variables_shapes

    def getAuxillaryVariablesTuples(self):
        return self.__auxillary_variables, self.__auxillary_variables_dims, self.__auxillary_variables_attrs, self.__auxillary_variables_typecodes, self.__auxillary_variables_shapes

    def getGlobalAttributes(self):
        return self.__global_attrs

    def getIndices(self):
        return self.__time_path_indices

    def getTimeStrings(self):
        return self.__time[0].strftime(self.__time_str_format), self.__time[-1].strftime(self.__time_str_format)

    def indexTimestampsToPaths(self, paths, concat_times):
        index_to_path = []
        for index in range(len(concat_times)):
            path_index = None
            for times in self.__history_file_times:
                if concat_times[index] in times:
                    path_index = index
                    break
            index_to_path.append(path_index)
        return index_to_path

    def generateOutputs(self):
        ts_slices = []
        yr_start = 0
        if self.__chunk_year_length is not None:
            for index in range(len(self.__time)):
                if self.__time[index].year >= self.__time[yr_start].year + self.__chunk_year_length:
                    ts_slices.append((yr_start, index))
                    yr_start = index
        else:
            ts_slices.append((0, len(self.__time)))

        input_hist_file_tuples = []
        for start_index, end_index in ts_slices:
            path_start_index = self.__time_path_indices[start_index]
            path_end_index = self.__time_path_indices[end_index-1]
            input_hist_file_tuples.append((self.__time[start_index], self.__time[end_index-1], self.__history_files_paths[path_start_index:path_end_index]))

        self.__history_file_groupings = input_hist_file_tuples

        for start_time, end_time, paths in input_hist_file_tuples:
            for variable in self.__primary_variables:
                self.__ts_output_tuples.append((self.__auxillary_variables + [variable], self.__output_path_template, start_time, end_time, paths))

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
        dt_hrs = (self.__time[1] - self.__time[0]).total_seconds() / 60 / 60
        if dt_hrs >= 24*365:
            return f"year_{int(dt_hrs / (24*365))}"
        elif 24*31 >= dt_hrs >= 24*30:
            return f"month_{int(dt_hrs / (24*30))}"
        elif dt_hrs >= 24:
            return f"day_{int(dt_hrs / (24))}"
        else:
            return f"hour_{int(dt_hrs)}"

    def generateTimeseries(self):
        hist_ds = netCDF4.MFDataset(self.getAllHistoryFilePaths(), aggdim="time")

        global_attrs = self.getGlobalAttributes()
        start_timestr, end_timestr = self.getTimeStrings()

        for var_index in range(len(self.__primary_variables)):
            variable = self.__primary_variables[var_index]
            dims = self.__primary_variables_dims[var_index]
            attrs = self.__primary_variables_attrs[var_index]
            dtype = self.__primary_variables_typecodes[var_index]
            shape = self.__primary_variables_shapes[var_index]

            ts_ds = netCDF4.Dataset(f"{self.getOutputPathTemplate()}{variable}.{start_timestr}.{end_timestr}.nc", mode="w")
            ts_ds.setncatts(global_attrs)

            for dim_index, dim in enumerate(dims):
                if dim == "time":
                    ts_ds.createDimension(dim, None)
                else:
                    ts_ds.createDimension(dim, shape[dim_index])

            var_data = ts_ds.createVariable(variable, dtype, dims)
            ts_ds[variable].setncatts(attrs)
            # This is the chunk-writing loop, bulk of computation occurs here
            for i in range(hist_ds[variable].shape[0]):
                var_data[i] = hist_ds[variable][i]

            for aux_index, aux_variable in enumerate(self.__auxillary_variables):
                for dim_index, dim in enumerate(self.__auxillary_variables_dims[aux_index]):
                    if dim not in ts_ds.dimensions:
                        ts_ds.createDimension(dim, self.__auxillary_variables_shapes[aux_index][dim_index])
                aux_var_data = ts_ds.createVariable(aux_variable, self.__auxillary_variables_typecodes[aux_index], self.__auxillary_variables_dims[aux_index])
                ts_ds[aux_variable].setncatts(self.__auxillary_variables_attrs[aux_index])
                # This is the chunk-writing loop for axuillary variables. We can't necessarily assume time is the first index,
                # this might be a performance issue, but if auxillary variables are small I wouldn't expect much
                aux_var_data[:] = hist_ds[aux_variable][:]

            ts_ds.close()
        hist_ds.close()


class TimeSeriesConfig:
    def __init__(self, input_head_dir, output_head_dir, directory_name_swaps={}, timestep_directory_names={}, file_name_exclusions=[], directory_name_exclusions=[]):
        input_head_dir = Path(input_head_dir)
        output_head_dir = Path(output_head_dir)

        netcdf_paths = []
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
                netcdf_paths.append(path)

        parent_directories = {}
        for path in netcdf_paths:
            if path.parent in parent_directories:
                parent_directories[path.parent].append(path)
            else:
                parent_directories[path.parent] = [path]

        self.ts_output_groups = {}
        for parent in parent_directories:
            match_index = np.inf
            for path in parent_directories[parent][1:]:
                longest_match = SequenceMatcher(None, parent_directories[parent][0].name, path.name).find_longest_match()
                if longest_match.size < match_index and longest_match.a == 0:
                    match_index = longest_match.size

            prefix_groups = {}
            for path in parent_directories[parent]:
                prefix = path.name[:match_index] + path.name[match_index:].split(".")[0]
                if prefix.split(".")[-1] in timestep_directory_names:
                    prefix = timestep_directory_names[prefix.split(".")[-1]] + "/" + prefix + "."

                if prefix in prefix_groups:
                    prefix_groups[prefix].append(path)
                else:
                    prefix_groups[prefix] = [path]

            parent_output = str(output_head_dir) + str(parent).split(str(input_head_dir))[1]
            for keyword in directory_name_swaps:
                parent_output = parent_output.replace(f"/{keyword}", f"/{directory_name_swaps[keyword]}")

            self.ts_output_groups[parent_output] = prefix_groups

        self.ts_orders = []
        for parent_directory in self.ts_output_groups:
            for prefix in self.ts_output_groups[parent_directory]:
                self.ts_orders.append(TimeSeriesOrder(f"{parent_directory}/{prefix}", self.ts_output_groups[parent_directory][prefix]))

    def generateAllTimeseries(self):
        return [delayed(order.generateTimeseries)() for order in self.ts_orders]

    def generateAllTimeseriesNCO(self):
        def execute_ncrcat_cmd(cmd):
            return subprocess.run([cmd], shell=True)

        cmd_strs = []
        for order in self.ts_orders:
            for cmd_str in order.getCommandStrings():
                cmd_strs.append(cmd_str)
        return [delayed(execute_ncrcat_cmd)(cmd_str) for cmd in cmd_strs]