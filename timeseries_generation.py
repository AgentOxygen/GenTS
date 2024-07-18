import numpy as np
from pathlib import Path
import xarray
from dask.distributed import wait
from uuid import uuid4
from os.path import isfile, getsize
from os import listdir, remove
import warnings
from time import time
from difflib import SequenceMatcher
import netCDF4
import cftime


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
            self.output_orders = self.get_output_orders()

        self.ts_orders = []
        for parent_directory in self.ts_output_groups:
            for prefix in self.ts_output_groups[parent_directory]:
                self.ts_orders.append(TimeSeriesOrder(f"{parent_directory}/{prefix}", self.ts_output_groups[parent_directory][prefix]))


class TimeSeriesOrder:
    def __init__(self, output_path_template, history_file_paths):
        self.__output_path_template = output_path_template
        self.__history_files_paths = history_file_paths
        self.__mfdataset = netCDF4.MFDataset(history_file_paths, check=False, aggdim="time")
        self.__time = cftime.num2date(self.__mfdataset["time"][:], self.__mfdataset["time"].units)
        self.__time_path_indices = self.indexTimestampsToPaths(self.__history_files_paths, self.__mfdataset["time"][:])
        self.__time_step = self.getTimeStep()
        self.__time_str_format = self.getTimeStrFormat(self.__time_step)
        self.__history_file_groupings = []
        self.__ts_output_tuples = []
        self.__chunk_year_length = None
        self.generateOutputs()

    def getIndices(self):
        return self.__time_path_indices

    def indexTimestampsToPaths(self, paths, concat_times):
        dataset_times = [netCDF4.Dataset(path)["time"][:] for path in paths]
        index_to_path = []
        for index in range(len(concat_times)):
            path_index = None
            for times in dataset_times:
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

        variables = list(self.__mfdataset.variables)
        primary_variables = []
        auxillary_variables = []
        for var_index, target_variable in enumerate(variables):
            dim_coords = np.unique(list(self.__mfdataset[target_variable].dimensions))
            if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
                primary_variables.append(target_variable)
            else:
                auxillary_variables.append(target_variable)

        for start_time, end_time, paths in input_hist_file_tuples:
            for variable in primary_variables:
                self.__ts_output_tuples.append((auxillary_variables + [variable], self.__output_path_template, start_time, end_time, paths))

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


def generate_timeseries(client, output_template, hist_paths, overwrite=False):
    logs = []
    Path(output_template).parent.mkdir(parents=True, exist_ok=True)

    with warnings.catch_warnings(action="ignore"):
        history_concat = xarray.open_mfdataset(hist_paths, parallel=True, decode_cf=True, data_vars="minimal", chunks={}, combine='nested', concat_dim="time")

    dt = history_concat.time.values[1] - history_concat.time.values[0]
    if dt.days == 0:
        time_str_format = "%Y-%m-%d-%H"
    elif 30 > dt.days > 0:
        time_str_format = "%Y-%m-%d"
    else:
        time_str_format = "%Y-%m"

    time_start = history_concat.time.values[0].strftime(time_str_format)
    time_end = history_concat.time.values[-1].strftime(time_str_format)

    variables = list(history_concat.variables)
    primary_variables = []
    auxillary_variables = []
    for var_index, target_variable in enumerate(variables):
        dim_coords = np.unique(list(history_concat[target_variable].coords) + list(history_concat[target_variable].dims))
        if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
            primary_variables.append(target_variable)
        else:
            auxillary_variables.append(target_variable)

    variable_datasets = []
    variable_output_paths = []
    for variable in list(history_concat.variables):
        if variable not in auxillary_variables:
            output_path = f"{output_template}{variable}.{time_start}.{time_end}.nc"
            if not isfile(output_path) or overwrite:
                variable_datasets.append(history_concat[[variable]])
                variable_output_paths.append(output_path)
            else:
                logs.append("Skipping file because it already exists (assuming integrity checks were done already): ")
                logs.append(f"\t '{output_path}'")

    if len(variable_datasets) == 0:
        logs.append("Skipping group because all timeseries files already exists (assuming integrity checks were done already): ")
        logs.append(f"\t '{output_template}'")
        return logs

    def export_dataset(ds, output_path):
        ds.to_netcdf(output_path, mode="w")
        return output_path

    export_futures = client.map(export_dataset, variable_datasets, variable_output_paths)
    export_paths = client.gather(export_futures)

    aux_ds = history_concat[auxillary_variables].compute()

    if client.amm.running():
        client.amm.stop()
    scattered_aux = client.scatter(aux_ds, broadcast=True)

    def add_auxillary_variables(output_path, ds):
        ds.to_netcdf(output_path, mode="a")
        return output_path

    futures = client.map(add_auxillary_variables, export_paths, ds=aux_ds)
    appended_paths = client.gather(futures)

    scattered_aux.release()
    client.amm.start()

    return logs


def generate_timeseries_serial(history_file_paths, output_template, overwrite=False):
    history_datasets = np.empty(len(history_file_paths), dtype=xarray.Dataset)
    for hist_index in range(len(history_file_paths)):
        history_datasets[hist_index] = xarray.open_dataset(history_file_paths[hist_index], chunks={})

    concat_ds = xarray.concat(history_datasets, dim="time", data_vars="minimal", coords="minimal")

    variables = list(history_datasets[0].variables)
    primary_variables = []
    auxillary_variables = []
    for var_index, target_variable in enumerate(variables):
        dim_coords = np.unique(list(history_datasets[0][target_variable].coords) + list(history_datasets[0][target_variable].dims))
        if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
            primary_variables.append(target_variable)
        else:
            auxillary_variables.append(target_variable)

    dt = concat_ds.time.values[1] - concat_ds.time.values[0]
    if dt.days == 0:
        time_str_format = "%Y-%m-%d-%H"
    elif 30 > dt.days > 0:
        time_str_format = "%Y-%m-%d"
    else:
        time_str_format = "%Y-%m"

    time_start = concat_ds.time.values[0].strftime(time_str_format)
    time_end = concat_ds.time.values[-1].strftime(time_str_format)

    aux_dataset = concat_ds[auxillary_variables]

    output_paths = []
    for variable in (primary_variables):
        output_path = f"{output_template}{variable}.{time_start}.{time_end}.nc"
        if isfile(output_path) and overwrite:
            remove(output_path)
        elif isfile(output_path):
            continue

        output_paths.append(output_path)

        output_ts = xarray.merge([aux_dataset, concat_ds[[variable]]])
        output_ts.to_netcdf(output_path)

    return output_paths


def generate_timeseries_batches(client, batches, verbose=False, overwrite=False):
    for index, (output_template, paths) in enumerate(batches):
        print(f"\nGenerating timeseries datasets for '{output_template}'", end="")
        start = time()
        logs = generate_timeseries(client, output_template, paths, overwrite=overwrite)
        print(f" ... done! {round(time() - start, 2)}s ({index+1}/{len(batches)})")
        if verbose:
            print(f"\t[Verbose=True, {len(logs)} log messages]")
            for log in logs:
                print(f"\t{log}")


def check_batch_integrity(batches):
    for output_dir in np.unique([batch[0] for batch in batches]):
        print(f"Attempting to read datasets in '{output_dir}'... ")
        failed_paths = []
        size = 0
        for file_name in listdir(f"{output_dir}/"):
            if ".nc" in file_name:
                try:
                    ds = xarray.open_dataset(f"{output_dir}/{file_name}")
                    size += ds.nbytes / 1024**3
                except ValueError:
                    failed_paths.append(f"{output_dir}/{file_name}")
        print(f"\tnetCDF files found: {len(listdir(output_dir))} [{round(size, 2)} GB]")
        print(f"\tFailed to open: {len(failed_paths)}")
        for path in failed_paths:
            print(f"\t\t{path}")
