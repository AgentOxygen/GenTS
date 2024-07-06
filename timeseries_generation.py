import numpy as np
from pathlib import Path
import xarray
from dask.distributed import wait
from uuid import uuid4
from shutil import rmtree
from os.path import isfile
import warnings


class GenerationConfig:
    def __build_groups(self, hist_dir_path, date_index, delimiter="."):
        paths = [path for path in hist_dir_path.iterdir()]
        files = [path.name for path in hist_dir_path.iterdir()]
        files_parsed = np.array([file.split(delimiter) for file in files])

        if date_index < 0:
            date_index = files_parsed.shape[1] + date_index

        prefix = ""
        sequence_identifiers = []
        for sequence_index in range(files_parsed.shape[1]):
            uniques = np.unique(files_parsed[:, sequence_index])
            if sequence_index != date_index and uniques.size > 1:
                sequence_identifiers.append((sequence_index, uniques))
            elif uniques.size == 1:
                if uniques[0] != "nc":
                    prefix += uniques[0] + delimiter

        groups = {}
        for index in range(len(files)):
            group_identifier = prefix
            for sequence_index, identifier in sequence_identifiers:
                group_identifier += files_parsed[index][sequence_index]
            if group_identifier in groups:
                groups[group_identifier].append(paths[index])
            else:
                groups[group_identifier] = [paths[index]]

        return groups

    def __init__(self, case_dir_paths, output_timeseries_path):
        self.input_case_dir_paths = [Path(str(path)) for path in case_dir_paths]
        for case_path in self.input_case_dir_paths:
            assert case_path.exists()
            assert not case_path.is_file()

        self.output_dir_path = Path(str(output_timeseries_path))
        assert self.output_dir_path.exists()
        assert not self.output_dir_path.is_file()

        self.output_timeseries_path = output_timeseries_path
        self.case_names = [path.name for path in self.input_case_dir_paths]
        self.possible_components = [
            "atm", "ocn", "lnd", "esp", "glc", "rof", "wav", "ice"
        ]
        self.history_dir_name = "hist"

        self.case_comp_hist_dir_paths = {}
        for case in self.input_case_dir_paths:
            comp_paths = {}
            for comp_path in case.iterdir():
                if comp_path.name in self.possible_components:
                    for sub_directory in comp_path.iterdir():
                        if sub_directory.name == self.history_dir_name:
                            comp_paths[sub_directory] = self.__build_groups(sub_directory, date_index=-2)
                            break
            self.case_comp_hist_dir_paths[case] = comp_paths

        print("Sampling dataset metadata from each case group to estimate total size in memory (this may take some time)...")
        self.group_nbytes = {}
        for case_dir in self.case_comp_hist_dir_paths:
            self.group_nbytes[case_dir] = {}
            for component_dir in self.case_comp_hist_dir_paths[case_dir]:
                self.group_nbytes[case_dir][component_dir] = {}
                for group in self.case_comp_hist_dir_paths[case_dir][component_dir]:
                    paths = self.case_comp_hist_dir_paths[case_dir][component_dir][group]
                    sample_ds = xarray.open_dataset(paths[0], chunks=dict(time=-1))
                    self.group_nbytes[case_dir][component_dir][group] = int(sample_ds.nbytes) * len(paths)
                    del sample_ds

    def fit_interm_timeseries_to_memory(self, memory_per_node_gb=150):
        interm_sizes = {}
        for case_dir in self.group_nbytes:
            interm_sizes[case_dir] = {}
            for component_dir in self.group_nbytes[case_dir]:
                interm_sizes[case_dir][component_dir] = {}
                for group in self.group_nbytes[case_dir][component_dir]:
                    total_size = (self.group_nbytes[case_dir][component_dir][group] / 1024**3)
                    num_files = len(self.case_comp_hist_dir_paths[case_dir][component_dir][group])
                    interm_sizes[case_dir][component_dir][group] = int(min(num_files / (total_size / memory_per_node_gb), num_files))
        return interm_sizes

    def get_timeseries_batches(self, interm_sizes):
        batches = []
        for case_dir in self.case_comp_hist_dir_paths:
            for component_dir in self.case_comp_hist_dir_paths[case_dir]:
                for group in self.case_comp_hist_dir_paths[case_dir][component_dir]:
                    output_dir = str(self.output_timeseries_path) + "/" + case_dir.name + str(component_dir).split(str(case_dir))[1]
                    output_dir = Path(output_dir.replace(self.history_dir_name, "tseries"))
                    file_paths = np.array(self.case_comp_hist_dir_paths[case_dir][component_dir][group])
                    interm_size = interm_sizes[case_dir][component_dir][group]
                    for batch_paths in np.array_split(file_paths, np.ceil(file_paths.size / interm_size)):
                        batches.append((output_dir, group, batch_paths))

        batches.sort(key=lambda entry: len(entry[2]))
        return batches


def generate_timeseries(client, output_dir, group, batch_paths):
    logs = []
    output_dir.mkdir(parents=True, exist_ok=True)
    history_zarr_path = f"{output_dir}/tmp_hs_store.zarr"

    with warnings.catch_warnings(action="ignore"):
        history_concat = xarray.open_mfdataset(batch_paths, parallel=True, decode_cf=True)

    dt = history_concat.time.values[1] - history_concat.time.values[0]
    if dt.days == 0:
        time_str_format = "%Y-%m-%d-%H"
    elif 30 > dt.days > 0:
        time_str_format = "%Y-%m-%d"
    else:
        time_str_format = "%Y-%m"

    time_start = history_concat.time.values[0].strftime(time_str_format)
    time_end = history_concat.time.values[-1].strftime(time_str_format)

    attribute_variables = []
    for variable in list(history_concat.variables):
        if "cell_methods" not in history_concat[variable].attrs:
            attribute_variables.append(variable)

    config_tuples = []
    for variable in list(history_concat.variables):
        if variable not in attribute_variables:
            output_path = f"{output_dir}/{group}.{variable}.{time_start}.{time_end}.nc"
            if not isfile(output_path):
                config_tuples.append((
                    history_zarr_path,
                    [variable],
                    output_path,
                    uuid4())
                )
            else:
                logs.append("Skipping file because it already exists (assuming integrity checks were done already): ")
                logs.append(f"\t '{output_path}'")

    if len(config_tuples) == 0:
        logs.append("Skipping group because all timeseries files already exists (assuming integrity checks were done already): ")
        logs.append(f"\t '{group}'")
        return logs

    target_chunk_size = 250*(1024**2)
    for variable in history_concat:
        time_chunk_size = 1
        if "time" in history_concat[variable].dims:
            time_size = 1
            for index, dim in enumerate(history_concat[variable].dims):
                if dim == "time":
                    time_size = history_concat[variable].shape[index]
            smallest_time_chunk = history_concat[variable].nbytes / time_size
            if smallest_time_chunk <= 2*target_chunk_size:
                time_chunk_size = int(target_chunk_size / smallest_time_chunk)
            history_concat[variable] = history_concat[variable].chunk(dict(time=time_chunk_size))
    with warnings.catch_warnings(action="ignore"):
        history_concat.to_zarr(history_zarr_path, mode="w", consolidated=True)

    def export_dataset(config_tuple):
        zarr_path, variables, output_path, uid = config_tuple
        xarray.open_zarr(zarr_path)[variables].to_netcdf(output_path, mode="w")
        return uid

    futures = client.map(export_dataset, config_tuples)

    attrs_ds = xarray.open_zarr(history_zarr_path)[attribute_variables].compute()

    for task in futures:
        wait(task)
        task.release()
    client.amm.stop()
    scatted_attrs = client.scatter(attrs_ds, broadcast=True)

    def add_descriptive_variables(path_ds_tuple):
        ds, path = path_ds_tuple
        ds.to_netcdf(path, mode="a")

    path_tuples = [(scatted_attrs, f"{output_dir}/{group}.{variable}.{time_start}.{time_end}.nc") for variable in list(history_concat.variables) if variable not in attribute_variables]
    futures = client.map(add_descriptive_variables, path_tuples)

    for task in futures:
        wait(task)
        task.release()
    scatted_attrs.release()

    rmtree(history_zarr_path)
    client.amm.start()
    return logs


def generate_timeseries_batches(client, batches, verbose=False):
    for output_dir, group, batch_paths in batches:
        print(f"\nGenerating timeseries datasets for '{group}'")
        logs = generate_timeseries(client, output_dir, group, batch_paths)
        print(" ... done!")
        if verbose:
            print("[Verbose=True, Logs]")
            for log in logs:
                print(f"\t{log}")