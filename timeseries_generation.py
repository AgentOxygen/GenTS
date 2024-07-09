import numpy as np
from pathlib import Path
import xarray
from dask.distributed import wait
from uuid import uuid4
from os.path import isfile
from os import listdir
import warnings
from time import time
from difflib import SequenceMatcher


class TSGenerationConfig:
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

    def get_output_templates(self):
        templates = []
        for parent_directory in self.ts_output_groups:
            for prefix in self.ts_output_groups[parent_directory]:
                templates.append((f"{parent_directory}/{prefix}", self.ts_output_groups[parent_directory][prefix]))

        return templates


def generate_timeseries(client, output_template, batch_paths, overwrite=False):
    logs = []
    Path(output_template).parent.mkdir(parents=True, exist_ok=True)

    with warnings.catch_warnings(action="ignore"):
        history_concat = xarray.open_mfdataset(batch_paths, parallel=True, decode_cf=True, data_vars="minimal", chunks={}, combine='nested', concat_dim="time")

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
            output_path = f"{output_template}{variable}.{time_start}.{time_end}.nc"
            if not isfile(output_path) or overwrite:
                config_tuples.append((
                    history_concat[[variable]],
                    output_path,
                    uuid4())
                )
            else:
                logs.append("Skipping file because it already exists (assuming integrity checks were done already): ")
                logs.append(f"\t '{output_path}'")

    if len(config_tuples) == 0:
        logs.append("Skipping group because all timeseries files already exists (assuming integrity checks were done already): ")
        logs.append(f"\t '{output_template}'")
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

    def export_dataset(config_tuple):
        ds, output_path, uid = config_tuple
        ds.to_netcdf(output_path, mode="w")
        return uid

    futures = client.map(export_dataset, config_tuples)

    attrs_ds = history_concat[attribute_variables].compute()

    for task in futures:
        wait(task)
        task.release()

    if client.amm.running():
        client.amm.stop()
    scatted_attrs = client.scatter(attrs_ds, broadcast=True)

    def add_descriptive_variables(path_ds_tuple):
        ds, path = path_ds_tuple
        ds.to_netcdf(path, mode="a")

    path_tuples = [(scatted_attrs, f"{output_template}{variable}.{time_start}.{time_end}.nc") for variable in list(history_concat.variables) if variable not in attribute_variables]
    futures = client.map(add_descriptive_variables, path_tuples)

    for task in futures:
        wait(task)
        task.release()

    scatted_attrs.release()
    client.amm.start()

    return logs


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