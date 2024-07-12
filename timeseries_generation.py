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
            self.output_orders = self.get_output_orders()

    def get_output_orders(self):
        templates = []
        for parent_directory in self.ts_output_groups:
            for prefix in self.ts_output_groups[parent_directory]:
                templates.append((f"{parent_directory}/{prefix}", self.ts_output_groups[parent_directory][prefix]))

        return templates

    def create_order_batches(self):
        orders = []
        for index, (template, paths) in enumerate(self.output_orders):
            num_output_files = len(xarray.open_dataset(paths[0], decode_cf=False).variables)
            estimated_total_file_size = sum([getsize(path) for path in paths])

            orders.append((index, len(paths), num_output_files, int(estimated_total_file_size / (10*1024**3))))

        order_specs = np.array(orders, dtype=[('order_index', int), ('num_inputs', int), ('num_outputs', int), ('est_total_size', int)])
        order_specs = np.sort(order_specs, order=["num_outputs", "est_total_size"])

        unique_output_sizes = np.unique([order[2] for order in order_specs])
        batches = {}
        for size in unique_output_sizes:
            for order_spec in order_specs:
                if order_spec[2] == size:
                    if size in batches:
                        batches[size].append(self.output_orders[order_spec[0]])
                    else:
                        batches[size] = [self.output_orders[order_spec[0]]]

        return batches


def generate_timeseries(client, output_template, batch_paths, overwrite=False):
    logs = []
    Path(output_template).parent.mkdir(parents=True, exist_ok=True)

    with warnings.catch_warnings(action="ignore"):
        history_concat = xarray.open_mfdataset(batch_paths,
                                               parallel=True,
                                               decode_cf=True,
                                               data_vars="minimal",
                                               combine='nested',
                                               concat_dim="time",
                                               compat="override",
                                               coords="minimal")

    dt = history_concat.time.values[1] - history_concat.time.values[0]
    if dt.days == 0:
        time_str_format = "%Y-%m-%d-%H"
    elif 30 > dt.days > 0:
        time_str_format = "%Y-%m-%d"
    else:
        time_str_format = "%Y-%m"

    time_start = history_concat.time.values[0].strftime(time_str_format)
    time_end = history_concat.time.values[-1].strftime(time_str_format)

    ds_variables = list(history_concat.variables)
    output_variables = []
    auxillary_variables = []
    for target_variable in ds_variables:
        dim_coords = np.unique(list(history_concat[target_variable].coords) + list(history_concat[target_variable].dims))
        if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
            output_variables.append(target_variable)
        else:
            auxillary_variables.append(target_variable)

    config_tuples = []
    for variable in output_variables:
        output_path = f"{output_template}{variable}.{time_start}.{time_end}.nc"
        if not isfile(output_path) or overwrite:
            config_tuples.append((
                history_concat[[variable]],
                output_path
            ))
        else:
            logs.append("Skipping file because it already exists (assuming integrity checks were done already): ")
            logs.append(f"\t '{output_path}'")

    if len(config_tuples) == 0:
        logs.append("Skipping group because all timeseries files already exists (assuming integrity checks were done already): ")
        logs.append(f"\t '{output_template}'")
        return logs

    def write_base_timeseries(config_tuple):
        ds, output_path = config_tuple
        ds.to_netcdf(output_path, mode="w")
        return output_path

    base_ts_futures = client.map(write_base_timeseries, config_tuples)

    aux_ds = history_concat[auxillary_variables].compute()

    base_ts_paths = []
    for future in base_ts_futures:
        base_ts_paths.append(future.result())
        future.release()

    if client.amm.running():
        client.amm.stop()
    scattered_aux = client.scatter(aux_ds, broadcast=True)

    def add_auxillary_variables(path, ds):
        ds.to_netcdf(path, mode="a")
        return path

    add_aux_futures = client.map(add_auxillary_variables, base_ts_paths, ds=scattered_aux)

    add_aux_paths = []
    for future in add_aux_futures:
        add_aux_paths.append(future.result())
        future.release()

    scattered_aux.release()
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


def unstable_generate_timeseries(client, history_paths, output_template, overwrite=False):
    # Unstable because if you don't chunk the reads, its super fast but blows up memory
    # If you do chunk, you lose the advantage of the serial per-worker open dataset calls because the latency is so high (I think)
    # And sometimes it will just hang for no reason :)
    Path(output_template).parent.mkdir(parents=True, exist_ok=True)
    init_ds = client.submit(xarray.open_dataset, history_paths[0], chunks={}, decode_cf=False).result()
    ds_variables = list(init_ds.variables)

    output_variables = []
    output_variables_drops = []
    auxillary_variables = []
    auxillary_variables_drops = []

    for target_variable in ds_variables:
        dim_coords = np.unique(list(init_ds[target_variable].coords) + list(init_ds[target_variable].dims))
        if len(dim_coords) > 1 and "time" in dim_coords and "nbnd" not in dim_coords and "chars" not in dim_coords:
            output_variables.append(target_variable)
            output_variables_drops.append([variable for variable in ds_variables if variable not in dim_coords and variable != target_variable])
        else:
            auxillary_variables.append(target_variable)
            auxillary_variables_drops.append([variable for variable in ds_variables if variable not in dim_coords and variable != target_variable])

    def export_base_timeseries(variable, variables_to_drop, paths, output_template, overwrite=False):
        ds = xarray.concat([xarray.open_dataset(path)[[variable]] for path in paths], dim="time", data_vars="minimal") #load vs dont load chunks here
        dt = ds.time.values[1] - ds.time.values[0]
        if dt.days == 0:
            time_str_format = "%Y-%m-%d-%H"
        elif 30 > dt.days > 0:
            time_str_format = "%Y-%m-%d"
        else:
            time_str_format = "%Y-%m"

        time_start = ds.time.values[0].strftime(time_str_format)
        time_end = ds.time.values[-1].strftime(time_str_format)
        output_path = f"{output_template}{variable}.{time_start}.{time_end}.nc"
        if not isfile(output_path):
            ds.to_netcdf(output_path, mode="w", unlimited_dims=["time"])
            return output_path
        elif overwrite:
            remove(output_path)
            ds.to_netcdf(output_path, mode="w", unlimited_dims=["time"])
            return output_path

        return None

    export_futures = client.map(export_base_timeseries,
                                output_variables,
                                output_variables_drops,
                                paths=history_paths,
                                output_template=output_template,
                                overwrite=overwrite)
    export_paths = []
    for future in export_futures:
        path = future.result()
        if path is not None:
            export_paths.append(path)

    def get_aux_ds(variable, paths):
        ds = xarray.concat([xarray.open_dataset(path)[[variable]] for path in paths], dim="time", data_vars="minimal")
        return ds

    aux_futures = client.map(get_aux_ds, auxillary_variables, paths=history_paths)
    aux_datasets = []
    for future in aux_futures:
        aux_datasets.append(future.result())
    aux_ds = xarray.merge(aux_datasets)

    scattered_aux = client.scatter(aux_ds, broadcast=True)

    def append_aux_variables(path, ds):
        ds.to_netcdf(path, mode="a")
        return path

    aux_futures = client.map(append_aux_variables, export_paths, ds=scattered_aux)
    appended_datasets_paths = []
    for future in aux_futures:
        appended_datasets_paths.append(future.result())

    scattered_aux.release()

    return appended_datasets_paths