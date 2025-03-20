#!/usr/bin/env python
"""
gents.py

Python package for generating time-series datasets from history files
efficiently by leveraging the netCDF4 engine and Dask.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 3/5/24
"""
import dask
from gents.read import *
from gents.timeseries import generate_time_series


def generate_time_series_from_meta_groups(group_metas, input_head_dir, output_head_dir, complevel=0, compression=None, overwrite=True, dask_client=None):
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()

    dask_gents = []
    for group_id in list(group_metas.keys()):
        template_path = generate_output_template(input_head_dir, group_id, output_head_dir=output_head_dir)
        group_hf_paths = [meta_ds.get_path() for meta_ds in group_metas[group_id]]
        delayed_func = dask.delayed(generate_time_series)(
            group_hf_paths,
            template_path.parent,
            template_path.name,
            complevel,
            compression,
            overwrite
        )
        dask_gents.append(delayed_func)
    
    ts_paths = dask.compute(*dask_gents)


def generate_time_series_from_directory(input_head_dir, output_head_dir, gents_config=None, complevel=0, compression=None, overwrite=True, dask_client=None, slice_size_years=10):
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()

    if gents_config is None:
        gents_config = get_default_config()
    
    paths = find_files(input_head_dir, "*.nc")

    if gents_config["exclude"] is not None:
        # path_to_meta_map = apply_exclusive_filters(path_to_meta_map, custom_filter["exclude"])
        # Applying exclusive filters using just path for now. We can implement more advanced filters later
        # This will save time on the metadata reads
        print(len(paths))
        filtered_paths = []
        for path in paths:
            exclude = False
            for tag in gents_config["exclude"]:
                if tag in str(path):
                    exclude = True
                    break
            if not exclude:
                filtered_paths.append(path) 
        paths = filtered_paths
        print(len(paths))

    check_config(gents_config)
    path_to_meta_map = get_metas_from_paths(paths, dask_client=dask_client)
    if gents_config["include"] is not None:
        path_to_meta_map = apply_inclusive_filters(path_to_meta_map, custom_filter["include"])

    groups = sort_hf_groups(list(path_to_meta_map.keys()))
    sliced_groups = slice_hf_groups(groups, path_to_meta_map, slice_size_years)
    group_metas = check_groups_by_variables(sliced_groups)
    
    generate_time_series_from_meta_groups(group_metas, input_head_dir, output_head_dir, complevel, compression, overwrite, dask_client)