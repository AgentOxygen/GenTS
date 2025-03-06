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
from gents.read import get_groups_from_path, generate_output_template
from gents.timeseries import generate_time_series


def generate_time_series_from_directory(input_head_dir, output_head_dir, complevel=0, compression=None, overwrite=True)
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()

    group_metas = get_groups_from_path(head_dir, dask_client=client)
    
    dask_gents = []
    for group_id in list(group_metas.keys()):
        template_path = generate_output_template(head_dir, group_id, output_head_dir=output_head_dir)
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