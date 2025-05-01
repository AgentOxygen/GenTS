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
from gents.timeseries import generate_time_series
from gents.hfcollection import HFCollection
from os import makedirs
from pathlib import Path


def generate_ts_from_hfcollection(hf_collection: HFCollection,
                                  output_dir: str,
                                  complevel=0,
                                  compression=None,
                                  overwrite=False,
                                  dir_swaps={"/hist/":"/t_series/"},
                                  dask_client=None
                                 ) -> list:
    if dask_client is None:
        dask_client = dask.distributed.client._get_global_client()

    dask_gents = []
    hf_groups = hf_collection.slice_groups()
    for glob_template in hf_groups:
        hf_paths = hf_groups[glob_template]
        
        output_template = glob_template.split(hf_collection.get_input_dir())[1]
        ts_path_template = f"{output_dir}{output_template}"
        for key in dir_swaps:
            ts_path_template = ts_path_template.replace(key, dir_swaps[key])
        ts_path_template = ts_path_template.split("*")[0]

        makedirs(Path(ts_path_template).parent, exist_ok=True)
        
        primary_vars = hf_collection[hf_paths[0]].get_primary_variables()
        secondary_vars = hf_collection[hf_paths[0]].get_secondary_variables()
        for variable in primary_vars:
            delayed_func = dask.delayed(generate_time_series)(
                hf_paths,
                ts_path_template,
                [variable],
                secondary_vars,
                complevel,
                compression,
                overwrite
            )
            dask_gents.append(delayed_func)
    return dask_client.compute(dask_gents, sync=True)