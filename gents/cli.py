#!/usr/bin/env python
"""
gents_cli

Command Line Interface for running GenTS in the terminal without a notebook.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 8/23/24
"""
import gents
import argparse
from dask.distributed import Client
from json import load
from importlib.metadata import version as getVersion


def main():
    parser = argparse.ArgumentParser(description = f"Command Line Interface (CLI) for Generating Time-Series (GenTS) Version {getVersion("gents")}")

    parser.add_argument("hf_head_dir", nargs = 1, metavar = "hf_head_dir", type = str, 
                         help = "Path to head directory with subdirectories containing history files.")
    parser.add_argument("ts_head_dir", nargs = 1, metavar = "ts_head_dir", type = str, 
                         help = "Path to head directory where structure reflecting `hf_head_dir` will be created and timeseries files will be written to.")
    parser.add_argument("-d", "--dask_address", nargs = "?", metavar = "dask_address", type = str, 
                         help = "Address to dask scheduler managing an active dask cluster.")
    parser.add_argument("-s", "--dir_name_swaps_path", nargs = "?", metavar = "dir_name_swaps_path", type = str, 
                         help = "Path to JSON containing dictionary for swapping out keyword directory names in the structure under `hf_head_dir` (e.g. ``{'hist' : 'proc/tseries'}``")
    parser.add_argument("-F", "--file_exclusions", nargs = '*', metavar = "file_exclusions", type = str, 
                         help = "File names containing any of the keywords in this list will be excluded from the database (Default: ['rest', 'logs']).")
    parser.add_argument("-D", "--dir_exclusions", nargs = '*', metavar = "dir_exclusions", type = str, 
                         help = "Directory names containing any of the keywords in this list will be excluded from the database (Default: ['rest', 'logs']).")
    parser.add_argument("-l", "--timeseries_year_length", nargs = "?", metavar = "timeseries_year_length", type = int, 
                         help = "Number of years each timeseries file should be chunked to using `getYearSlices` (Default: 10).")
    parser.add_argument("-i", "--include_variables", nargs = '*', metavar = "include_variables", type = str, 
                         help = "Variables to include in either creating individual timeseries files for adding as auxiliary variables (Default: None).")
    parser.add_argument("-e", "--exclude_variables", nargs = '*', metavar = "exclude_variables", type = str, 
                         help = "Variables to exclude from either creating individual timeseries files for adding as auxiliary variables (Default: None).")
    parser.add_argument("-n", "--year_start", nargs = "?", metavar = "year_start", type = int, 
                         help = "Starting year for timeseries generation, must be later than first history file timestamp to have an effect (Default: None).")
    parser.add_argument("-m", "--year_end", nargs = "?", metavar = "year_end", type = int, 
                         help = "Ending year for timeseries generation, must be later than last history file timestamp to have an effect (Default: None).")
    parser.add_argument("-C", "--compression_level", nargs = "?", metavar = "compression_level", type = int, 
                         help = "Compression level to pass to netCDF4 engine when generating timeseries files (Default: None).")
    parser.add_argument("-a", "--compression_algo", nargs = "?", metavar = "compression_algo", type = str, 
                         help = "Compression algorithm to pass to netCDF4 engine when generating timeseries files. See netCDF4-python documentation for available algorithms (Default: 'bzip2').")
    parser.add_argument("-c", "--variable_compression_levels_path", nargs = "?", metavar = "variable_compression_levels_path", type = str, 
                         help = "Path to JSON containing compression levels to apply to specific variables. Variable name is key and the compression level is the value (Default: None).")
    parser.add_argument("-v", "--verbosity_level", nargs = "?", metavar = "verbosity_level", type = int, 
                         help = "Level of logging to output to the standard output stream. 0 = No output, 1 = high level computational stages with timings, 2 = all stages and iterations with timings, including logic (Default: 1).")
    parser.add_argument("--overwrite", action='store_true', 
                         help = "Overwrite timeseries files if they already exist at the generated paths.")
    args = parser.parse_args()

    if args.hf_head_dir is not None:
        hf_head_dir = args.hf_head_dir[0]

    if args.ts_head_dir is not None:
        ts_head_dir = args.ts_head_dir[0]

    if args.dask_address is not None:
        client = Client(args.dask_address[0])
    else:
        client = None

    if args.dir_name_swaps_path is not None:
        with open(args.dir_name_swaps_path[0], "r") as f:
            variable_compression_levels = load(f)
    else:
        dir_name_swaps = {}

    if args.file_exclusions is not None:
        file_exclusions = args.file_exclusions
    else:
        file_exclusions = []

    if args.dir_exclusions is not None:
        dir_exclusions = args.dir_exclusions
    else:
        dir_exclusions = []

    if args.timeseries_year_length is not None:
        timeseries_year_length = args.timeseries_year_length[0]
    else:
        timeseries_year_length = 10

    if args.include_variables is not None:
        include_variables = args.include_variables
    else:
        include_variables = None

    if args.exclude_variables is not None:
        exclude_variables = args.exclude_variables
    else:
        exclude_variables = None

    if args.year_start is not None:
        year_start = args.year_start[0]
    else:
        year_start = None

    if args.year_end is not None:
        year_end = args.year_end[0]
    else:
        year_end = None

    if args.compression_level is not None:
        compression_level = args.compression_level[0]
    else:
        compression_level = None

    if args.compression_algo is not None:
        compression_algo = args.compression_algo[0]
    else:
        compression_algo = None

    if args.variable_compression_levels_path is not None:
        with open(args.variable_compression_levels_path[0], "r") as f:
            variable_compression_levels = load(f)
    else:
        variable_compression_levels = None

    if args.verbosity_level is not None:
        verbosity_level = args.verbosity_level[0]
    else:
        verbosity_level = 1

    if args.overwrite is not None:
        overwrite = args.overwrite
    else:
        overwrite = False


    modb = gents.ModelOutputDatabase(
        hf_head_dir=hf_head_dir,
        ts_head_dir=ts_head_dir,
        dir_name_swaps=dir_name_swaps,
        file_exclusions=file_exclusions,
        dir_exclusions=dir_exclusions,
        timeseries_year_length=timeseries_year_length,
        overwrite=overwrite,
        include_variables=include_variables,
        exclude_variables=exclude_variables,
        year_start=year_start,
        year_end=year_end,
        compression_level=compression_level,
        compression_algo=compression_algo,
        variable_compression_levels=variable_compression_levels,
        verbosity_level=verbosity_level
    )

    modb.run(client=client)


if __name__ == '__main__':
    main()