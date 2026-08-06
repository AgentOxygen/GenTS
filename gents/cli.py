#!/usr/bin/env python
"""
``run_gents`` -- command line driver for the history file to time series pipeline.
"""
import argparse
import sys
import yaml
from gents.utils import get_version, log_hfcollection_info, log_tscollection_info, enable_logging
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection, DEFAULT_MEMORY_LIMIT_BYTES
from pathlib import Path


def check_config(config_dict):
    """
    Asserts that a model YAML config has the required top-level keys.

    :param config_dict: Parsed contents of a ``gents/configs/*.yaml`` file.
    :type config_dict: dict
    :raises AssertionError: If any required key is missing.
    """
    assert "version" in config_dict
    assert "model" in config_dict
    assert "input_hf" in config_dict
    assert "output_ts" in config_dict


def parse_arguments():
    """
    Parses ``run_gents`` command line arguments.

    Run ``run_gents --help`` for the full list; each flag's help text below is
    its documentation.

    :returns: Namespace populated with the parsed argument values.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="GenTS "
    )
    parser.add_argument(
        "hf_head_dir",
        type=str,
        help="Path to head directory for history files."
    )
    parser.add_argument(
        "-o", "--outputdir",
        type=str,
        help="Path to the output time-series directory structure and files to. (Default is hf_head_dir)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output."
    )
    parser.add_argument(
        "-V", "--version",
        action="version",
        version=f"%(prog)s {get_version()}"
    )
    parser.add_argument(
        "-d", "--dryrun",
        action="store_true",
        help="Reads and interprets history file metadata but does not generate time series files."
    )
    parser.add_argument(
        "-nd", "--no-data",
        dest="no_data",
        action="store_true",
        help="Generate the full time series structure but skip reading/writing primary "
             "variable data (primaries read back as their fill value). Used for fast "
             "conformity testing over missing-value case clones."
    )
    parser.add_argument(
        "-w", "--overwrite",
        action="store_true",
        help="Overwrite existing time series files if they exist."
    )
    parser.add_argument(
        "-sl", "--slice",
        type=int,
        default=10,
        help="Maximum length of individual time series files in years. (Default 10)"
    )
    parser.add_argument(
        "--slice_start_year",
        type=int,
        default=None,
        help="Year to start slice windows at. (Default is start year for history files)"
    )
    parser.add_argument(
        "-hc", "--hfcores",
        type=int,
        default=64,
        help="Maximum number of cores to use for metadata-reads if running in parallel. (Default 64)"
    )
    parser.add_argument(
        "-tc", "--tscores",
        type=int,
        default=8,
        help="Maximum number of cores to use for writing timeseries if running in parallel. (Default 8)"
    )
    parser.add_argument(
        "--memory-limit",
        dest="memory_limit_gb",
        type=float,
        default=None,
        help="Maximum memory (in GB) MHFDataset may use to cache variable data per worker "
             "while generating time series. (Default: 4 GB per worker)"
    )
    parser.add_argument(
        "-m", "--model",
        type=str,
        default=None,
        help="Specify a model default GenTS configuration to use: 'CESM3', 'CESM2', 'E3SM'. (Default None)"
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Pattern to exclude (can be specified multiple times). Overrides default unless '--append' used."
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Pattern to include (can be specified multiple times). Overrides default unless '--append' used."
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append arguments to base configuration instead of overwrite."
    )
    parser.add_argument(
        "--align_method",
        type=str,
        default="midpoint",
        help="Method to use when aligning the history files by time. ('midpoint', 'direct_time', 'start_bound', 'end_bound')"
    )
    parser.add_argument(
        "--compression",
        type=str,
        default=None,
        help="Compression algorithm to use. ('zlib', 'szip', 'zstd', 'bzip2', 'blosc_lz', "
             "'blosc_lz4', 'blosc_lz4hc', 'blosc_zlib', 'blosc_zstd')"
    )
    parser.add_argument(
        "--level",
        type=int,
        default=None,
        help="Compression level to use. (0-9, default 4; ignored if --compression is not set)"
    )
    return parser.parse_args()


def main():
    """
    Entry point for ``run_gents``.

    Loads the YAML config for ``--model`` from ``gents/configs/``, builds an
    :class:`~gents.hfcollection.HFCollection` and
    :class:`~gents.timeseries.TSCollection` from it (with command line flags
    replacing the config's filters and slicing, or extending them under
    ``--append``), and executes the result unless ``--dryrun`` was given.

    :raises ValueError: If ``--model`` names an unknown model, or ``--compression``
        is given without ``--level``.
    """
    args = parse_arguments()

    if args.outputdir is None:
        args.outputdir = args.hf_head_dir
    
    if args.model is not None:
        args.model = args.model.lower()

    if args.compression is not None and args.level is None:
        raise ValueError(f"Compression '{args.compression}' selected, please specifiy a level using `--level`")

    memory_limit_bytes = args.memory_limit_gb * (1024**3) if args.memory_limit_gb is not None else DEFAULT_MEMORY_LIMIT_BYTES

    if args.verbose:
        print(f"  Input (HF) directory path    : {args.hf_head_dir}")
        print(f"  Output (TS) directory path   : {args.outputdir}")
        print(f"  Model Configuration          : {args.model}")
        print(f"  Overwrite TS Files           : {args.overwrite}")
        print(f"  Slice size                   : {args.slice}")
        print(f"  Dry run                      : {args.dryrun}")
        print(f"  Skip primary data (no-data)  : {args.no_data}")
        print(f"  Number of HF processes (cores)  : {args.hfcores}")
        print(f"  Number of TS processes (cores)  : {args.tscores}")
        print(f"  Include filters                 : {args.include}")
        print(f"  Exclude filters                 : {args.exclude}")
        print(f"  Append filters to defaults      : {args.append}")
        print(f"  Time alignment method           : {args.align_method}")
        print(f"  Slice start year                : {args.slice_start_year}")
        print(f"  Compression method              : {args.compression}")
        print(f"  Compression level               : {args.level}")
        print(f"  Memory limit (GB)               : {args.memory_limit_gb}")
        enable_logging(verbose=True)

    config_dir = Path(__file__).parent / "configs"
    model_config_files = {
        None: "gents_example.yaml",
        "cesm3": "gents_cesm3.yaml",
        "cesm2": "gents_cesm2.yaml",
        "e3sm": "gents_e3sm.yaml",
    }

    if args.model not in model_config_files:
        raise ValueError(f"No GenTS configuration available for model '{args.model}'.")

    with open(str(config_dir / model_config_files[args.model]), 'r') as file:
        yaml_config = yaml.safe_load(file)

    check_config(yaml_config)

    hfc = HFCollection(args.hf_head_dir, hf_glob_pattern=yaml_config["input_hf"]["match"], num_processes=args.hfcores)
    if "include" in yaml_config["input_hf"]:
        hf_include = yaml_config["input_hf"]["include"]
    else:
        hf_include = []

    if "exclude" in yaml_config["input_hf"]:
        hf_exclude = yaml_config["input_hf"]["exclude"]
    else:
        hf_exclude = []

    if args.append:
        hf_include += args.include
        hf_exclude += args.exclude
    else:
        if len(args.include) > 0:
            hf_include = args.include
        if len(args.exclude) > 0:
            hf_exclude = args.exclude

    hfc = hfc.include(hf_include).exclude(hf_exclude)

    if "slicing" in yaml_config["input_hf"]:
        slice_batches = yaml_config["input_hf"]["slicing"]
    else:
        slice_batches = []
    
    if args.append:
        slice_batches.append({
            "slice_size_years": args.slice,
            "start_year": args.slice_start_year
        })
    else:
        slice_batches = [{
            "slice_size_years": args.slice,
            "start_year": args.slice_start_year
        }]

    for slice_batch in slice_batches:
        if args.slice_start_year is not None:
            slice_batch["start_year"] = args.slice_start_year
        hfc = hfc.slice_groups(**slice_batch)

    tsc = TSCollection(hfc, args.outputdir, num_processes=args.tscores)

    if args.compression is not None:
        tsc = tsc.apply_compression(alg=args.compression, level=args.level, path_glob="*")

    if "path_swaps" in yaml_config["output_ts"]:
        for swap_batch in yaml_config["output_ts"]["path_swaps"]:
            tsc = tsc.apply_path_swap(**swap_batch)

    if "compression" in yaml_config["output_ts"]:
        for comp_batch in yaml_config["output_ts"]["compression"]:
            tsc = tsc.apply_compression(**comp_batch)

    if args.verbose:
        log_hfcollection_info(hfc)
        log_tscollection_info(tsc)

    tsc = tsc.add_attrs({"gents_command": " ".join(sys.argv)})

    if not args.dryrun:
        tsc.execute(no_data=args.no_data, memory_limit_bytes=memory_limit_bytes)
    else:
        print(f"Dry run: {len(tsc)} timeseries files would be generated.")
    print("GenTS done!")