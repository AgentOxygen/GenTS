import argparse
import sys
from gents.utils import get_version

def parse_arguments():
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
        "-s", "--serial",
        action="store_true",
        help="Disables Dask parallelism."
    )
    parser.add_argument(
        "-nc", "--numcores",
        type=int,
        default=8,
        help="Numer of cores to use if running in parallel. (Default 8)"
    )
    parser.add_argument(
        "-ml", "--memorylimit",
        type=int,
        default=2,
        help="Parallel per-core memory limit in GB. (Default 2)"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.outputdir is None:
        args.outputdir = args.hf_head_dir
    
    if args.verbose:
        print(f"  Input (HF) directory path    : {args.hf_head_dir}")
        print(f"  Output (TS) directory path   : {args.outputdir}")
        print(f"  Model Configuration          : CESM3")
        print(f"  Overwrite TS Files           : {args.overwrite}")
        print(f"  Slice size                   : {args.slice}")
        print(f"  Dry run                      : {args.dryrun}")
        print(f"  Disable Dask                 : {args.serial}")
        if not args.serial:
            print(f"  # Cores (Dask workers)       : {args.numcores}")
            print(f"  Per-core Memory              : {args.memorylimit}")
    
    from gents.configs.gents_cesm3 import run_config

    run_config(args)