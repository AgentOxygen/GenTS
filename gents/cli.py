import argparse
import sys
from gents.utils import get_version, log_hfcollection_info, log_tscollection_info, enable_logging

def parse_arguments():
    """
    Parses command-line arguments for the ``gents`` CLI entry point.

    Constructs an :class:`argparse.ArgumentParser` with all supported flags and
    positional arguments, then parses ``sys.argv`` and returns the resulting
    namespace.

    Supported arguments:

    - ``hf_head_dir`` *(positional)*: Path to the head directory containing history files.
    - ``-o`` / ``--outputdir``: Output directory for time-series files (defaults to ``hf_head_dir`` if omitted).
    - ``-v`` / ``--verbose``: Enable verbose console output.
    - ``-V`` / ``--version``: Print the installed ``gents`` version and exit.
    - ``-d`` / ``--dryrun``: Parse metadata only; do not write time-series files.
    - ``-w`` / ``--overwrite``: Overwrite existing time-series output files.
    - ``-sl`` / ``--slice``: Maximum length of individual time-series files in years (default ``10``).
    - ``-hc`` / ``--hfcores``: Maximum number of cores for parallel metadata reads (default ``64``).
    - ``-tc`` / ``--tscores``: Maximum number of cores for parallel time-series writes (default ``8``).
    - ``-m`` / ``--model``: Model default configuration to apply (``'CESM3'``, ``'CESM2'``, or ``'E3SM'``; default ``'none'``).
    - ``--exclude``: Glob pattern to exclude; may be specified multiple times. Overrides the model default unless ``--append`` is also set.
    - ``--include``: Glob pattern to include; may be specified multiple times. Overrides the model default unless ``--append`` is also set.
    - ``--append``: Append ``--exclude``/``--include`` filters to the model default configuration instead of replacing them.

    :returns: Namespace object populated with parsed argument values.
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
    return parser.parse_args()


def main():
    """
    Entry point for the ``gents`` command-line interface.

    Performs the following steps:

    1. Calls :func:`parse_arguments` to obtain the parsed CLI namespace.
    2. Defaults ``outputdir`` to ``hf_head_dir`` when ``-o`` is not supplied.
    3. Selects the appropriate model configuration:

       - ``--model e3sm`` flag → imports :func:`~gents.configs.gents_e3sm.run_config` (E3SM).
       - ``--model cesm3`` → imports :func:`~gents.configs.gents_cesm3.run_config` (CESM3).

    4. If ``--verbose`` is set, prints a summary of all active settings to stdout.
    5. Delegates execution to the selected ``run_config(args)`` function.
    """
    command_str = " ".join(sys.argv)
    args = parse_arguments()

    if args.outputdir is None:
        args.outputdir = args.hf_head_dir
    
    if args.model is not None:
        args.model = args.model.lower()

    if args.model == "cesm3" or args.model == "cesm2":
        from gents.configs.gents_cesm3 import CESM3Config as ModelConfig
    elif args.model == "e3sm":
        from gents.configs.gents_e3sm import E3SMConfig as ModelConfig
    elif args.model == None:
        from gents.configs.config import GenTSConfig as ModelConfig
    else:
        raise ValueError(f"Configuration module for '{args.model}' not found ('gents.configs.gents_{args.model}' does not exist).")

    if args.verbose:
        print(f"  Input (HF) directory path    : {args.hf_head_dir}")
        print(f"  Output (TS) directory path   : {args.outputdir}")
        print(f"  Model Configuration          : {args.model}")
        print(f"  Overwrite TS Files           : {args.overwrite}")
        print(f"  Slice size                   : {args.slice}")
        print(f"  Dry run                      : {args.dryrun}")
        print(f"  Number of HF processes (cores)  : {args.hfcores}")
        print(f"  Number of TS processes (cores)  : {args.tscores}")
        print(f"  Include filters                 : {args.include}")
        print(f"  Exclude filters                 : {args.exclude}")
        print(f"  Append filters to defaults      : {args.append}")
        print(f"  Time alignment method           : {args.align_method}")
        print(f"  Slice start year                : {args.slice_start_year}")
        enable_logging(verbose=True)

    model_conf = ModelConfig(args.hf_head_dir, args.outputdir)

    if args.append:
        for pattern in args.include:
            model_conf.hf_include_patterns.append(pattern)
        for pattern in args.exclude:
            model_conf.hf_exclude_patterns.append(pattern)
    else:
        if len(args.include) > 0:
            model_conf.hf_include_patterns = args.include
        if len(args.exclude) > 0:
            model_conf.hf_exclude_patterns = args.exclude

    hf_collection = model_conf.get_hfcollection(
        num_cores=args.hfcores,
        slice_size_years=args.slice,
        slice_start_year=args.slice_start_year,
        align_method=args.align_method
    )
    ts_collection = model_conf.get_tscollection(
        hfc=hf_collection,
        num_cores=args.tscores,
        append_dirs=True,
        overwrite=args.overwrite
    )

    ts_collection = ts_collection.add_attrs({"gents_command": command_str})

    log_hfcollection_info(hf_collection)
    log_tscollection_info(ts_collection)

    if not args.dryrun:
        ts_collection.execute()
    else:
        print(f"Dry run: {len(ts_collection)} timeseries files would be generated.")
    print("GenTS done!")