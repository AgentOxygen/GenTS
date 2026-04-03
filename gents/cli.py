import argparse
import sys
from gents.utils import get_version

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
    - ``-nc`` / ``--numcores``: Maximum worker processes for parallel execution (default ``64``).
    - ``-e3`` / ``--e3sm``: Use the E3SM model configuration instead of the default CESM3 configuration.

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
        "-e3", "--e3sm",
        action="store_true",
        help="Enable E3SM configuration."
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Pattern to exclude (can be specified multiple times). Overrides default."
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Pattern to include (can be specified multiple times). Overrides default."
    )
    return parser.parse_args()


def main():
    """
    Entry point for the ``gents`` command-line interface.

    Performs the following steps:

    1. Calls :func:`parse_arguments` to obtain the parsed CLI namespace.
    2. Defaults ``outputdir`` to ``hf_head_dir`` when ``-o`` is not supplied.
    3. Selects the appropriate model configuration:

       - ``--e3sm`` flag → imports :func:`~gents.configs.gents_e3sm.run_config` (E3SM).
       - Otherwise → imports :func:`~gents.configs.gents_cesm3.run_config` (CESM3).

    4. If ``--verbose`` is set, prints a summary of all active settings to stdout.
    5. Delegates execution to the selected ``run_config(args)`` function.
    """
    args = parse_arguments()

    if args.outputdir is None:
        args.outputdir = args.hf_head_dir
    
    if args.e3sm:
        from gents.configs.gents_e3sm import run_config
        model = "E3SM"
    else:
        from gents.configs.gents_cesm3 import run_config
        model = "CESM3"
    
    if args.verbose:
        print(f"  Input (HF) directory path    : {args.hf_head_dir}")
        print(f"  Output (TS) directory path   : {args.outputdir}")
        print(f"  Model Configuration          : {model}")
        print(f"  Overwrite TS Files           : {args.overwrite}")
        print(f"  Slice size                   : {args.slice}")
        print(f"  Dry run                      : {args.dryrun}")
        print(f"  Number of HF processes (cores)  : {args.hfcores}")
        print(f"  Number of TS processes (cores)  : {args.tscores}")

    run_config(args)