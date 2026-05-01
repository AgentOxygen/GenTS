from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.utils import log_hfcollection_info, log_tscollection_info
import gents

INCLUDE_PATTERNS = [
    "*.nc"
]

EXCLUDE_PATTERNS = [
    "*.log"
]

def run_config(args):
    gents.utils.enable_logging(verbose=args.verbose)

    include_patterns = INCLUDE_PATTERNS
    exclude_patterns = EXCLUDE_PATTERNS

    if args.append:
        for pattern in args.include:
            include_patterns.append(pattern)
        for pattern in args.exclude:
            exclude_patterns.append(pattern)
    else:
        if len(args.include) > 0:
            include_patterns = args.include
        if len(args.exclude) > 0:
            exclude_patterns = args.exclude

    hf_collection = HFCollection(args.hf_head_dir, num_processes=args.hfcores)
    hf_collection = hf_collection.include(include_patterns).exclude(exclude_patterns).slice_groups(
        slice_size_years=args.slice,
        start_year=args.slice_start_year,
        time_alignment_method=args.align_method
    )
    ts_collection = TSCollection(hf_collection, args.outputdir, num_processes=args.tscores)
    ts_collection = ts_collection.append_timestep_dirs()

    if args.align_method != "midpoint":
        ts_collection = ts_collection.update_ts_orders(
            time_alignment_method=args.align_method
        )

    if args.overwrite:
        ts_collection = ts_collection.apply_overwrite("*")

    log_hfcollection_info(hf_collection)
    log_tscollection_info(ts_collection)

    if not args.dryrun:
        ts_collection.execute()
    else:
        print(f"{len(ts_collection)} timeseries files.")
    print("GenTS done!")