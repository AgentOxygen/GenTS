from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
import gents

INCLUDE_PATTERNS = [
    "*/atm/*",
    "*/ice/*",
    "*/lnd/*",
    "*/glc/*",
    "*/ocn/*",
    "*/rof/*"
]

EXCLUDE_PATTERNS = [
    "*/rest/*",
    "*/logs/*",
    "*.ocean_geometry.nc",
    "*mom6.ic.*",
    "*cam.i.*"
]

def run_config(args):
    gents.utils.enable_logging(verbose=args.verbose)

    hf_collection = HFCollection(args.hf_head_dir, num_processes=args.numcores)
    hf_collection = hf_collection.include(INCLUDE_PATTERNS).exclude(EXCLUDE_PATTERNS).slice_groups(slice_size_years=args.slice)
    ts_collection = TSCollection(hf_collection, args.outputdir, num_processes=args.numcores).apply_path_swap("/hist/", "/proc/tseries/").append_timestep_dirs()
    ts_collection = ts_collection.apply_compression(2, "zstd", "*")
    if args.overwrite:
        ts_collection = ts_collection.apply_overwrite("*")

    if not args.dryrun:
        ts_collection.execute()
    else:
        print(f"{len(ts_collection)} timeseries files.")
    print("GenTS done!")