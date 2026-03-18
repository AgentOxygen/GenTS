from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
import gents


def run_config(args):
    gents.utils.enable_logging(verbose=args.verbose)

    hf_collection = HFCollection(args.hf_head_dir, num_processes=args.numcores)
    hf_collection = hf_collection.include(["*/atm/*"])
    hf_collection = hf_collection.slice_groups(slice_size_years=args.slice)
    ts_collection = TSCollection(hf_collection, args.outputdir, num_processes=args.numcores)
    ts_collection = ts_collection.apply_path_swap("/hist/", "/proc/tseries/").append_timestep_dirs()
    if args.overwrite:
        ts_collection = ts_collection.apply_overwrite("*")

    if not args.dryrun:
        ts_collection.execute()
    else:
        print(f"{len(ts_collection)} timeseries files.")
    print("GenTS done!")