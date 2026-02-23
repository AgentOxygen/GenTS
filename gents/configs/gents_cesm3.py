from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
import gents


def run_config(args):
    gents.utils.enable_logging(verbose=args.verbose)

    client = None
    if not args.serial:
        from dask.distributed import LocalCluster, Client
        cluster = LocalCluster(n_workers=args.numcores, threads_per_worker=1, memory_limit=f"{args.memorylimit}GB", processes=True)
        client = cluster.get_client()
    
    hf_collection = HFCollection(args.hf_head_dir, dask_client=client)
    hf_collection = hf_collection.include(["*/atm/*", "*/ice/*", "*/lnd/*", "*/glc/*", "*/rof/*"]).slice_groups(slice_size_years=args.slice)
    hf_collection.pull_metadata()
    
    ts_collection = TSCollection(hf_collection, args.outputdir, dask_client=client).apply_path_swap("/hist/", "/proc/tseries/").append_timestep_dirs()
    if args.overwrite:
        ts_collection = ts_collection.apply_overwrite("*")

    if not args.dryrun:
        ts_collection.execute()

    if not args.serial:
        client.shutdown()
        cluster.close()
    
    print("GenTS done!")