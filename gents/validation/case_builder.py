from gents.hfcollection import HFCollection
from gents.utils import get_version, log_hfcollection_info, enable_logging
from pathlib import Path
from netCDF4 import Dataset
from os.path import commonpath
import numpy as np
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="GenTS Validation Case Builder Tool"
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
        "-hc", "--hfcores",
        type=int,
        default=64,
        help="Maximum number of cores to use for metadata-reads if running in parallel. (Default 64)"
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Pattern to exclude (can be specified multiple times)."
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Pattern to include (can be specified multiple times)."
    )
    return parser.parse_args()


def clone_netcdf_with_missing(src_path: str, dst_path: str):
    """
    Create a structurally identical NetCDF file whose variables are filled with
    NaNs (floating point) or _FillValue (integer types).

    Preserves:
        - dimensions
        - global attributes
        - variable attributes
        - compression
        - chunking
        - endianness
        - groups
        - unlimited dimensions
    """

    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)

    with Dataset(src_path, "r") as src, Dataset(dst_path, "w", format=src.file_format,) as dst:
        def copy_group(src_grp, dst_grp):
            dst_grp.setncatts(
                {attr: src_grp.getncattr(attr) for attr in src_grp.ncattrs()}
            )

            for name, dim in src_grp.dimensions.items():
                dst_grp.createDimension(
                    name,
                    None if dim.isunlimited() else len(dim),
                )

            for name, src_var in src_grp.variables.items():

                kwargs = {}

                try:
                    filters = src_var.filters()
                
                    allowed = ["zlib", "complevel", "shuffle", "fletcher32"]
                
                    kwargs.update({k: v for k, v in filters.items() if k in allowed})
                except Exception:
                    pass

                if src_var.filters()["zlib"]:
                    kwargs["zlib"] = True
                    kwargs["complevel"] = src_var.filters()["complevel"]
                
                kwargs["shuffle"] = src_var.filters()["shuffle"]
                kwargs["fletcher32"] = src_var.filters()["fletcher32"]
                
                try:
                    chunking = src_var.chunking()
                    if chunking != "contiguous":
                        kwargs["chunksizes"] = chunking
                except Exception:
                    pass

                try:
                    kwargs["endian"] = src_var.endian()
                except Exception:
                    pass

                dst_var = dst_grp.createVariable(
                    name,
                    src_var.dtype,
                    src_var.dimensions,
                    **kwargs,
                )

                dst_var.setncatts(
                    {attr: src_var.getncattr(attr) for attr in src_var.ncattrs()}
                )

                if (len(src_var.dimensions) == 1
                    or name in src_var.dimensions
                    or "time" in name
                   ):
                    dst_var[:] = src_var[:]
                    continue

                if src_var.size == 0:
                    continue

                if np.issubdtype(src_var.dtype, np.floating):
                    dst_var[:] = np.full(src_var.shape, np.nan, dtype=src_var.dtype)
                elif np.issubdtype(src_var.dtype, np.integer):
                    fill = getattr(src_var, "_FillValue", None)
                    if fill is None:
                        fill = np.iinfo(src_var.dtype).min

                    dst_var[:] = np.full(src_var.shape, fill, dtype=src_var.dtype)
                elif src_var.dtype.kind in ("S", "U"):
                    dst_var[:] = ""
                else:
                    try:
                        dst_var[:] = src_var[:]
                    except Exception:
                        pass

            for name, subgroup in src_grp.groups.items():
                copy_group(subgroup, dst_grp.createGroup(name))
        copy_group(src, dst)


def main():
    enable_logging(verbose=True)
    args = parse_arguments()

    if args.outputdir is None:
        raise ValueError(f"No output directory specified.")

    hfc = HFCollection(args.hf_head_dir, num_processes=args.hfcores)
    
    if len(args.include) > 0:
        hfc = hfc.include(args.include)
    if len(args.exclude) > 0:
        hfc = hfc.exclude(args.exclude)
    
    hfc = hfc.pull_metdata()
    log_hfcollection_info(hfc)

    common_head_dir = commonpath(hfc)

    prog_bar = ProgressBar(total=len(hfc), label="Creating NaN-Filled Clone")
    for hf_path in hfc:
        out_path = args.outputdir + "/" + hf_path.split(common_head_dir)[-1]
        clone_netcdf_with_missing(hf_path, out_path)
        prog_bar.step()
    
    print("GenTS done!")