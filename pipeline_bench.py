#!/usr/bin/env python
"""
Profiling / benchmarking driver for the HF -> TS pipeline.

Builds (or reuses, via ``benchmarks.fixtures.build_bench_case``) an on-disk
synthetic history-file case, runs one ``TSCollection.execute()`` configuration
against it, and prints a single machine-readable metrics row: execute() wall
time, durable-write (fsync) wall time, bytes written, peak RSS, and derived
throughput.

Deliberately a plain script at the repo root, outside ``benchmarks/``, so ASV
does not try to discover it as a benchmark suite. Two uses:

    python pipeline_bench.py [options]
    py-spy record --native -r 250 --idle -o out.svg -- python pipeline_bench.py [options]

See ``docs/llm/workflows.md`` (Performance profiling) for the full iteration
loop this is meant to sit inside: run once for a baseline row, change one
knob, re-run, compare.
"""
import argparse
import os
import resource
import sys
import time
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp

from benchmarks.fixtures import build_bench_case
from gents.hfcollection import HFCollection
from gents.timeseries import CHUNK_TARGET_BYTES, TSCollection


def durable_write_seconds(paths):
    """fsync every output file; returns the wall time spent doing so."""
    t0 = time.perf_counter()
    for path in paths:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return time.perf_counter() - t0


def peak_rss_mib():
    """
    Max of this process's and its (reaped) children's peak RSS, in MiB.

    ``ru_maxrss`` is KiB on Linux (the only platform this driver is
    documented against; it is bytes on macOS). Children only count once
    ``ProcessPoolExecutor`` has been joined -- true by the time
    ``execute()`` returns, since it waits on its pool in a ``with`` block.
    """
    self_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return max(self_kib, children_kib) / 1024


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run one HF->TS pipeline configuration and print a metrics row.",
    )
    case = parser.add_argument_group("bench case (forwarded to build_bench_case)")
    case.add_argument("--case-dir", default="./_pipeline_bench_case",
                       help="Directory for the synthetic case; reused if a matching one exists (default: %(default)s)")
    case.add_argument("--n-files", type=int, default=24)
    case.add_argument("--n-steps", type=int, default=24)
    case.add_argument("--n-lat", type=int, default=192)
    case.add_argument("--n-lon", type=int, default=288)
    case.add_argument("--n-vars", type=int, default=2)
    case.add_argument("--step-days", type=int, default=30)
    case.add_argument("--dtype", default="float64", choices=["float64", "float32"],
                       help="Primary/auxiliary variable dtype (default: %(default)s). "
                            "float32 is representative of typical model output.")
    case.add_argument("--fill", default="constant", choices=["constant", "random"],
                       help="constant (default) compresses to nearly nothing regardless of "
                            "--complevel/--compression; use random for realistic compression numbers.")

    run = parser.add_argument_group("pipeline configuration")
    run.add_argument("--num-processes", type=int, default=1,
                      help="1 runs in-process (no ProcessPoolExecutor) -- what py-spy needs to see real frames (default: %(default)s)")
    run.add_argument("--no-optimize", action="store_true",
                      help="Disable order batching (one worker call per variable instead of per group)")
    run.add_argument("--complevel", type=int, default=None, help="netCDF4 compression level (0-9); requires --compression")
    run.add_argument("--compression", default=None, help="netCDF4 compression algorithm, e.g. zlib; requires --complevel")
    run.add_argument("--chunk-target-bytes", type=int, default=None,
                      help="Override the 4 MiB chunk-size target (gents.timeseries.CHUNK_TARGET_BYTES). "
                           "Note: output written with a non-default value will fail check_timeseries_conform, "
                           "which always checks against the real default, not this override.")

    out = parser.add_argument_group("output")
    out.add_argument("--out-dir", default=None,
                      help="Output directory; defaults to a fresh temp dir, removed after the run unless --keep-output")
    out.add_argument("--keep-output", action="store_true",
                      help="Don't delete --out-dir afterwards (ignored if --out-dir was not given)")

    args = parser.parse_args()
    if (args.complevel is None) != (args.compression is None):
        parser.error("--complevel and --compression must be given together")
    return args


def main():
    args = parse_args()

    hf_paths = build_bench_case(
        args.case_dir, n_files=args.n_files, n_steps=args.n_steps,
        n_lat=args.n_lat, n_lon=args.n_lon, n_vars=args.n_vars, step_days=args.step_days,
        dtype=args.dtype, fill=args.fill,
    )

    out_dir = Path(args.out_dir) if args.out_dir else Path(mkdtemp(prefix="pipeline_bench_out_"))
    cleanup_out_dir = args.out_dir is None or not args.keep_output

    hf_collection = HFCollection(args.case_dir, num_processes=args.num_processes)
    # Pull explicitly (and quietly) before TSCollection construction, which
    # would otherwise auto-pull with a visible progress bar the first time
    # sort_along_time() calls check_pulled().
    hf_collection.pull_metadata(show_progress=False)
    ts_collection = TSCollection(hf_collection, str(out_dir), num_processes=args.num_processes)
    if args.compression is not None:
        ts_collection = ts_collection.apply_compression(level=args.complevel, alg=args.compression, path_glob="*")
    if args.chunk_target_bytes is not None:
        ts_collection = ts_collection.apply_chunk_target_bytes(args.chunk_target_bytes, path_glob="*")
    # Always force a real write: an --out-dir reused across runs would
    # otherwise skip every file whose integrity stamp already checks out,
    # silently turning "second run" into a near-instant no-op.
    ts_collection = ts_collection.apply_overwrite("*")

    t0 = time.perf_counter()
    ts_paths = ts_collection.execute(optimize=not args.no_optimize, show_progress=False)
    execute_s = time.perf_counter() - t0

    sync_s = durable_write_seconds(ts_paths)
    bytes_out = sum(os.path.getsize(p) for p in ts_paths)
    throughput_mbps = (bytes_out / (1024 * 1024)) / (execute_s + sync_s) if (execute_s + sync_s) > 0 else float("nan")

    metrics = {
        "case_dir": args.case_dir, "n_files": args.n_files, "n_steps": args.n_steps,
        "n_lat": args.n_lat, "n_lon": args.n_lon, "n_vars": args.n_vars,
        "dtype": args.dtype, "fill": args.fill,
        "num_processes": args.num_processes, "optimize": not args.no_optimize,
        "complevel": args.complevel, "compression": args.compression,
        "chunk_target_bytes": args.chunk_target_bytes or CHUNK_TARGET_BYTES,
        "n_source_files": len(hf_paths), "n_output_files": len(ts_paths),
        "execute_s": round(execute_s, 4), "sync_s": round(sync_s, 4),
        "bytes_out": bytes_out, "throughput_mbps": round(throughput_mbps, 2),
        "peak_rss_mib": round(peak_rss_mib(), 2),
    }
    # Prefixed so it's greppable (`grep '^RESULT'`) regardless of anything
    # else the pipeline logs; progress bars are explicitly suppressed above.
    print("RESULT " + " ".join(f"{k}={v}" for k, v in metrics.items()))

    if cleanup_out_dir:
        rmtree(out_dir, ignore_errors=True)
    else:
        print(f"Output kept at: {out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
