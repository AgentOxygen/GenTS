"""
Shared synthetic history-file case builder for the ASV benchmarks and the
profiling driver (``pipeline_bench.py``).

Generation has to stay out of what is being measured: ASV reruns ``setup()``
before every timed repeat, and py-spy has no setup phase to exclude from a
profile. ``build_bench_case()`` is therefore idempotent, keyed off the
parameters that determine the case's shape.
"""
import json
from pathlib import Path
from shutil import rmtree

import numpy as np

from gents.tests.test_cases import generate_history_file

MANIFEST_NAME = "_case_manifest.json"


def build_bench_case(root, n_files, n_steps, n_lat=3, n_lon=4, n_vars=1, step_days=30, dtype=float, fill="constant"):
    """
    Builds (or reuses) a synthetic history-file case under ``root``.

    Writes ``n_files`` netCDF history files, each holding ``n_steps``
    consecutive time steps ``step_days`` apart, with ``n_vars`` primary
    variables shaped ``(n_steps, n_lat, n_lon)``. Time values accumulate
    across files, so the whole case forms one contiguous time axis
    (``n_files == 1`` and large ``n_steps`` stresses a single big file;
    ``n_steps == 1`` and large ``n_files`` stresses many small ones).

    Idempotent: a ``root`` whose manifest records these exact parameters is
    reused as-is. Anything else under ``root`` is wiped and rebuilt -- no
    partial match, no resume, matching ``gents_conform_build``'s convention.

    :param root: Directory to build the case in. Created if missing.
    :type root: str or pathlib.Path
    :param n_files: Number of history files to generate.
    :type n_files: int
    :param n_steps: Number of time steps per file.
    :type n_steps: int
    :param n_lat: Size of the synthetic ``lat`` dimension.
    :type n_lat: int
    :param n_lon: Size of the synthetic ``lon`` dimension.
    :type n_lon: int
    :param n_vars: Number of primary variables per file.
    :type n_vars: int
    :param step_days: Spacing between time steps, in days.
    :type step_days: int
    :param dtype: Variable dtype; ``float32`` is representative of real output.
    :type dtype: type or numpy.dtype or str
    :param fill: ``"constant"`` or ``"random"``. Constant data compresses to
        nearly nothing, so use ``"random"`` when measuring compression.
    :type fill: str
    :returns: Sorted list of generated (or reused) file paths.
    :rtype: list[pathlib.Path]
    """
    root = Path(root)
    params = {
        "n_files": n_files, "n_steps": n_steps, "n_lat": n_lat,
        "n_lon": n_lon, "n_vars": n_vars, "step_days": step_days,
        "dtype": np.dtype(dtype).name, "fill": fill,
    }
    manifest_path = root / MANIFEST_NAME

    if manifest_path.is_file() and json.loads(manifest_path.read_text()) == params:
        return sorted(root.glob("*.nc"))

    rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)

    dim_shapes = {"time": None, "bnds": 2, "lat": n_lat, "lon": n_lon, "lev": 1}
    hf_paths = [root / f"case.hf.{index:05d}.nc" for index in range(n_files)]

    step = 0
    for path in hf_paths:
        times = [(step + i) * step_days for i in range(n_steps)]
        bounds = [[(step + i) * step_days, (step + i + 1) * step_days] for i in range(n_steps)]
        generate_history_file(str(path), times, bounds, num_vars=n_vars, dim_shapes=dim_shapes, dtype=dtype, fill=fill)
        step += n_steps

    manifest_path.write_text(json.dumps(params))
    return hf_paths
