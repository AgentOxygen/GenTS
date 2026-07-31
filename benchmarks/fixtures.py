"""
Shared synthetic history-file case builder for ASV benchmarks and the
profiling driver (``pipeline_bench.py``).

Both consumers need the same kind of on-disk case without paying to
regenerate it every time they run: ASV's ``setup()`` methods rerun before
every timed repeat, and py-spy has no "setup phase" it excludes from a
profile, so inline generation shows up as noise in the flamegraph.
``build_bench_case()`` is the one place that does the generation; it is
idempotent, keyed off the parameters that determine the case's shape, so a
second call with the same parameters against the same root is a no-op.
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

    Idempotent: if ``root`` already holds a manifest recording these exact
    parameters, generation is skipped and the existing file list is
    returned. Any other content under ``root`` -- a case built with
    different parameters, or unrelated files -- is removed and rebuilt from
    scratch; there is no partial-match or resume logic, matching the "every
    run rebuilds everything it touches" convention used by
    ``gents_conform_build`` (see docs/llm/workflows.md).

    :param root: Directory to build the case in. Created if missing.
    :type root: str or pathlib.Path
    :param n_files: Number of history files to generate.
    :type n_files: int
    :param n_steps: Number of time steps per file.
    :type n_steps: int
    :param n_lat: Size of the synthetic ``lat`` dimension. Defaults to ``3``.
    :type n_lat: int
    :param n_lon: Size of the synthetic ``lon`` dimension. Defaults to ``4``.
    :type n_lon: int
    :param n_vars: Number of primary variables per file. Defaults to ``1``.
    :type n_vars: int
    :param step_days: Spacing between time steps, in days. Defaults to ``30``.
    :type step_days: int
    :param dtype: Primary/auxiliary variable dtype, forwarded to
        :func:`~gents.tests.test_cases.generate_history_file`. Defaults to
        ``float`` (float64); pass ``"float32"`` (or ``np.float32``) for
        fixtures representative of typical model output.
    :type dtype: type or numpy.dtype or str
    :param fill: ``"constant"`` (default) or ``"random"``, forwarded to
        :func:`~gents.tests.test_cases.generate_history_file`. Use
        ``"random"`` for any benchmark measuring compression -- constant
        data compresses to nearly nothing regardless of the compression
        settings under test.
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
