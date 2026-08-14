#!/usr/bin/env python
"""
Logging setup, progress reporting, versioning, and collection summaries.

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
"""
from time import time
from importlib.metadata import version
import logging
import sys
import datetime
import numpy as np

LOG_LEVEL_IO_WARNING = 5


def get_time_stamp():
    """
    Returns the current date and time as a ``'YYYY-MM-DD HH:MM'`` string.

    :rtype: str
    """
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')


def get_version():
    """
    Returns the version of the installed ``gents`` package.

    :rtype: str
    """
    return version('gents')


def enable_logging(verbose=False, output_path=None):
    """
    Configures the ``gents`` logger to emit to stdout, and optionally to a file.

    :param verbose: Log at ``LOG_LEVEL_IO_WARNING`` (5), which adds per-file I/O
        traces, instead of ``DEBUG``. There is no quieter setting.
    :type verbose: bool
    :param output_path: File to write log output to in addition to stdout.
    :type output_path: str or None
    """
    logger = logging.getLogger("gents")

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    if output_path:
        file_handler = logging.FileHandler(output_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if verbose:
        logger.setLevel(LOG_LEVEL_IO_WARNING)
    else:
        logger.setLevel(logging.DEBUG)

    logger.addHandler(stdout_handler)

    logger.info(f"GenTS version {get_version()}")
    logger.info(f"Logging enabled (verbose={verbose}, output_path={output_path})")


def is_terminal(stream):
    """
    Returns whether a stream is an interactive terminal.

    Streams that stand in for stdout do not all implement ``isatty``, so a
    stream that cannot answer is treated as not a terminal.

    :param stream: Stream to test, usually ``sys.stdout``.
    :type stream: io.IOBase
    :rtype: bool
    """
    try:
        return stream.isatty()
    except (AttributeError, ValueError):
        return False


class ProgressBar:
    """
    Terminal progress bar drawn by overwriting a single stdout line in place.

    Drawing is skipped entirely when stdout is not a terminal. Overwriting in
    place only works on one, so in a log file, a CI job or a pipe every redraw
    would land as another line and bury the output worth reading.
    """

    def __init__(self, total, length=40, label="", quiet=False):
        """
        :param total: Total number of expected iterations.
        :type total: int
        :param length: Width of the rendered bar in characters.
        :type length: int
        :param label: Short text label shown beside the counter.
        :type label: str
        :param quiet: Count steps but write nothing to stdout. Forced on when
            stdout is not a terminal.
        :type quiet: bool
        """
        self.total = total
        self.length = length
        self.start_time = time()
        self.count = -1
        self.label = label
        self.quiet = quiet or not is_terminal(sys.stdout)
        self.step()

    def step(self):
        """
        Advances the bar by one iteration and redraws it, writing a trailing
        newline once the counter reaches ``total``.
        """
        self.count += 1
        if self.quiet:
            return
        percent = self.count / self.total
        filled_length = int(self.length * percent)
        bar = "█" * filled_length + "-" * (self.length - filled_length)
        elapsed = time() - self.start_time
        sys.stdout.write(
            f"\r |{bar}| {percent:6.2%} [{self.label}]  {self.count}/{self.total}  Elapsed: {elapsed:5.1f}s"
        )
        sys.stdout.flush()

        if self.count >= self.total:
            sys.stdout.write("\n")


def log_hfcollection_info(hfc, show_progress=True):
    """
    Logs summary statistics for an ``HFCollection`` at INFO level.

    Reports the input directory, file and group counts, total mapped data volume,
    the largest groups by variable and file count, and the largest single-timestep
    variable. Sizes are approximations (per-file variable sizes times file counts),
    not exact totals. Pulls metadata if it has not been pulled already.

    :param hfc: Collection to inspect.
    :type hfc: gents.hfcollection.HFCollection
    :param show_progress: If ``False``, suppress the stdout progress bar.
    :type show_progress: bool
    """
    logger = logging.getLogger("gents")

    hfc.check_pulled()
    logger.info(f"=============================================")
    logger.info(f"              HFCollection Info              ")
    logger.info(f"=============================================")
    logger.info(f"Input Directory: {hfc.get_input_dir()}")
    logger.info(f"Number history files found: {len(hfc)}")
    
    hf_groups = hfc.get_groups()
    logger.info(f"Output Groups formed: {len(hf_groups)}")

    prog_bar = ProgressBar(total=len(hf_groups), label="Calculating HFCollection Statistics", quiet=not show_progress)
    total_data_tb = 0
    largest_num_vars = 0
    largest_group_num_vars = None
    largest_timestep_mb = 0
    largest_timestep_group_num_hf = 0
    largest_timestep_group_num_hf_count = 0
    largest_timestep_group = None
    largest_timestep_var = None
    largest_timestep_shape = None
    largest_timestep_dims = None

    for hf_group_name in hf_groups:
        prog_bar.step()
        if len(hf_groups[hf_group_name]) > largest_timestep_group_num_hf_count:
            largest_timestep_group_num_hf_count = len(hf_groups[hf_group_name])
            largest_timestep_group_num_hf = hf_group_name

        hf_meta = hfc[hf_groups[hf_group_name][0]]
        if len(hf_meta.get_variables()) > largest_num_vars:
            largest_num_vars = len(hf_meta.get_variables())
            largest_group_num_vars = hf_group_name

        for var_name in hf_meta.get_variables():
            var_shape = hf_meta.get_variable_shapes(var_name)
            var_dims = hf_meta.get_variable_dims(var_name)
            var_dtype = hf_meta.get_variable_dtype(var_name)
            var_data_size_mb = var_dtype.itemsize * np.prod(var_shape) / (1024**2)
            total_data_tb += (var_data_size_mb / (1024**2))*len(hf_groups[hf_group_name])
            
            if var_data_size_mb > largest_timestep_mb:
                largest_timestep_group = hf_group_name
                largest_timestep_mb = var_data_size_mb
                largest_timestep_var = var_name
                largest_timestep_shape = var_shape
                largest_timestep_dims = var_dims

    logger.info(f"Total data mapped (TB): {total_data_tb}")
    logger.info(f"Total data mapped (GB): {total_data_tb * 1024}")
    logger.info(f"Largest group by number of variables: {largest_num_vars} variables for {largest_group_num_vars}")
    logger.info(f"Largest group by number of files: {largest_timestep_group_num_hf_count} files for {largest_timestep_group_num_hf}")
    logger.info(
        f"Largest variable timestep (memory footprint) \n" +
        f"    Group: {largest_timestep_group}\n" +
        f"    Variable: {largest_timestep_var}\n" +
        f"    Shape: {largest_timestep_shape}\n" +
        f"    Dimensions: {largest_timestep_dims}\n" +
        f"    Timestep size (MB): {largest_timestep_mb}"
    )


def log_tscollection_info(tsc, show_progress=True):
    """
    Logs summary statistics for a ``TSCollection`` at INFO level.

    Reports the output directory, the number of time series files to generate, and
    the largest of them (source variable, shape, dimensions, projected size).
    Auxiliary-only orders are skipped and sizes are estimates, not exact totals.

    :param tsc: Collection to inspect.
    :type tsc: gents.timeseries.TSCollection
    :param show_progress: If ``False``, suppress the stdout progress bar.
    :type show_progress: bool
    """
    logger = logging.getLogger("gents")

    logger.info(f"=============================================")
    logger.info(f"              TSCollection Info              ")
    logger.info(f"=============================================")
    logger.info(f"Output Directory: {tsc.get_output_dir()}")
    logger.info(f"Number time series files to generate: {len(tsc)}")

    total_files_in = 0
    total_data_out_mb = 0
    largest_ts_size_mb = 0
    largest_ts_hf_sample = None
    largest_ts_variable = None
    largest_ts_shape = None
    largest_ts_dims = None
    largest_ts_num_files = None

    prog_bar = ProgressBar(total=len(tsc), label="Calculating TSCollection Statistics", quiet=not show_progress)
    for order in tsc:
        prog_bar.step()
        if order["primary_var"] != "auxiliary":
            path = order["hf_paths"][0]
            hf_meta = tsc.get_hf_collection()[path]
            var_shape = hf_meta.get_variable_shapes(order["primary_var"])
            var_dims = hf_meta.get_variable_dims(order["primary_var"])
            var_dtype = hf_meta.get_variable_dtype(order["primary_var"])
            var_data_size_mb = var_dtype.itemsize * np.prod(var_shape) / (1024**2)* len(order["hf_paths"])

            total_data_out_mb += var_data_size_mb * len(order["hf_paths"])
            total_files_in += len(order["hf_paths"])

            if var_data_size_mb > largest_ts_size_mb:
                largest_ts_size_mb = var_data_size_mb
                largest_ts_hf_sample = path
                largest_ts_variable = order["primary_var"]
                largest_ts_shape = var_shape
                largest_ts_dims = var_dims
                largest_ts_num_files = len(order["hf_paths"])
        
    logger.info(
        f"Largest time series file (minimum memory requirement) \n" +
        f"    Sample HF path: {largest_ts_hf_sample}\n" +
        f"    Variable: {largest_ts_variable}\n" +
        f"    Shape: {largest_ts_shape}\n" +
        f"    Dimensions: {largest_ts_dims}\n" +
        f"    Number of history files to read: {largest_ts_num_files}\n"
        f"    Time series size (GB): {largest_ts_size_mb / 1024}"
    )