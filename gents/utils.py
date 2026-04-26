#!/usr/bin/env python
"""
utils.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 07/03/25
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
    Returns the current system date and time as a formatted string.

    :returns: Date-time string formatted as ``'YYYY-MM-DD HH:MM'``.
    :rtype: str
    """
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')


def get_version():
    """
    Returns the version string of the installed ``gents`` package.

    :returns: Package version string (e.g. ``'1.0.0'``).
    :rtype: str
    """
    return version('gents')


def enable_logging(verbose=False, output_path=None):
    """
    Configures the ``gents`` package logger and begins emitting log messages.

    At ``verbose=True``, the log level is set to ``LOG_LEVEL_IO_WARNING`` (5), enabling
    per-file I/O trace messages. At the default ``verbose=False``, the level is ``DEBUG``
    (10), suppressing those low-level traces. The installed GenTS version is logged
    immediately on initialisation.

    :param verbose: If ``True``, enable per-file I/O trace messages at
        ``LOG_LEVEL_IO_WARNING`` level. Defaults to ``False``.
    :type verbose: bool
    :param output_path: Optional file path to additionally write log output to.
        Defaults to ``None`` (stdout only).
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


class ProgressBar:
    """
    Terminal progress bar for visualising long-running loops.

    Displays a continuously-updated bar, percentage, item count, and elapsed
    time by overwriting a single terminal line in place.
    """

    def __init__(self, total, length=40, label=""):
        """
        Initialises the progress bar state.

        :param total: Total number of expected iterations.
        :type total: int
        :param length: Width of the rendered bar in characters. Defaults to ``40``.
        :type length: int
        :param label: Short text label displayed beside the progress counter.
            Defaults to an empty string.
        :type label: str
        """
        self.total = total
        self.length = length
        self.start_time = time()
        self.count = -1
        self.label = label
        self.step()

    def step(self):
        """
        Advances the progress bar by one iteration and redraws the terminal line.

        Writes a newline once the counter reaches ``total``.
        """
        self.count += 1
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


def log_hfcollection_info(hfc):
    """
    Logs summary statistics for an ``HFCollection`` at INFO level.

    Iterates over all groups in the collection to compute aggregate metrics and
    identify outliers. Requires metadata to have been pulled (calls
    ``hfc.check_pulled()``). A progress bar is displayed on stdout during
    the scan.

    Statistics logged:

    - Input directory and total number of history files found.
    - Number of output groups formed.
    - Total mapped data volume in TB and GB.
    - Group with the most variables.
    - Group with the most history files.
    - Variable with the largest single-timestep memory footprint (shape,
      dimensions, and size in MB).

    :param hfc: A pulled ``HFCollection`` instance to inspect.
    :type hfc: gents.hfcollection.HFCollection
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

    prog_bar = ProgressBar(total=len(hf_groups), label="Calculating HFCollection Statistics")
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


def log_tscollection_info(tsc):
    """
    Logs summary statistics for a ``TSCollection`` at INFO level.

    Iterates over all time series orders in the collection to compute aggregate
    metrics and identify the largest output file. Auxiliary-only orders are
    skipped. A progress bar is displayed on stdout during the scan.

    Statistics logged:

    - Output directory and total number of time series files to generate.
    - Largest time series file by estimated total size, including the sample
      history file path, variable name, shape, dimensions, number of source
      history files, and projected size in GB.

    :param tsc: A ``TSCollection`` instance to inspect.
    :type tsc: gents.timeseries.TSCollection
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

    prog_bar = ProgressBar(total=len(tsc), label="Calculating TSCollection Statistics")
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