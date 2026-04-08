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