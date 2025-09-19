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


def get_time_stamp():
    """Returns system date-time timestamp."""
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')


def get_version():
    """Returns the version of the installed GenTS package."""
    return version('gents')


def enable_logging(verbose=False, output_path=None):
    """
    Initializes logger to output GenTS logs.
    
    :param verbose: If true, all logs will be output. Otherwise, only messages with level INFO or higher will be output. (Defautls to False)
    :param output_path: File path to output logs to. (Defaults to None)
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
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.addHandler(stdout_handler)

    logger.info(f"GenTS version {get_version()}")
    logger.info(f"Logging enabled (verbose={verbose}, output_path={output_path})")


class ProgressBar:
    def __init__(self, total, length=40):
        """
        Progress bar for visualizing various processes throughout the package.

        :param total: Total number of iterations in the loop.
        :param length: Length of the progress bar in characters. (Default is 40)
        """
        self.total = total
        self.length = length
        self.start_time = time()
        self.count = 0

    def step(self):
        """Update the progress bar by a given step."""
        self.count += 1
        percent = self.count / self.total
        filled_length = int(self.length * percent)
        bar = "█" * filled_length + "-" * (self.length - filled_length)
        elapsed = time() - self.start_time
        sys.stdout.write(
            f"\r|{bar}| {percent:6.2%}  {self.count}/{self.total}  Elapsed: {elapsed:5.1f}s"
        )
        sys.stdout.flush()

        if self.count >= self.total:
            sys.stdout.write("\n")