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