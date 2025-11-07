#!/usr/bin/env python
"""
utils.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 07/03/25
"""
from time import time
from importlib.metadata import version
from netCDF4 import Dataset
import logging
import sys
import datetime
import numpy as np

LOG_LEVEL_IO_WARNING = 5


def get_time_stamp():
    """Returns system date-time timestamp."""
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')


def get_version():
    """Returns the version of the installed GenTS package."""
    return version('gents')


def enable_logging(verbose=False, output_path=None):
    """
    Initializes logger to output GenTS logs.
    
    :param verbose: If true, all logs will be output including messages for individual file paths. Otherwise, only messages with level DEBUG or higher will be output. (Defaults to False)
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
        logger.setLevel(LOG_LEVEL_IO_WARNING)
    else:
        logger.setLevel(logging.DEBUG)

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


def generate_history_file(path, time_val, time_bounds_val, num_vars=6, nc_format="NETCDF4_CLASSIC", time_bounds_attrs=True, auxiliary=False):
    with Dataset(path, "w", format=nc_format) as ds:

        dim_shapes = {
            "time": None,
            "bnds": 2,
            "lat": 3,
            "lon": 4,
            "lev": 5
        }
        
        for dim in dim_shapes:
            ds.createDimension(dim, dim_shapes[dim])

        for index in range(num_vars):
            if auxiliary:
                var_data = ds.createVariable(f"VAR{index}", float, ("time"))

                var_data[:] = np.random.random((len(time_val))).astype(float)
                var_data.setncatts({
                    "units": "kg/g/m^2/K",
                    "standard_name": f"VAR{index}",
                    "long_name": f"variable_{index}"
                })
            else:
                var_data = ds.createVariable(f"VAR{index}", float, ("time", "lat", "lon"))

                var_data[:] = np.random.random((len(time_val), dim_shapes["lat"], dim_shapes["lon"])).astype(float)
                var_data.setncatts({
                    "units": "kg/g/m^2/K",
                    "standard_name": f"VAR{index}",
                    "long_name": f"variable_{index}"
                })

        time_data = ds.createVariable(f"time", np.double, "time")
        time_data[:] = time_val
        time_data.setncatts({
            "calendar": "360_day",
            "units": "days since 1850-01-01",
            "standard_name": "time",
            "long_name": "time"
        })

        if time_bounds_val is not None:
            time_bnds_data = ds.createVariable(f"time_bounds", np.double, ("time", "bnds"))
            time_bnds_data[:] = time_bounds_val
            if time_bounds_attrs:
                time_bnds_data.setncatts({
                    "calendar": "360_day",
                    "units": "days since 1850-01-01",
                    "standard_name": "time_bounds",
                    "long_name": "time_bounds"
                })
            
        ds.setncatts({
            "source": "GenTS testing suite",
            "description": "Synthetic data used for testing with the GenTS package.",
            "frequency": "month",
        })