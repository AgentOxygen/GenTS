#!/usr/bin/env python
"""
utils.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 07/03/25
"""
import datetime
from time import time
from importlib.metadata import version
import logging


def log(msg, level=logging.NOTSET):
    """
    Internal logging tool.

    :param msg: Message to add to the log.
    :param level: Logging level to assign to the message.
    """
    logging.getLogger('gents').log(level=level, msg=msg)

def get_time_stamp():
    """Returns system date-time timestamp."""
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')

def get_version():
    """Returns the version of the installed GenTS package."""
    return version('gents')