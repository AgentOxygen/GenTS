import datetime
from time import time
from importlib.metadata import version
import logging


def log(msg, level=logging.NOTSET):
    logging.getLogger('gents').log(level=level, msg=msg)

def get_time_stamp():
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')

def get_version():
    return version('gents')