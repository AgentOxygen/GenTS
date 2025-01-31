import datetime
from time import time
from importlib.metadata import version

def get_time_stamp():
    return datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M')


def get_version():
    return version('gents')