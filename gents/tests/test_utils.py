from gents import utils

def test_timestamp():
    assert type(utils.get_time_stamp()) == str

def test_version():
    assert type(utils.get_version()) == str