from gents.utils import *
import pytest

LOGGER_HEADER_NUM_LINES = 2


def test_timestamp():
    """get_time_stamp() returns a string."""
    assert type(get_time_stamp()) == str


def test_version():
    """get_version() returns a string."""
    assert type(get_version()) == str


@pytest.fixture(scope="session")
def log_output_dir(tmp_path_factory):
    """Session-scoped temp directory for log file output."""
    return tmp_path_factory.mktemp("log_data")


def test_logging(log_output_dir):
    """Non-verbose mode suppresses LOG_LEVEL_IO_WARNING messages; verbose mode includes them; both emit exactly two header lines on init."""
    logger = logging.getLogger("gents")

    enable_logging(verbose=False, output_path=f"{log_output_dir}/log_header.txt")

    with open(f"{log_output_dir}/log_header.txt") as f:
        assert len(f.readlines()) == LOGGER_HEADER_NUM_LINES

    enable_logging(verbose=False, output_path=f"{log_output_dir}/non_verbose.txt")

    logger.debug("test")
    logger.info("test")
    logger.warning("test")
    logger.log(LOG_LEVEL_IO_WARNING, "test")

    with open(f"{log_output_dir}/non_verbose.txt") as f:
        assert len(f.readlines()) == LOGGER_HEADER_NUM_LINES + 3
    
    enable_logging(verbose=True, output_path=f"{log_output_dir}/verbose.txt")

    logger.debug("test")
    logger.info("test")
    logger.warning("test")
    logger.log(LOG_LEVEL_IO_WARNING, "test")

    with open(f"{log_output_dir}/verbose.txt") as f:
        assert len(f.readlines()) == LOGGER_HEADER_NUM_LINES + 4