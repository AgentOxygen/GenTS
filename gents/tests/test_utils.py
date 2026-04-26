from gents.utils import *
from gents.tests.test_cases import *
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
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


def test_hfc_info_logger(structured_case, log_output_dir):
    logger = logging.getLogger("gents")
    enable_logging(verbose=True, output_path=f"{log_output_dir}/hfc_logger.txt")

    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)

    log_hfcollection_info(hf_collection)

    with open(f"{log_output_dir}/hfc_logger.txt") as f:
        info_output_found = False
        for line in f.readlines():
            if "HFCollection Info" in line:
                info_output_found = True
                break
        assert info_output_found


def test_hfc_info_logger(structured_case, log_output_dir):
    logger = logging.getLogger("gents")
    enable_logging(verbose=True, output_path=f"{log_output_dir}/tsc_logger.txt")

    input_head_dir, output_head_dir = structured_case
    hf_collection = HFCollection(input_head_dir)
    ts_collection = TSCollection(hf_collection, output_head_dir)

    log_tscollection_info(ts_collection)

    with open(f"{log_output_dir}/tsc_logger.txt") as f:
        info_output_found = False
        for line in f.readlines():
            if "TSCollection Info" in line:
                info_output_found = True
                break
        assert info_output_found