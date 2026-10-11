from gents.utils import *
from gents.tests.test_cases import *
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
import pytest
from datetime import timedelta

LOGGER_HEADER_NUM_LINES = 2


def test_timestamp():
    """get_time_stamp() returns a string."""
    assert type(get_time_stamp()) == str


def test_version():
    """get_version() returns a string."""
    assert type(get_version()) == str


class FakeStdout:
    """Minimal stdout stand-in that records writes and reports a chosen tty state."""

    def __init__(self, tty):
        self.tty = tty
        self.written = ""

    def isatty(self):
        return self.tty

    def write(self, text):
        self.written += text

    def flush(self):
        pass


def test_progress_bar_draws_on_a_terminal(monkeypatch):
    """A progress bar draws itself when stdout is a terminal."""
    stdout = FakeStdout(tty=True)
    monkeypatch.setattr(sys, "stdout", stdout)

    bar = ProgressBar(total=2, label="Testing")
    bar.step()

    assert "Testing" in stdout.written
    assert bar.count == 1


def test_progress_bar_silent_without_a_terminal(monkeypatch):
    """
    A progress bar writes nothing when stdout is not a terminal.

    Redrawing in place only makes sense on a terminal. Anywhere else -- a log
    file, a CI job, a pipe -- every redraw lands as another line and buries the
    output that matters, so counting continues but drawing does not.
    """
    stdout = FakeStdout(tty=False)
    monkeypatch.setattr(sys, "stdout", stdout)

    bar = ProgressBar(total=2, label="Testing")
    bar.step()
    bar.step()

    assert stdout.written == ""
    assert bar.count == 2


def test_progress_bar_with_nothing_to_do_draws_complete(monkeypatch):
    """A zero-total bar (an empty execute(), e.g. after skip_existing() finds
    everything done) draws as complete on a terminal instead of dividing by zero."""
    stdout = FakeStdout(tty=True)
    monkeypatch.setattr(sys, "stdout", stdout)

    ProgressBar(total=0, label="Testing")

    assert "100.00%" in stdout.written
    assert stdout.written.endswith("\n")


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

def test_log_hfcollection_info_show_progress_reaches_metadata_pull(simple_case):
    """show_progress=False must also silence the 'Pulling Metadata' bar that check_pulled() triggers."""
    from unittest.mock import patch

    input_head_dir, output_head_dir = simple_case
    hf_collection = HFCollection(input_head_dir)
    assert not hf_collection.is_pulled()

    with patch("gents.meta.ProgressBar") as mock_bar:
        log_hfcollection_info(hf_collection, show_progress=False)

    assert mock_bar.call_args.kwargs["quiet"] is True


def test_filter_by_globs():
    """filter_by_globs() keeps or drops matches, accepts a str or list, and leaves its input untouched."""
    path_map = {"/a/0_dir/x.nc": 1, "/a/1_dir/y.nc": 2, "/a/2_dir/z.nc": 3}
    assert list(filter_by_globs(path_map, "*/0_dir/*", True)) == ["/a/0_dir/x.nc"]
    assert list(filter_by_globs(path_map, ["*/0_dir/*", "*/1_dir/*"], False)) == ["/a/2_dir/z.nc"]
    assert filter_by_globs(path_map, [], True) == {}
    assert filter_by_globs(path_map, [], False) == path_map
    assert len(path_map) == 3


def test_get_timestep_label_unknown_delta():
    """An unknown timestep duration is labelled 'unsorted' rather than guessed at."""
    assert get_timestep_label(None) == "unsorted"


@pytest.mark.parametrize("delta, label", [
    (timedelta(hours=1), "hour_1"),
    (timedelta(hours=6), "hour_6"),
    (timedelta(hours=23), "hour_23"),
    (timedelta(hours=24), "day_1"),
    (timedelta(days=27), "day_27"),
    (timedelta(days=28), "month_1"),
    (timedelta(days=31), "month_1"),
    (timedelta(days=90), "month_3"),
    (timedelta(days=335), "month_11"),
    (timedelta(days=360), "year_1"),
    (timedelta(days=365), "year_1"),
    (timedelta(days=3650), "year_10"),
])
def test_get_timestep_label(delta, label):
    """get_timestep_label() maps a step duration to its frequency label, including the hour/day/month/year boundaries."""
    assert get_timestep_label(delta) == label
