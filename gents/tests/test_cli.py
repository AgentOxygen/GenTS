import pytest
import sys
from unittest.mock import patch, MagicMock
from gents.cli import parse_arguments, main
from gents.tests.test_cases import *
from gents.datastore import GenTSDataStore
from gents.hfcollection import find_files
from gents.utils import get_version
from gents.tests.test_workflow import is_monotonic
from os import listdir


def test_parse_required_positional():
    """hf_head_dir is captured as a positional argument."""
    with patch.object(sys, "argv", ["run_gents", "/data/input"]):
        args = parse_arguments()
    assert args.hf_head_dir == "/data/input"


def test_parse_defaults():
    """All optional arguments have the expected default values when omitted."""
    with patch.object(sys, "argv", ["run_gents", "/data/input"]):
        args = parse_arguments()
    assert args.outputdir is None
    assert args.verbose is False
    assert args.dryrun is False
    assert args.overwrite is False
    assert args.slice == 10
    assert args.hfcores == 64
    assert args.tscores == 8
    assert args.model is None
    assert args.exclude == []
    assert args.include == []
    assert args.slice_start_year is None


def test_parse_outputdir():
    """-o / --outputdir captures the output path."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "-o", "/data/output"]):
        args = parse_arguments()
    assert args.outputdir == "/data/output"


def test_parse_flags():
    """-v, -d, -w, -e3 flip their respective boolean flags."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "-v", "-d", "-w"]):
        args = parse_arguments()
    assert args.verbose is True
    assert args.dryrun is True
    assert args.overwrite is True


def test_parse_slice_and_cores():
    """-sl, -hc, -tc accept integer values."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "-sl", "5", "-hc", "8", "-tc", "4"]):
        args = parse_arguments()
    assert args.slice == 5
    assert args.hfcores == 8
    assert args.tscores == 4


def test_parse_include_exclude():
    """--include and --exclude accumulate multiple values into lists."""
    with patch.object(sys, "argv", [
        "gents", "/data/input",
        "--include", "*/atm/*", "--include", "*/ocn/*",
        "--exclude", "*/rest/*", "--exclude", "*/logs/*"
    ]):
        args = parse_arguments()
    assert args.include == ["*/atm/*", "*/ocn/*"]
    assert args.exclude == ["*/rest/*", "*/logs/*"]


def test_parse_missing_required_exits():
    """Omitting hf_head_dir causes argparse to exit with a non-zero code."""
    with patch.object(sys, "argv", ["run_gents"]):
        with pytest.raises(SystemExit) as exc:
            parse_arguments()
    assert exc.value.code != 0


def test_parse_version_exits(capsys):
    """-V / --version prints the version string and exits with code 0."""
    with patch.object(sys, "argv", ["run_gents", "--version"]):
        with pytest.raises(SystemExit) as exc:
            parse_arguments()
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "gents" in (captured.out + captured.err).lower()


def test_main_verbose_prints_settings(capsys):
    """--verbose causes all active settings to be printed to stdout."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--verbose", "--model", "cesm3"]):
        with pytest.raises(FileNotFoundError) as exc:
            main()
    out = capsys.readouterr().out
    assert "/data/input" in out
    assert "cesm3" in out


def test_cli_simple_case(simple_case):
    """CLI produces expected time series for a simple case."""
    input_head_dir, output_head_dir = simple_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir)]):
        main()
    
    ts_paths = find_files(output_head_dir, "*.nc")

    assert len(ts_paths) == SIMPLE_NUM_VARS
    
    for path in ts_paths:
        assert "*" not in str(path)
        with GenTSDataStore(path, 'r') as ts_ds:
            assert ts_ds["time"].size == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds["time_bounds"].shape[0] == SIMPLE_NUM_TEST_HIST_FILES
            assert ts_ds.getncattr("gents_version") == get_version()
            assert is_monotonic(ts_ds["time"][:])
            var_name = str(path).split(".")[-3]
            assert ts_ds.getncattr("gents_command") == f"run_gents {str(input_head_dir)} -o {str(output_head_dir)}"


def test_cli_long_hf_slicing(long_case):
    """Changing slicing parameters has intended effect."""

    input_head_dir, output_head_dir = long_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir), "-sl", "5"]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) == LONG_TEST_NUM_HIST_FILES / 12 / 5