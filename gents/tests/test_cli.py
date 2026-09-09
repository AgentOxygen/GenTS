import pytest
import sys
from unittest.mock import patch, MagicMock
from gents.cli import parse_arguments, main
from gents.tests.test_cases import *
from gents.datastore import GenTSDataStore
from gents.hfcollection import find_files
from gents.timeseries import TSCollection
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
    assert args.compression is None
    assert args.level is None
    assert args.memory_limit_gb is None


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


def test_parse_compression_and_level():
    """--compression and --level capture the algorithm name and integer level."""
    with patch.object(sys, "argv", [
        "run_gents", "/data/input", "--compression", "zlib", "--level", "3"
    ]):
        args = parse_arguments()
    assert args.compression == "zlib"
    assert args.level == 3


def test_parse_compression_without_level():
    """--compression may be parsed on its own; --level stays at its None default."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--compression", "zstd"]):
        args = parse_arguments()
    assert args.compression == "zstd"
    assert args.level is None


def test_parse_memory_limit():
    """--memory-limit captures a float GB value."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--memory-limit", "2.5"]):
        args = parse_arguments()
    assert args.memory_limit_gb == 2.5


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


def test_main_compression_without_level_raises():
    """--compression without --level raises a ValueError before any work is done."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--compression", "zlib"]):
        with pytest.raises(ValueError) as exc:
            main()
    assert "level" in str(exc.value).lower()


def test_main_verbose_prints_compression(capsys):
    """--verbose reports the selected compression method and level."""
    with patch.object(sys, "argv", [
        "run_gents", "/data/input", "--verbose", "--compression", "zlib", "--level", "5"
    ]):
        with pytest.raises(FileNotFoundError):
            main()
    out = capsys.readouterr().out
    assert "Compression method" in out
    assert "zlib" in out
    assert "Compression level" in out
    assert "5" in out


def test_main_verbose_prints_memory_limit(capsys):
    """--verbose reports the selected memory limit."""
    with patch.object(sys, "argv", [
        "run_gents", "/data/input", "--verbose", "--memory-limit", "4.0"
    ]):
        with pytest.raises(FileNotFoundError):
            main()
    out = capsys.readouterr().out
    assert "Memory limit" in out
    assert "4.0" in out


def test_main_memory_limit_forwarded_to_execute(simple_case):
    """--memory-limit is converted from GB to bytes and forwarded to TSCollection.execute()."""
    input_head_dir, output_head_dir = simple_case
    with patch.object(TSCollection, "execute") as mock_execute:
        mock_execute.return_value = []
        with patch.object(sys, "argv", [
            "run_gents", str(input_head_dir), "-o", str(output_head_dir), "--memory-limit", "2"
        ]):
            main()
    assert mock_execute.call_args.kwargs["memory_limit_bytes"] == 2 * (1024**3)


def test_main_no_memory_limit_forwards_default(simple_case):
    """Without --memory-limit, TSCollection.execute() receives the bounded per-worker default."""
    from gents.timeseries import DEFAULT_MEMORY_LIMIT_BYTES
    input_head_dir, output_head_dir = simple_case
    with patch.object(TSCollection, "execute") as mock_execute:
        mock_execute.return_value = []
        with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir)]):
            main()
    assert mock_execute.call_args.kwargs["memory_limit_bytes"] == DEFAULT_MEMORY_LIMIT_BYTES


def test_cli_compression_applied(simple_case):
    """--compression / --level cause output time-series variables to be compressed."""
    input_head_dir, output_head_dir = simple_case
    with patch.object(sys, "argv", [
        "run_gents", str(input_head_dir), "-o", str(output_head_dir),
        "--compression", "zlib", "--level", "2"
    ]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        var_name = str(path).split(".")[-3]
        with GenTSDataStore(path, 'r') as ts_ds:
            filters = ts_ds[var_name].filters()
            assert filters["zlib"] is True
            assert filters["complevel"] == 2


def test_cli_no_compression_by_default(simple_case):
    """Without --compression, output variables are written uncompressed."""
    input_head_dir, output_head_dir = simple_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir)]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) == SIMPLE_NUM_VARS

    for path in ts_paths:
        var_name = str(path).split(".")[-3]
        with GenTSDataStore(path, 'r') as ts_ds:
            filters = ts_ds[var_name].filters()
            assert filters["zlib"] is False
            assert filters["complevel"] == 0


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

def test_cli_exclude_filters_history_files(structured_case):
    """--exclude reaches the HFCollection and keeps matching history files out of the output."""
    input_head_dir, output_head_dir = structured_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir),
                                    "--exclude", "*/0_dir/*"]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) > 0
    for path in ts_paths:
        assert "/0_dir/" not in str(path)


def test_cli_include_narrows_to_matching_files(structured_case):
    """--include overrides the config default, restricting output to the matched subtree."""
    input_head_dir, output_head_dir = structured_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir),
                                    "--include", "*/0_dir/*"]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) > 0
    for path in ts_paths:
        assert "/0_dir/" in str(path)


def test_cli_append_keeps_config_filters(structured_case):
    """--append adds to the bundled config's filters instead of replacing them."""
    input_head_dir, output_head_dir = structured_case
    with patch.object(sys, "argv", ["run_gents", str(input_head_dir), "-o", str(output_head_dir),
                                    "--append", "--exclude", "*/0_dir/*"]):
        main()

    ts_paths = find_files(output_head_dir, "*.nc")
    assert len(ts_paths) > 0
    for path in ts_paths:
        assert "/0_dir/" not in str(path)
