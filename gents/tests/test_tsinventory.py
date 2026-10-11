from pathlib import Path
from gents.tests.test_cases import *
from gents.hfcollection import HFCollection
from gents.timeseries import TSCollection
from gents.tsinventory import TSInventory, get_ts_meta_from_path, get_time_sort_key, is_gents_output
import pytest


@pytest.fixture
def two_batch_output(continued_case):
    """Time series output written in two runs, so each variable has two files covering consecutive periods."""
    input_head_dir, output_head_dir, extend = continued_case
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).execute()
    extend()
    TSCollection(HFCollection(input_head_dir), str(output_head_dir)).skip_existing().execute()
    return output_head_dir


def test_missing_and_empty_dir(tmp_path):
    """A missing or empty directory gives an empty inventory instead of raising."""
    for head_dir in [tmp_path / "does_not_exist", tmp_path]:
        inventory = TSInventory(str(head_dir))
        assert len(inventory) == 0
        assert inventory.is_pulled()
        assert inventory.get_variables() == {}


def test_discover_and_pull(two_batch_output):
    """Files are found unpulled, then pulled and grouped into one sorted entry per variable."""
    inventory = TSInventory(str(two_batch_output))
    assert len(inventory) == CONTINUED_NUM_VARS * 2
    assert not inventory.is_pulled()
    assert all(meta is None for meta in inventory.values())

    assert inventory.pull_metadata(show_progress=False) is inventory
    assert inventory.is_pulled()
    assert all(is_gents_output(meta) for meta in inventory.values())

    variables = inventory.get_variables()
    assert len(variables) == CONTINUED_NUM_VARS
    for key, paths in variables.items():
        assert key.endswith(".month_1")
        assert len(paths) == 2
        keys = [get_time_sort_key(inventory[path]) for path in paths]
        assert keys == sorted(keys)
        assert keys[0] != keys[1]


def test_parallel_pull_matches_serial(two_batch_output):
    """Pulling with a process pool yields the same variable grouping as a serial pull."""
    serial = TSInventory(str(two_batch_output))
    parallel = TSInventory(str(two_batch_output), num_processes=2)
    serial.pull_metadata(show_progress=False)
    parallel.pull_metadata(show_progress=False)
    assert serial.get_variables() == parallel.get_variables()


def test_unfinished_and_foreign_files_are_dropped(two_batch_output):
    """Files without the gents_version stamp (unfinished writes, history files) are removed on pull."""
    foreign = f"{two_batch_output}/foreign.nc"
    generate_history_file(foreign, [15.0], [[0.0, 30.0]])

    inventory = TSInventory(str(two_batch_output))
    assert len(inventory) == CONTINUED_NUM_VARS * 2 + 1
    inventory.pull_metadata(show_progress=False)

    assert len(inventory) == CONTINUED_NUM_VARS * 2
    assert Path(foreign) not in inventory

    unvalidated = TSInventory(str(two_batch_output)).pull_metadata(check_valid=False, show_progress=False)
    assert len(unvalidated) == CONTINUED_NUM_VARS * 2 + 1


def test_filters_are_immutable_and_prune_variables(two_batch_output):
    """include()/exclude() return new inventories and a cached variable grouping is pruned, not rebuilt."""
    inventory = TSInventory(str(two_batch_output)).pull_metadata(show_progress=False)
    variables = inventory.get_variables()
    var_key = sorted(variables)[0]
    var_name = var_key.split(".")[0]

    only_var = inventory.include(f"*.{var_name}.*")
    without_var = inventory.exclude(f"*.{var_name}.*")
    assert only_var is not inventory
    assert len(inventory) == CONTINUED_NUM_VARS * 2
    assert len(only_var) == 2
    assert len(without_var) == (CONTINUED_NUM_VARS - 1) * 2
    assert list(only_var.get_variables()) == [var_key]
    assert only_var.get_variables()[var_key] == variables[var_key]
    assert var_key not in without_var.get_variables()
    assert len(inventory.include([])) == 0


def test_get_ts_meta_from_path_keeps_variable_attrs(two_batch_output):
    """Time series metadata keeps per-variable attributes, which history-file metadata drops."""
    inventory = TSInventory(str(two_batch_output))
    path = next(iter(inventory))
    meta = get_ts_meta_from_path(path)
    var_name = meta.get_primary_variables()[0]
    assert isinstance(meta.get_variable_attrs(var_name), dict)
