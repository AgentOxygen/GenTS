import sys
import pytest
import yaml
from pathlib import Path
from unittest.mock import patch
from gents.cli import check_config, main


CONFIG_DIR = Path(__file__).parents[1] / "configs"
REQUIRED_KEYS = ["version", "model", "input_hf", "output_ts"]


def _valid_config():
    """A minimal config dictionary containing every required top-level key."""
    return {
        "version": 1,
        "model": "Example",
        "input_hf": {"match": "*.nc"},
        "output_ts": {},
    }


# ---------------------------------------------------------------------------
# check_config unit tests
# ---------------------------------------------------------------------------

def test_check_config_accepts_valid():
    """A config with all required keys passes validation without raising."""
    check_config(_valid_config())


@pytest.mark.parametrize("missing_key", REQUIRED_KEYS)
def test_check_config_rejects_missing_key(missing_key):
    """Removing any required top-level key causes check_config to raise."""
    config = _valid_config()
    del config[missing_key]
    with pytest.raises(AssertionError):
        check_config(config)


def test_check_config_rejects_empty():
    """An empty config is missing every required key and fails validation."""
    with pytest.raises(AssertionError):
        check_config({})


# ---------------------------------------------------------------------------
# Bundled YAML configuration files
# ---------------------------------------------------------------------------

def _bundled_yaml_files():
    """All YAML configuration files shipped in the configs directory."""
    return sorted(CONFIG_DIR.glob("*.yaml"))


def test_bundled_yaml_files_exist():
    """At least the example and CESM3 configuration files are bundled."""
    names = {path.name for path in _bundled_yaml_files()}
    assert "gents_example.yaml" in names
    assert "gents_cesm3.yaml" in names


@pytest.mark.parametrize("yaml_path", _bundled_yaml_files(), ids=lambda p: p.name)
def test_bundled_yaml_is_parseable(yaml_path):
    """Every bundled YAML file parses into a dictionary."""
    with open(yaml_path, "r") as file:
        config = yaml.safe_load(file)
    assert isinstance(config, dict)


@pytest.mark.parametrize("yaml_path", _bundled_yaml_files(), ids=lambda p: p.name)
def test_bundled_yaml_passes_check_config(yaml_path):
    """Every bundled YAML file satisfies the required-key contract."""
    with open(yaml_path, "r") as file:
        config = yaml.safe_load(file)
    check_config(config)


@pytest.mark.parametrize("yaml_path", _bundled_yaml_files(), ids=lambda p: p.name)
def test_bundled_yaml_input_hf_has_match(yaml_path):
    """The input_hf section always defines a 'match' glob used to find history files."""
    with open(yaml_path, "r") as file:
        config = yaml.safe_load(file)
    assert "match" in config["input_hf"]
    assert isinstance(config["input_hf"]["match"], str)


def test_example_yaml_structure():
    """The example config exposes the include/exclude/slicing structure the CLI reads."""
    with open(CONFIG_DIR / "gents_example.yaml", "r") as file:
        config = yaml.safe_load(file)

    assert isinstance(config["input_hf"]["include"], list)
    assert isinstance(config["input_hf"]["exclude"], list)
    assert isinstance(config["input_hf"]["slicing"], list)
    for slice_batch in config["input_hf"]["slicing"]:
        assert "slice_size_years" in slice_batch


def test_cesm3_yaml_has_compression_section():
    """The CESM3 config defines an output compression batch with alg and level."""
    with open(CONFIG_DIR / "gents_cesm3.yaml", "r") as file:
        config = yaml.safe_load(file)

    comp_batches = config["output_ts"]["compression"]
    assert isinstance(comp_batches, list) and len(comp_batches) > 0
    for batch in comp_batches:
        assert "alg" in batch
        assert "level" in batch


# ---------------------------------------------------------------------------
# Model -> configuration selection in main()
# ---------------------------------------------------------------------------

def test_main_unknown_model_raises():
    """Requesting a model with no bundled configuration raises a ValueError."""
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--model", "not_a_model"]):
        with pytest.raises(ValueError) as exc:
            main()
    assert "not_a_model" in str(exc.value).lower()


def test_main_model_is_case_insensitive():
    """--model is lower-cased before lookup, so 'CESM3' selects the CESM3 config."""
    # The config loads successfully; execution then fails on the missing input
    # directory, confirming the (valid) config was accepted for the given model.
    with patch.object(sys, "argv", ["run_gents", "/data/input", "--model", "CESM3"]):
        with pytest.raises(FileNotFoundError):
            main()
