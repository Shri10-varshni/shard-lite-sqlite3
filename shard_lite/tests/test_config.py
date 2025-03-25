import pytest
import json
import yaml
import tempfile
import os
from shard_lite.utils.config import Config

@pytest.fixture
def temp_json_config():
    config = {
        "connection_timeout": 60,
        "pool_size": 10,
        "mutable": True
    }
    with tempfile.NamedTemporaryFile(suffix='.json', mode='w', delete=False) as f:
        json.dump(config, f)
        f.flush()
        yield f.name
    os.unlink(f.name)

@pytest.fixture
def temp_yaml_config():
    config = {
        "connection_timeout": 45,
        "pool_size": 8,
        "mutable": False
    }
    with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w', delete=False) as f:
        yaml.dump(config, f)
        f.flush()
        yield f.name
    os.unlink(f.name)

def test_load_from_file(temp_json_config, temp_yaml_config):
    # Test JSON config loading
    json_config = Config(config_file=temp_json_config)
    assert json_config.get("connection_timeout") == 60
    assert json_config.get("pool_size") == 10
    assert json_config.get("mutable") is True

    # Test YAML config loading
    yaml_config = Config(config_file=temp_yaml_config)
    assert yaml_config.get("connection_timeout") == 45
    assert yaml_config.get("pool_size") == 8
    assert yaml_config.get("mutable") is False

def test_load_from_parameters():
    # Test direct parameter initialization
    config = Config(connection_timeout=50, pool_size=15)
    assert config.get("connection_timeout") == 50
    assert config.get("pool_size") == 15
    assert config.get("mutable") is False

def test_validation_errors():
    # Test invalid connection timeout
    with pytest.raises(ValueError):
        Config(connection_timeout=-1)

    # Test invalid pool size
    with pytest.raises(ValueError):
        Config(pool_size=0)

    # Test invalid mutable type
    with pytest.raises(ValueError):
        Config(mutable="true")  # Should be boolean

def test_default_values():
    # Test default configuration values
    config = Config()
    assert config.get("connection_timeout") == 30
    assert config.get("pool_size") == 5
    assert config.get("mutable") is False

def test_immutability():
    # Test immutable configuration
    config = Config(mutable=False)
    with pytest.raises(RuntimeError):
        config.set("connection_timeout", 100)

    # Test mutable configuration
    mutable_config = Config(mutable=True)
    mutable_config.set("connection_timeout", 100)
    assert mutable_config.get("connection_timeout") == 100

