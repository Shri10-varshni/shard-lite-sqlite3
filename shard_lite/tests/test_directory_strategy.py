import pytest
from shard_lite.strategies.directory_strategy import DirectoryStrategy
from shard_lite.utils.config import Config
from shard_lite.exceptions.shard_exceptions import StrategyError

@pytest.fixture
def directory_strategy():
    config = Config(directory_path="./test_directory.json")
    return DirectoryStrategy(config)

def test_get_shard_for_key(directory_strategy):
    # Test looking up a shard for a key
    directory_strategy.add_mapping("key1", "shard_1")
    assert directory_strategy.get_shard_for_key("key1") == "shard_1"
    with pytest.raises(StrategyError):
        directory_strategy.get_shard_for_key("key2")

def test_add_and_remove_mapping(directory_strategy):
    # Test adding and removing mappings
    directory_strategy.add_mapping("key1", "shard_1")
    assert directory_strategy.get_shard_for_key("key1") == "shard_1"
    directory_strategy.remove_mapping("key1")
    with pytest.raises(StrategyError):
        directory_strategy.get_shard_for_key("key1")

def test_get_mappings_for_shard(directory_strategy):
    # Test retrieving all keys for a shard
    directory_strategy.add_mapping("key1", "shard_1")
    directory_strategy.add_mapping("key2", "shard_1")
    assert set(directory_strategy.get_mappings_for_shard("shard_1")) == {"key1", "key2"}

def test_import_mappings(directory_strategy):
    # Test bulk importing mappings
    mappings = {"key1": "shard_1", "key2": "shard_2"}
    directory_strategy.import_mappings(mappings)
    assert directory_strategy.get_shard_for_key("key1") == "shard_1"
    assert directory_strategy.get_shard_for_key("key2") == "shard_2"

def test_lru_cache(directory_strategy):
    # Test LRU caching behavior
    directory_strategy.add_mapping("key1", "shard_1")
    directory_strategy.add_mapping("key2", "shard_2")
    directory_strategy.get_shard_for_key("key1")  # Access key1
    directory_strategy.add_mapping("key3", "shard_3")  # Add a new key
    assert "key1" in directory_strategy.cache
    assert "key2" in directory_strategy.cache
