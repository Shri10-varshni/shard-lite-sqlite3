import pytest
from shard_lite.strategies.hash_strategy import HashStrategy
from shard_lite.utils.config import Config
from shard_lite.exceptions.shard_exceptions import StrategyError

@pytest.fixture
def hash_strategy():
    config = Config(active_shards=["shard_1", "shard_2", "shard_3"])
    return HashStrategy(config)

def test_get_shard_for_key(hash_strategy):
    # Test mapping keys to shards
    shard = hash_strategy.get_shard_for_key("test_key")
    assert shard in ["shard_1", "shard_2", "shard_3"]

def test_add_and_remove_shard(hash_strategy):
    # Test adding and removing shards
    hash_strategy.add_shard("shard_4")
    assert "shard_4" in hash_strategy.get_all_shards()
    hash_strategy.remove_shard("shard_4")
    assert "shard_4" not in hash_strategy.get_all_shards()

def test_invalid_key(hash_strategy):
    # Test handling of invalid keys
    with pytest.raises(StrategyError):
        hash_strategy.get_shard_for_key(None)
    with pytest.raises(StrategyError):
        hash_strategy.get_shard_for_key([])

def test_rebalance_shards(hash_strategy):
    # Test shard rebalancing (placeholder)
    hash_strategy.rebalance_shards()
    # Add assertions as rebalance logic is implemented
    pass
