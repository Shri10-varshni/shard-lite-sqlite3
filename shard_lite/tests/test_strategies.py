import pytest
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.utils.config import Config
from shard_lite.exceptions.shard_exceptions import StrategyError

class DummyStrategy(BaseStrategy):
    """Dummy implementation of BaseStrategy for testing purposes."""
    def get_shard_for_key(self, key):
        return "shard_1"

    def get_shards_for_query(self, criteria):
        return ["shard_1", "shard_2"]

    def create_shard(self, shard_id):
        pass

    def add_shard(self, shard_id):
        pass

    def remove_shard(self, shard_id):
        pass

@pytest.fixture
def dummy_strategy():
    config = Config(active_shards=["shard_1", "shard_2"], shard_base_path="./shards")
    return DummyStrategy(config)

def test_validate_key(dummy_strategy):
    # Test valid keys
    dummy_strategy.validate_key("key1")
    dummy_strategy.validate_key(123)

    # Test invalid keys
    with pytest.raises(StrategyError):
        dummy_strategy.validate_key(None)
    with pytest.raises(StrategyError):
        dummy_strategy.validate_key([])

def test_get_all_shards(dummy_strategy):
    # Test retrieving all active shards
    assert dummy_strategy.get_all_shards() == ["shard_1", "shard_2"]

def test_get_shard_file_path(dummy_strategy):
    # Test generating shard file paths
    assert dummy_strategy.get_shard_file_path("shard_1") == "./shards/shard_1.db"

def test_log_strategy_operation(dummy_strategy, caplog):
    # Test logging strategy operations
    dummy_strategy._log_strategy_operation("Test operation", key="value")
    assert "Test operation" in caplog.text
    assert "key=value" in caplog.text
