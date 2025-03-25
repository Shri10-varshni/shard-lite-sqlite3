import pytest
from shard_lite.strategies.range_strategy import RangeStrategy
from shard_lite.utils.config import Config
from shard_lite.exceptions.shard_exceptions import StrategyError

@pytest.fixture
def range_strategy():
    config = Config()
    ranges = {(0, 10): "shard_1", (10, 20): "shard_2"}
    return RangeStrategy(config, ranges)

def test_get_shard_for_key(range_strategy):
    # Test finding the shard for a key
    assert range_strategy.get_shard_for_key(5) == "shard_1"
    assert range_strategy.get_shard_for_key(15) == "shard_2"
    with pytest.raises(StrategyError):
        range_strategy.get_shard_for_key(25)

def test_add_and_remove_shard(range_strategy):
    # Test adding and removing shards
    range_strategy.add_shard("shard_3", 20, 30)
    assert range_strategy.get_shard_for_key(25) == "shard_3"
    range_strategy.remove_shard("shard_3")
    with pytest.raises(StrategyError):
        range_strategy.get_shard_for_key(25)

def test_split_range(range_strategy):
    # Test splitting a range
    range_strategy.split_range(0, 5, "shard_3")
    assert range_strategy.get_shard_for_key(3) == "shard_3"
    assert range_strategy.get_shard_for_key(7) == "shard_1"

def test_merge_ranges(range_strategy):
    # Test merging ranges
    range_strategy.add_shard("shard_3", 20, 30)
    range_strategy.merge_ranges((10, 20), (20, 30), "shard_4")
    assert range_strategy.get_shard_for_key(25) == "shard_4"
    assert range_strategy.get_shard_for_key(15) == "shard_4"
