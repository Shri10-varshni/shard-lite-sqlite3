import pytest
from shard_lite.handlers.default_handler import DefaultHandler
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.exceptions.shard_exceptions import QueryExecutionError, ShardingError

class DummyQueryRouter(BaseStrategy):
    """Dummy implementation of BaseStrategy for testing purposes."""
    def get_shard_for_key(self, key):
        return "shard_1"

    def get_shards_for_query(self, criteria):
        return ["shard_1"]

    def create_shard(self, shard_id):
        pass

    def add_shard(self, shard_id):
        pass

    def remove_shard(self, shard_id):
        pass

@pytest.fixture
def default_handler():
    query_router = DummyQueryRouter(None)
    connection_pool = ConnectionPool(None)
    return DefaultHandler(query_router, connection_pool)

def test_insert(default_handler):
    # Test inserting valid data
    default_handler.insert({"id": 1, "name": "test"})

    # Test inserting invalid data
    with pytest.raises(ShardingError):
        default_handler.insert(None)

def test_select(default_handler):
    # Test selecting with valid criteria
    results = default_handler.select({"id": 1})
    assert isinstance(results, list)

    # Test selecting with invalid criteria
    with pytest.raises(ShardingError):
        default_handler.select(None)

def test_update(default_handler):
    # Test updating with valid criteria and data
    default_handler.update({"id": 1}, {"name": "updated"})

    # Test updating with invalid criteria
    with pytest.raises(ShardingError):
        default_handler.update(None, {"name": "updated"})

    # Test updating with invalid data
    with pytest.raises(ShardingError):
        default_handler.update({"id": 1}, None)

def test_delete(default_handler):
    # Test deleting with valid criteria
    default_handler.delete({"id": 1})

    # Test deleting with invalid criteria
    with pytest.raises(ShardingError):
        default_handler.delete(None)
