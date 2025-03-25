import pytest
from shard_lite.handlers.batch_handler import BatchHandler
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.exceptions.shard_exceptions import ShardingError

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
def batch_handler():
    query_router = DummyQueryRouter(None)
    connection_pool = ConnectionPool(None)
    return BatchHandler(query_router, connection_pool)

def test_insert_batch(batch_handler):
    # Test inserting multiple records in batch
    data = [{"id": i, "name": f"test_{i}"} for i in range(10)]
    batch_handler.insert(data)

def test_select_batch(batch_handler):
    # Test selecting multiple records in batch
    criteria = [{"id": i} for i in range(10)]
    results = batch_handler.select(criteria)
    assert isinstance(results, list)

def test_update_batch(batch_handler):
    # Test updating multiple records in batch
    criteria = [{"id": i} for i in range(10)]
    data = [{"name": f"updated_{i}"} for i in range(10)]
    batch_handler.update(criteria, data)

def test_delete_batch(batch_handler):
    # Test deleting multiple records in batch
    criteria = [{"id": i} for i in range(10)]
    batch_handler.delete(criteria)

def test_chunk_data(batch_handler):
    # Test chunking large datasets
    data = list(range(100))
    chunks = list(batch_handler._chunk_data(data, 10))
    assert len(chunks) == 10
    assert all(len(chunk) == 10 for chunk in chunks)
