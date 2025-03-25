import pytest
from shard_lite.core.shard_manager import ShardManager
from shard_lite.exceptions.shard_exceptions import ShardingError

@pytest.fixture
def shard_manager():
    return ShardManager()

def test_create_shard(shard_manager):
    # Test creating a new shard
    shard_id = shard_manager.create_shard()
    assert shard_id in shard_manager.list_shards()

def test_crud_operations(shard_manager):
    # Test basic CRUD operations
    shard_manager.insert({"id": 1, "name": "test"})
    results = shard_manager.select({"id": 1})
    assert len(results) == 1
    assert results[0]["name"] == "test"

    shard_manager.update({"id": 1}, {"name": "updated"})
    results = shard_manager.select({"id": 1})
    assert results[0]["name"] == "updated"

    shard_manager.delete({"id": 1})
    results = shard_manager.select({"id": 1})
    assert len(results) == 0

def test_transaction(shard_manager):
    # Test transaction execution
    operations = [
        {
            "type": "insert",
            "params": {"data": {"id": 1, "name": "test"}}
        },
        {
            "type": "update",
            "params": {"criteria": {"id": 1}, "data": {"name": "updated"}}
        }
    ]
    shard_manager.execute_transaction(operations)
    results = shard_manager.select({"id": 1})
    assert results[0]["name"] == "updated"

def test_handler_types(shard_manager):
    # Test different handler types
    default_handler = shard_manager.get_handler('default')
    batch_handler = shard_manager.get_handler('batch')
    assert default_handler != batch_handler

def test_cleanup(shard_manager):
    # Test resource cleanup
    shard_manager.close()
    # Additional assertions could be added here
