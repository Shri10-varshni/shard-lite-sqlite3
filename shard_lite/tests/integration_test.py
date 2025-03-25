import pytest
from shard_lite import create_shard_manager, Config

def test_integration():
    # Initialize configuration
    config = Config(
        connection_timeout=30,
        pool_size=5,
        shard_base_path="./test_shards"
    )

    # Create shard manager
    manager = create_shard_manager(config, strategy='hash')

    try:
        # Create shards
        shard1 = manager.create_shard()
        shard2 = manager.create_shard()
        assert len(manager.list_shards()) == 2

        # Insert test data
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
            {"id": 3, "name": "test3"}
        ]
        manager.insert(test_data, handler_type='batch')

        # Query data
        result = manager.select({"name": "test1"})
        assert len(result) == 1
        assert result[0]["id"] == 1

        # Update data
        manager.update({"id": 1}, {"name": "updated"})
        result = manager.select({"id": 1})
        assert result[0]["name"] == "updated"

        # Test transaction
        operations = [
            {
                "type": "insert",
                "params": {"data": {"id": 4, "name": "test4"}}
            },
            {
                "type": "update",
                "params": {
                    "criteria": {"id": 4},
                    "data": {"name": "updated4"}
                }
            }
        ]
        manager.execute_transaction(operations)

        # Verify transaction results
        result = manager.select({"id": 4})
        assert result[0]["name"] == "updated4"

    finally:
        # Clean up
        manager.close()
