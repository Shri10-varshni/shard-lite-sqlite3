import pytest
from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.utils.config import Config
from shard_lite.exceptions.shard_exceptions import ConnectionTimeoutError

@pytest.fixture
def connection_pool():
    config = Config(connection_timeout=5, pool_size=3, shard_base_path="./test_shards")
    return ConnectionPool(config)

def test_get_connection(connection_pool):
    # Test getting a connection from the pool
    connection = connection_pool.get_connection("shard_1")
    assert connection is not None
    connection_pool.release_connection(connection, "shard_1")

def test_release_connection(connection_pool):
    # Test releasing a connection back to the pool
    connection = connection_pool.get_connection("shard_1")
    connection_pool.release_connection(connection, "shard_1")
    assert connection_pool.get_pool_status()["shard_1"] == 3

def test_connection_timeout(connection_pool):
    # Test connection timeout when pool is exhausted
    connections = [connection_pool.get_connection("shard_1") for _ in range(3)]
    with pytest.raises(ConnectionTimeoutError):
        connection_pool.get_connection("shard_1")
    for conn in connections:
        connection_pool.release_connection(conn, "shard_1")

def test_close_shard_connections(connection_pool):
    # Test closing all connections for a shard
    connection_pool.get_connection("shard_1")
    connection_pool.close_shard_connections("shard_1")
    assert "shard_1" not in connection_pool.get_pool_status()

def test_close_all(connection_pool):
    # Test closing all connections in the pool
    connection_pool.get_connection("shard_1")
    connection_pool.get_connection("shard_2")
    connection_pool.close_all()
    assert connection_pool.get_pool_status() == {}
