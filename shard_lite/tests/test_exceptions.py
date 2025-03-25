import pytest
from shard_lite.exceptions.shard_exceptions import (
    ConfigurationError,
    ConnectionError,
    ConnectionTimeoutError,
    TooManyConnectionsError,
    StrategyError,
    InvalidKeyError,
    ShardingRangeError,
    QueryError,
    QuerySyntaxError,
    QueryExecutionError,
    TransactionError,
    TransactionAbortedError,
    CrossShardTransactionError,
)

def test_configuration_error():
    # Test raising and handling ConfigurationError.
    with pytest.raises(ConfigurationError):
        raise ConfigurationError(context={"key": "value"})

def test_connection_error():
    # Test raising and handling ConnectionError and its subclasses.
    with pytest.raises(ConnectionTimeoutError):
        raise ConnectionTimeoutError(context={"timeout": 30})
    with pytest.raises(TooManyConnectionsError):
        raise TooManyConnectionsError(context={"pool_size": 5})

def test_strategy_error():
    # Test raising and handling StrategyError and its subclasses.
    with pytest.raises(InvalidKeyError):
        raise InvalidKeyError(context={"key": "invalid_key"})
    with pytest.raises(ShardingRangeError):
        raise ShardingRangeError(context={"range": "invalid_range"})

def test_query_error():
    # Test raising and handling QueryError and its subclasses.
    with pytest.raises(QuerySyntaxError):
        raise QuerySyntaxError(context={"query": "SELECT * FROM"})
    with pytest.raises(QueryExecutionError):
        raise QueryExecutionError(context={"query": "SELECT * FROM table"})

def test_transaction_error():
    # Test raising and handling TransactionError and its subclasses.
    with pytest.raises(TransactionAbortedError):
        raise TransactionAbortedError(context={"transaction_id": 123})
    with pytest.raises(CrossShardTransactionError):
        raise CrossShardTransactionError(context={"shards": ["shard1", "shard2"]})
