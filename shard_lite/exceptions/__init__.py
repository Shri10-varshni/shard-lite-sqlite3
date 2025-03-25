"""Exception classes for the sharding library."""

from shard_lite.exceptions.shard_exceptions import (
    ShardingError,
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
    CrossShardTransactionError
)

__all__ = [
    'ShardingError',
    'ConfigurationError',
    'ConnectionError',
    'ConnectionTimeoutError',
    'TooManyConnectionsError',
    'StrategyError',
    'InvalidKeyError',
    'ShardingRangeError',
    'QueryError',
    'QuerySyntaxError',
    'QueryExecutionError',
    'TransactionError',
    'TransactionAbortedError',
    'CrossShardTransactionError'
]
