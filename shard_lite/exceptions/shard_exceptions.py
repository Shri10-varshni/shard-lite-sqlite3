from shard_lite.utils.logger import Logger

class ShardingError(Exception):
    """
    Base exception for all library errors.

    Attributes:
        message (str): Error message.
        error_code (int): Numeric error code.
        context (dict): Additional context information.
    """

    def __init__(self, message, error_code=1000, **context):
        self.message = message
        self.error_code = error_code
        self.context = context
        super().__init__(self.__str__())
        Logger().error(self.__str__(), component="exceptions")

    def __str__(self):
        context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
        return f"[{self.error_code}] {self.message} ({context_str})"


class ConfigurationError(ShardingError):
    def __init__(self, message="Invalid or missing configuration", **context):
        super().__init__(message, error_code=1100, **context)


class ConnectionError(ShardingError):
    def __init__(self, message="Database connection issue", **context):
        super().__init__(message, error_code=1200, **context)


class ConnectionTimeoutError(ConnectionError):
    def __init__(self, message="Connection timed out", **context):
        super().__init__(message, error_code=1210, **context)


class TooManyConnectionsError(ConnectionError):
    def __init__(self, message="Connection pool exhausted", **context):
        super().__init__(message, error_code=1220, **context)


class StrategyError(ShardingError):
    def __init__(self, message="Sharding strategy issue", **context):
        super().__init__(message, error_code=1300, **context)


class InvalidKeyError(StrategyError):
    def __init__(self, message="Sharding key validation failed", **context):
        super().__init__(message, error_code=1310, **context)


class ShardingRangeError(StrategyError):
    def __init__(self, message="Invalid range in RangeStrategy", **context):
        super().__init__(message, error_code=1320, **context)


class QueryError(ShardingError):
    def __init__(self, message="Query execution issue", **context):
        super().__init__(message, error_code=1400, **context)


class QuerySyntaxError(QueryError):
    def __init__(self, message="Malformed SQL query", **context):
        super().__init__(message, error_code=1410, **context)


class QueryExecutionError(QueryError):
    def __init__(self, message="Runtime query failure", **context):
        super().__init__(message, error_code=1420, **context)


class TransactionError(ShardingError):
    def __init__(self, message="Transaction management issue", **context):
        super().__init__(message, error_code=1500, **context)


class TransactionAbortedError(TransactionError):
    def __init__(self, message="Transaction aborted", **context):
        super().__init__(message, error_code=1510, **context)


class CrossShardTransactionError(TransactionError):
    def __init__(self, message="Issue with multi-shard transaction", **context):
        super().__init__(message, error_code=1520, **context)
