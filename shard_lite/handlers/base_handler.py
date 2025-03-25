from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from shard_lite.utils.logger import Logger
from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.exceptions.shard_exceptions import ShardingError

class BaseHandler(ABC):
    """
    Abstract base class for all CRUD operation handlers.

    Defines the interface and utility methods for CRUD operations.
    """

    def __init__(self, query_router: BaseStrategy, connection_pool: ConnectionPool, logger: Optional[Logger] = None):
        """
        Initialize the handler with dependencies.

        Args:
            query_router (BaseStrategy): Strategy for determining shard(s).
            connection_pool (ConnectionPool): Connection pool for managing connections.
            logger (Logger, optional): Logger instance for logging operations.
        """
        self.query_router = query_router
        self.connection_pool = connection_pool
        self.logger = logger or Logger()

    @abstractmethod
    def insert(self, data: Dict[str, Any]) -> None:
        """
        Insert data into appropriate shard(s).

        Args:
            data (Dict[str, Any]): Data to insert.
        """
        pass

    @abstractmethod
    def select(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Query data from shard(s).

        Args:
            criteria (Dict[str, Any]): Query criteria.

        Returns:
            List[Dict[str, Any]]: Query results.
        """
        pass

    @abstractmethod
    def update(self, criteria: Dict[str, Any], data: Dict[str, Any]) -> None:
        """
        Update data in shard(s).

        Args:
            criteria (Dict[str, Any]): Query criteria.
            data (Dict[str, Any]): Data to update.
        """
        pass

    @abstractmethod
    def delete(self, criteria: Dict[str, Any]) -> None:
        """
        Delete data from shard(s).

        Args:
            criteria (Dict[str, Any]): Query criteria.
        """
        pass

    def _execute_on_shards(self, operation: str, shards: Optional[List[str]] = None, **kwargs) -> Any:
        """
        Execute an operation on multiple shards.

        Args:
            operation (str): Operation to execute (e.g., "insert", "select").
            shards (List[str], optional): List of shard IDs. Defaults to all shards.
            **kwargs: Additional arguments for the operation.

        Returns:
            Any: Operation result.
        """
        shards = shards or self.query_router.get_all_shards()
        connections = self._acquire_connections(shards)
        results = []

        try:
            for shard_id, connection in connections.items():
                self.logger.info(f"Executing {operation} on shard {shard_id}")
                result = getattr(self, f"_{operation}_on_shard")(connection, **kwargs)
                if result is not None:
                    results.append(result)
        finally:
            self._release_connections(connections)

        return results if operation == "select" else None

    def _validate_data(self, data: Dict[str, Any]) -> None:
        """
        Verify that the data structure is valid.

        Args:
            data (Dict[str, Any]): Data to validate.

        Raises:
            ShardingError: If the data is invalid.
        """
        if not isinstance(data, dict):
            raise ShardingError("Data must be a dictionary", context={"data": data})

    def _validate_criteria(self, criteria: Dict[str, Any]) -> None:
        """
        Verify that the query criteria is valid.

        Args:
            criteria (Dict[str, Any]): Criteria to validate.

        Raises:
            ShardingError: If the criteria is invalid.
        """
        if not isinstance(criteria, dict):
            raise ShardingError("Criteria must be a dictionary", context={"criteria": criteria})

    def _acquire_connections(self, shard_ids: List[str]) -> Dict[str, Any]:
        """
        Get connections to specified shards.

        Args:
            shard_ids (List[str]): List of shard IDs.

        Returns:
            Dict[str, Any]: Mapping of shard IDs to connections.
        """
        connections = {}
        for shard_id in shard_ids:
            connections[shard_id] = self.connection_pool.get_connection(shard_id)
        return connections

    def _release_connections(self, connections: Dict[str, Any]) -> None:
        """
        Return connections to the pool.

        Args:
            connections (Dict[str, Any]): Mapping of shard IDs to connections.
        """
        for shard_id, connection in connections.items():
            self.connection_pool.release_connection(connection, shard_id)
