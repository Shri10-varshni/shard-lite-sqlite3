from shard_lite.handlers.base_handler import BaseHandler
from shard_lite.handlers.default_handler import DefaultHandler
from shard_lite.exceptions.shard_exceptions import ShardingError
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Generator
from functools import partial

class BatchHandler(BaseHandler):
    """
    Handler optimized for bulk operations on SQLite shards.
    """

    def __init__(self, query_router, connection_pool, logger=None):
        """
        Initialize the BatchHandler.

        Args:
            query_router (BaseStrategy): Strategy for determining shard(s).
            connection_pool (ConnectionPool): Connection pool for managing connections.
            logger (Logger, optional): Logger instance for logging operations.
        """
        super().__init__(query_router, connection_pool, logger)
        self.default_handler = DefaultHandler(query_router, connection_pool, logger)

    def insert(self, data: List[Dict[str, Any]]) -> None:
        """
        Optimized bulk insert.

        Args:
            data (List[Dict[str, Any]]): List of records to insert.
        """
        self.insert_batch(data)

    def select(self, criteria: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Optimized bulk select.

        Args:
            criteria (List[Dict[str, Any]]): List of query criteria.

        Returns:
            List[Dict[str, Any]]: List of query results.
        """
        return self.select_batch(criteria)

    def update(self, criteria: List[Dict[str, Any]], data: List[Dict[str, Any]]) -> None:
        """
        Optimized bulk update.

        Args:
            criteria (List[Dict[str, Any]]): List of query criteria.
            data (List[Dict[str, Any]]): List of data to update.
        """
        self.update_batch(list(zip(criteria, data)))

    def delete(self, criteria: List[Dict[str, Any]]) -> None:
        """
        Optimized bulk delete.

        Args:
            criteria (List[Dict[str, Any]]): List of query criteria.
        """
        self.delete_batch(criteria)

    def insert_batch(self, data_list: List[Dict[str, Any]], batch_size: int = 100) -> None:
        """
        Insert multiple records efficiently in batches.

        Args:
            data_list (List[Dict[str, Any]]): List of records to insert.
            batch_size (int): Number of records per batch.
        """
        for chunk in self._chunk_data(data_list, batch_size):
            self._execute_in_parallel([partial(self.default_handler.insert, record) for record in chunk])

    def select_batch(self, criteria_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Select records matching multiple criteria.

        Args:
            criteria_list (List[Dict[str, Any]]): List of query criteria.

        Returns:
            List[Dict[str, Any]]: List of query results.
        """
        results = []
        operations = [lambda: results.extend(self.default_handler.select(criteria)) for criteria in criteria_list]
        self._execute_in_parallel(operations)
        return results

    def update_batch(self, criteria_data_pairs: List[tuple]) -> None:
        """
        Update records in batch.

        Args:
            criteria_data_pairs (List[tuple]): List of (criteria, data) pairs.
        """
        operations = [lambda: self.default_handler.update(criteria, data) for criteria, data in criteria_data_pairs]
        self._execute_in_parallel(operations)

    def delete_batch(self, criteria_list: List[Dict[str, Any]]) -> None:
        """
        Delete records in batch.

        Args:
            criteria_list (List[Dict[str, Any]]): List of query criteria.
        """
        operations = [lambda: self.default_handler.delete(criteria) for criteria in criteria_list]
        self._execute_in_parallel(operations)

    def _chunk_data(self, data_list: List[Any], size: int) -> Generator[List[Any], None, None]:
        """
        Split large datasets into manageable chunks.

        Args:
            data_list (List[Any]): List of data to chunk.
            size (int): Size of each chunk.

        Returns:
            Generator[List[Any], None, None]: Generator yielding chunks of data.
        """
        for i in range(0, len(data_list), size):
            yield data_list[i:i + size]

    def _execute_in_parallel(self, operations: List[callable]) -> None:
        """
        Execute operations in parallel.

        Args:
            operations (List[callable]): List of operations to execute.
        """
        with ThreadPoolExecutor() as executor:
            executor.map(lambda op: op(), operations)

    def _insert_on_shard(self, connection, data_list):
        """Execute batch insert operation on a specific shard."""
        for data in data_list:
            query, params = self._build_insert_query(data)
            self._execute_with_retry(query, params, connection)

    def _select_on_shard(self, connection, criteria_list):
        """Execute batch select operation on a specific shard."""
        results = []
        for criteria in criteria_list:
            query, params = self._build_select_query(criteria)
            cursor = connection.execute(query, params)
            results.extend(cursor.fetchall())
        return results

    def _update_on_shard(self, connection, criteria_data_pairs):
        """Execute batch update operation on a specific shard."""
        for criteria, data in criteria_data_pairs:
            query, params = self._build_update_query(criteria, data)
            self._execute_with_retry(query, params, connection)

    def _delete_on_shard(self, connection, criteria_list):
        """Execute batch delete operation on a specific shard."""
        for criteria in criteria_list:
            query, params = self._build_delete_query(criteria)
            self._execute_with_retry(query, params, connection)

    def _build_insert_query(self, data):
        """Delegate to default handler."""
        return self.default_handler._build_insert_query(data)

    def _build_select_query(self, criteria):
        """Delegate to default handler."""
        return self.default_handler._build_select_query(criteria)

    def _build_update_query(self, criteria, data):
        """Delegate to default handler."""
        return self.default_handler._build_update_query(criteria, data)

    def _build_delete_query(self, criteria):
        """Delegate to default handler."""
        return self.default_handler._build_delete_query(criteria)

    def _execute_with_retry(self, query, params, connection, retries=3):
        """Delegate to default handler."""
        return self.default_handler._execute_with_retry(query, params, connection, retries)
