from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.utils.logger import Logger
from shard_lite.exceptions.shard_exceptions import ShardingError
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional

class QueryRouter:
    """
    Routes queries to appropriate shards based on the sharding strategy.
    """

    def __init__(self, connection_pool: ConnectionPool, strategy: BaseStrategy, logger: Optional[Logger] = None):
        """
        Initialize the QueryRouter.

        Args:
            connection_pool (ConnectionPool): Connection pool for managing connections.
            strategy (BaseStrategy): Sharding strategy for determining target shards.
            logger (Logger, optional): Logger instance for logging operations.
        """
        self.connection_pool = connection_pool
        self.strategy = strategy
        self.logger = logger or Logger()

    def execute_query(self, query: str, params: List[Any], shard_ids: Optional[List[str]] = None) -> List[Any]:
        """
        Execute a query on specific shards.

        Args:
            query (str): SQL query to execute.
            params (List[Any]): Query parameters.
            shard_ids (List[str], optional): List of shard IDs to execute the query on.

        Returns:
            List[Any]: Aggregated results from all shards.
        """
        shard_ids = shard_ids or self.strategy.get_all_shards()
        results = []

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._execute_on_shard, query, params, shard_id): shard_id for shard_id in shard_ids
            }
            for future in futures:
                try:
                    results.extend(future.result())
                except Exception as e:
                    self.logger.error("Query execution failed on shard", shard_id=futures[future], error=str(e))
                    raise ShardingError("Query execution failed", context={"shard_id": futures[future], "error": str(e)})

        return self._aggregate_results(results)

    def execute_read(self, query: str, params: List[Any], criteria: Dict[str, Any]) -> List[Any]:
        """
        Execute a read operation based on criteria.

        Args:
            query (str): SQL query to execute.
            params (List[Any]): Query parameters.
            criteria (Dict[str, Any]): Query criteria to determine target shards.

        Returns:
            List[Any]: Aggregated results from all shards.
        """
        shard_ids = self._determine_target_shards(criteria)
        return self.execute_query(query, params, shard_ids)

    def execute_write(self, query: str, params: List[Any], data: Dict[str, Any]) -> None:
        """
        Execute a write operation based on data.

        Args:
            query (str): SQL query to execute.
            params (List[Any]): Query parameters.
            data (Dict[str, Any]): Data to determine target shard.
        """
        shard_id = self.strategy.get_shard_for_key(data["id"])
        self._execute_on_shard(query, params, shard_id)

    def _determine_target_shards(self, criteria: Dict[str, Any]) -> List[str]:
        """
        Identify target shards using the sharding strategy.

        Args:
            criteria (Dict[str, Any]): Query criteria.

        Returns:
            List[str]: List of target shard IDs.
        """
        return self.strategy.get_shards_for_query(criteria)

    def _aggregate_results(self, results: List[Any]) -> List[Any]:
        """
        Combine results from multiple shards.

        Args:
            results (List[Any]): Results from individual shards.

        Returns:
            List[Any]: Aggregated results.
        """
        # Handle different types of aggregations
        if not results:
            return []
            
        if isinstance(results[0], (list, tuple)):
            # Combine result sets
            aggregated = []
            seen = set()
            
            for result in results:
                for record in result:
                    record_key = tuple(record) if isinstance(record, list) else record
                    if record_key not in seen:
                        seen.add(record_key)
                        aggregated.append(record)
                        
            return sorted(aggregated, key=lambda x: x[0] if isinstance(x, (list, tuple)) else x)
        
        return results

    def _execute_on_shard(self, query: str, params: List[Any], shard_id: str) -> List[Any]:
        """
        Execute a query on a specific shard.

        Args:
            query (str): SQL query to execute.
            params (List[Any]): Query parameters.
            shard_id (str): Shard ID.

        Returns:
            List[Any]: Query results.
        """
        connection = self.connection_pool.get_connection(shard_id)
        try:
            self.logger.info("Executing query on shard", shard_id=shard_id, query=query)
            cursor = connection.execute(query, params)
            return cursor.fetchall()
        finally:
            self.connection_pool.release_connection(connection, shard_id)

    def _optimize_query(self, query: str, shard_id: str) -> str:
        """
        Optimize a query for a specific shard.

        Args:
            query (str): Original query.
            shard_id (str): Shard ID.

        Returns:
            str: Optimized query.
        """
        # Add shard-specific optimizations
        optimizations = {
            # Add index hints
            "SELECT": lambda q: q.replace("SELECT", "SELECT /*+ INDEX(records) */"),
            # Use specific shard tables
            "FROM": lambda q: q.replace("FROM records", f"FROM {shard_id}_records"),
            # Add query hints for large operations
            "WHERE": lambda q: "/*+ PARALLEL(4) */ " + q if "IN" in q else q
        }
        
        for keyword, optimizer in optimizations.items():
            if keyword in query.upper():
                query = optimizer(query)
        
        self.logger.debug("Optimized query", original=query, optimized=query)
        return query
