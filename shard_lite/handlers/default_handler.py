from shard_lite.handlers.base_handler import BaseHandler
from shard_lite.exceptions.shard_exceptions import QueryExecutionError, ShardingError
import sqlite3

class DefaultHandler(BaseHandler):
    """
    Default implementation of CRUD operations for SQLite sharding.
    """

    def insert(self, data):
        """
        Insert a single record or small batch into the appropriate shard(s).

        Args:
            data (dict or list of dict): Data to insert.

        Raises:
            ShardingError: If data is invalid or insertion fails.
        """
        self._validate_data(data)
        if isinstance(data, dict):
            data = [data]  # Convert single record to a list for uniformity

        for record in data:
            shard_id = self.query_router.get_shard_for_key(record["id"])
            connection = self.connection_pool.get_connection(shard_id)
            try:
                self._insert_on_shard(connection, record)
                self.logger.info("Inserted record", shard_id=shard_id, record=record)
            finally:
                self.connection_pool.release_connection(connection, shard_id)

    def select(self, criteria):
        """
        Select records matching the criteria from the appropriate shard(s).

        Args:
            criteria (dict): Query criteria.

        Returns:
            list: List of records matching the criteria.

        Raises:
            ShardingError: If criteria is invalid or query fails.
        """
        self._validate_criteria(criteria)
        shards = self.query_router.get_shards_for_query(criteria)
        results = []

        for shard_id in shards:
            connection = self.connection_pool.get_connection(shard_id)
            try:
                results.extend(self._select_on_shard(connection, criteria))
                self.logger.info("Selected records", shard_id=shard_id, criteria=criteria)
            finally:
                self.connection_pool.release_connection(connection, shard_id)

        return results

    def update(self, criteria, data):
        """
        Update records matching the criteria in the appropriate shard(s).

        Args:
            criteria (dict): Query criteria.
            data (dict): Data to update.

        Raises:
            ShardingError: If criteria or data is invalid or update fails.
        """
        self._validate_criteria(criteria)
        self._validate_data(data)
        shards = self.query_router.get_shards_for_query(criteria)

        for shard_id in shards:
            connection = self.connection_pool.get_connection(shard_id)
            try:
                self._update_on_shard(connection, criteria, data)
                self.logger.info("Updated records", shard_id=shard_id, criteria=criteria, data=data)
            finally:
                self.connection_pool.release_connection(connection, shard_id)

    def delete(self, criteria):
        """
        Delete records matching the criteria from the appropriate shard(s).

        Args:
            criteria (dict): Query criteria.

        Raises:
            ShardingError: If criteria is invalid or deletion fails.
        """
        self._validate_criteria(criteria)
        shards = self.query_router.get_shards_for_query(criteria)

        for shard_id in shards:
            connection = self.connection_pool.get_connection(shard_id)
            try:
                self._delete_on_shard(connection, criteria)
                self.logger.info("Deleted records", shard_id=shard_id, criteria=criteria)
            finally:
                self.connection_pool.release_connection(connection, shard_id)

    def _build_select_query(self, criteria):
        """
        Build a SELECT query from criteria.

        Args:
            criteria (dict): Query criteria.

        Returns:
            tuple: Query string and parameters.
        """
        query = "SELECT * FROM records WHERE "
        conditions = []
        params = []
        for key, value in criteria.items():
            conditions.append(f"{key} = ?")
            params.append(value)
        query += " AND ".join(conditions)
        return query, params

    def _build_insert_query(self, data):
        """
        Build an INSERT query from data.

        Args:
            data (dict): Data to insert.

        Returns:
            tuple: Query string and parameters.
        """
        keys = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        query = f"INSERT INTO records ({keys}) VALUES ({placeholders})"
        params = list(data.values())
        return query, params

    def _build_update_query(self, criteria, data):
        """
        Build an UPDATE query.

        Args:
            criteria (dict): Query criteria.
            data (dict): Data to update.

        Returns:
            tuple: Query string and parameters.
        """
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        where_clause = " AND ".join([f"{key} = ?" for key in criteria.keys()])
        query = f"UPDATE records SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(criteria.values())
        return query, params

    def _build_delete_query(self, criteria):
        """
        Build a DELETE query.

        Args:
            criteria (dict): Query criteria.

        Returns:
            tuple: Query string and parameters.
        """
        where_clause = " AND ".join([f"{key} = ?" for key in criteria.keys()])
        query = f"DELETE FROM records WHERE {where_clause}"
        params = list(criteria.values())
        return query, params

    def _execute_with_retry(self, query, params, connection, retries=3):
        """
        Execute a query with retry logic.

        Args:
            query (str): SQL query.
            params (list): Query parameters.
            connection (sqlite3.Connection): SQLite connection.
            retries (int): Number of retry attempts.

        Raises:
            QueryExecutionError: If the query fails after retries.
        """
        for attempt in range(retries):
            try:
                connection.execute(query, params)
                connection.commit()
                return
            except sqlite3.Error as e:
                self.logger.error("Query execution failed", query=query, params=params, error=str(e))
                if attempt == retries - 1:
                    raise QueryExecutionError("Query execution failed after retries", context={"query": query, "params": params})

    def _insert_on_shard(self, connection, data):
        """Execute insert operation on a specific shard."""
        query, params = self._build_insert_query(data)
        self._execute_with_retry(query, params, connection)

    def _select_on_shard(self, connection, criteria):
        """Execute select operation on a specific shard."""
        query, params = self._build_select_query(criteria)
        cursor = connection.execute(query, params)
        return cursor.fetchall()

    def _update_on_shard(self, connection, criteria, data):
        """Execute update operation on a specific shard."""
        query, params = self._build_update_query(criteria, data)
        self._execute_with_retry(query, params, connection)

    def _delete_on_shard(self, connection, criteria):
        """Execute delete operation on a specific shard."""
        query, params = self._build_delete_query(criteria)
        self._execute_with_retry(query, params, connection)
