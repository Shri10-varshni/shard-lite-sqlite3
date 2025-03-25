import sqlite3
import threading
import os
from queue import Queue, Empty
from shard_lite.utils.config import Config
from shard_lite.utils.logger import Logger
from shard_lite.exceptions.shard_exceptions import ConnectionError, ConnectionTimeoutError

class ConnectionPool:
    """
    Thread-safe connection pool for managing SQLite database shard connections.

    Attributes:
        config (Config): Configuration instance for pool settings.
        logger (Logger): Logger instance for connection events.
        pool (dict): Dictionary of connection queues for each shard.
    """

    def __init__(self, config, logger=None):
        """
        Initialize the connection pool.

        Args:
            config (Config): Configuration instance for pool settings.
            logger (Logger, optional): Logger instance for logging events.
        """
        self.config = config
        self.logger = logger or Logger()
        self.pool = {}
        self.lock = threading.Lock()
        self.connection_timeout = self.config.get("connection_timeout", 30)
        self.pool_size = self.config.get("pool_size", 5)

    def get_connection(self, shard_id):
        """
        Get a connection to a specific shard.

        Args:
            shard_id (str): The shard ID.

        Returns:
            sqlite3.Connection: SQLite connection object.

        Raises:
            ConnectionTimeoutError: If no connection is available within the timeout.
        """
        with self.lock:
            if shard_id not in self.pool:
                self._initialize_shard_pool(shard_id)

        try:
            connection = self.pool[shard_id].get(timeout=self.connection_timeout)
            if not self._validate_connection(connection):
                self.logger.warning("Invalid connection detected, creating a new one", shard_id=shard_id)
                connection = self.create_shard_connection(shard_id)
            return connection
        except Empty:
            raise ConnectionTimeoutError("No available connections for shard", context={"shard_id": shard_id})

    def release_connection(self, connection, shard_id):
        """
        Return a connection to the pool.

        Args:
            connection (sqlite3.Connection): The connection to release.
            shard_id (str): The shard ID.
        """
        with self.lock:
            if shard_id in self.pool:
                self.pool[shard_id].put(connection)

    def create_shard_connection(self, shard_id, db_path=None):
        """
        Create a new connection to a shard.

        Args:
            shard_id (str): The shard ID.
            db_path (str, optional): Path to the shard database file.

        Returns:
            sqlite3.Connection: SQLite connection object.
        """
        db_path = db_path or self.config.get("shard_base_path", "./shards") + f"/{shard_id}.db"
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        connection = sqlite3.connect(db_path, check_same_thread=False)
        self.logger.info("Created new connection", shard_id=shard_id, db_path=db_path)
        return connection

    def close_shard_connections(self, shard_id):
        """
        Close all connections to a specific shard.

        Args:
            shard_id (str): The shard ID.
        """
        with self.lock:
            if shard_id in self.pool:
                while not self.pool[shard_id].empty():
                    connection = self.pool[shard_id].get()
                    connection.close()
                del self.pool[shard_id]
                self.logger.info("Closed all connections for shard", shard_id=shard_id)

    def close_all(self):
        """
        Close all connections in the pool.
        """
        with self.lock:
            for shard_id in list(self.pool.keys()):
                self.close_shard_connections(shard_id)

    def get_pool_status(self):
        """
        Return statistics about pool usage.

        Returns:
            dict: Dictionary with shard IDs and their connection counts.
        """
        with self.lock:
            return {shard_id: self.pool[shard_id].qsize() for shard_id in self.pool}

    def _initialize_shard_pool(self, shard_id):
        """
        Initialize the connection pool for a shard.

        Args:
            shard_id (str): The shard ID.
        """
        self.pool[shard_id] = Queue(maxsize=self.pool_size)
        for _ in range(self.pool_size):
            connection = self.create_shard_connection(shard_id)
            self.pool[shard_id].put(connection)
        self.logger.info("Initialized connection pool for shard", shard_id=shard_id)

    def _validate_connection(self, connection):
        """
        Validate a connection before returning it from the pool.

        Args:
            connection (sqlite3.Connection): The connection to validate.

        Returns:
            bool: True if the connection is valid, False otherwise.
        """
        try:
            connection.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False
