from abc import ABC, abstractmethod
from shard_lite.utils.config import Config
from shard_lite.utils.logger import Logger
from shard_lite.exceptions.shard_exceptions import StrategyError

class BaseStrategy(ABC):
    """
    Abstract base class for all sharding strategies.

    Defines the interface and common utilities for sharding strategies.
    """

    def __init__(self, config, logger=None):
        """
        Initialize the sharding strategy.

        Args:
            config (Config): Configuration instance for the strategy.
            logger (Logger, optional): Logger instance for logging operations.
        """
        if not isinstance(config, Config):
            raise StrategyError("Invalid configuration provided", context={"config": config})
        self.config = config
        self.logger = logger or Logger()

    @abstractmethod
    def get_shard_for_key(self, key):
        """
        Determine which shard contains the given key.

        Args:
            key: The key to locate.

        Returns:
            str: The shard ID containing the key.
        """
        pass

    @abstractmethod
    def get_shards_for_query(self, criteria):
        """
        Find all shards needed for a query.

        Args:
            criteria: Query criteria to determine relevant shards.

        Returns:
            list: List of shard IDs.
        """
        pass

    @abstractmethod
    def create_shard(self, shard_id):
        """
        Initialize a new shard.

        Args:
            shard_id (str): Unique identifier for the new shard.
        """
        pass

    @abstractmethod
    def add_shard(self, shard_id):
        """
        Add an existing shard to the rotation.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        pass

    @abstractmethod
    def remove_shard(self, shard_id):
        """
        Remove a shard from the rotation.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        pass

    def validate_key(self, key):
        """
        Validate that the key is valid for sharding.

        Args:
            key: The key to validate.

        Raises:
            StrategyError: If the key is invalid.
        """
        if not key:
            raise StrategyError("Sharding key cannot be null or empty", context={"key": key})
        if not isinstance(key, (int, str)):
            raise StrategyError("Sharding key must be an integer or string", context={"key": key})

    def get_all_shards(self):
        """
        Return a list of all active shards.

        Returns:
            list: List of active shard IDs.
        """
        return self.config.get("active_shards", [])

    def get_shard_file_path(self, shard_id):
        """
        Generate the file path for a shard.

        Args:
            shard_id (str): Unique identifier for the shard.

        Returns:
            str: File path for the shard.
        """
        base_path = self.config.get("shard_base_path", "./shards")
        return f"{base_path}/{shard_id}.db"

    def _log_strategy_operation(self, operation, **context):
        """
        Log a strategy operation.

        Args:
            operation (str): Description of the operation.
            **context: Additional context information.
        """
        self.logger.info(f"Strategy operation: {operation}", **context)
