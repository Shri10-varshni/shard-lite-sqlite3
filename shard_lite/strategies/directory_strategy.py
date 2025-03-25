from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.exceptions.shard_exceptions import StrategyError
from collections import OrderedDict
import json
import os

class DirectoryStrategy(BaseStrategy):
    """
    Directory-based sharding strategy for explicitly mapping keys to shards.

    Attributes:
        directory (dict): Key-to-shard mapping.
        cache (OrderedDict): LRU cache for frequently accessed mappings.
    """

    def __init__(self, config, directory=None, cache_size=100):
        """
        Initialize the DirectoryStrategy.

        Args:
            config (Config): Configuration instance for the strategy.
            directory (dict, optional): Initial key-to-shard mapping.
            cache_size (int): Maximum size of the LRU cache.
        """
        super().__init__(config)
        self.directory = directory or {}
        self.cache = OrderedDict()
        self.cache_size = cache_size
        self._load_directory()

    def get_shard_for_key(self, key):
        """
        Look up the shard for a key in the directory.

        Args:
            key: The key to locate.

        Returns:
            str: The shard ID containing the key.

        Raises:
            StrategyError: If the key is not found in the directory.
        """
        self.validate_key(key)
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        if key in self.directory:
            shard_id = self.directory[key]
            self._update_cache(key, shard_id)
            return shard_id
        raise StrategyError("Key not found in directory", context={"key": key})

    def get_shards_for_query(self, criteria):
        """
        Find all shards for a query.

        Args:
            criteria: Query criteria to determine relevant shards.

        Returns:
            list: List of shard IDs.
        """
        # For simplicity, assume criteria is a list of keys.
        shards = set()
        for key in criteria:
            try:
                shards.add(self.get_shard_for_key(key))
            except StrategyError:
                continue
        return list(shards)

    def create_shard(self, shard_id):
        """
        Create a new shard.

        Args:
            shard_id (str): Unique identifier for the new shard.
        """
        # Placeholder for actual shard creation logic.
        self.add_shard(shard_id)

    def add_shard(self, shard_id):
        """
        Add an existing shard to the directory.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        if shard_id in self.directory.values():
            raise StrategyError("Shard already exists in directory", context={"shard_id": shard_id})
        self._log_strategy_operation("Added shard to directory", shard_id=shard_id)

    def remove_shard(self, shard_id):
        """
        Remove a shard from the directory.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        keys_to_remove = [key for key, shard in self.directory.items() if shard == shard_id]
        for key in keys_to_remove:
            self.remove_mapping(key)
        self._log_strategy_operation("Removed shard from directory", shard_id=shard_id)

    def add_mapping(self, key, shard_id):
        """
        Add or update a key-to-shard mapping.

        Args:
            key: The key to map.
            shard_id (str): The shard ID to map the key to.
        """
        self.validate_key(key)
        self.directory[key] = shard_id
        self._update_cache(key, shard_id)
        self._save_directory()

    def remove_mapping(self, key):
        """
        Remove a key-to-shard mapping.

        Args:
            key: The key to remove.
        """
        if key in self.directory:
            del self.directory[key]
            self.cache.pop(key, None)
            self._save_directory()

    def get_mappings_for_shard(self, shard_id):
        """
        Get all keys mapped to a specific shard.

        Args:
            shard_id (str): The shard ID.

        Returns:
            list: List of keys mapped to the shard.
        """
        return [key for key, shard in self.directory.items() if shard == shard_id]

    def _update_cache(self, key, shard_id):
        """
        Update the LRU cache with a key-to-shard mapping.

        Args:
            key: The key to cache.
            shard_id (str): The shard ID to cache.
        """
        self.cache[key] = shard_id
        self.cache.move_to_end(key)
        if len(self.cache) > self.cache_size:
            self.cache.popitem(last=False)

    def _load_directory(self):
        """
        Load the directory from persistent storage.
        """
        directory_path = self.config.get("directory_path", "./directory.json")
        if os.path.exists(directory_path):
            with open(directory_path, "r") as file:
                self.directory = json.load(file)

    def _save_directory(self):
        """
        Save the directory to persistent storage.
        """
        directory_path = self.config.get("directory_path", "./directory.json")
        with open(directory_path, "w") as file:
            json.dump(self.directory, file)

    def import_mappings(self, mappings_dict):
        """
        Bulk import mappings into the directory.

        Args:
            mappings_dict (dict): Dictionary of key-to-shard mappings.
        """
        self.directory.update(mappings_dict)
        self.cache.clear()
        self._save_directory()
