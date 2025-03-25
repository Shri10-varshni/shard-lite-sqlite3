import hashlib
from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.exceptions.shard_exceptions import StrategyError

class HashStrategy(BaseStrategy):
    """
    Hash-based sharding strategy for distributing data across shards.

    Attributes:
        hash_function (callable): Hash function to use (default: MD5).
        hash_ring (dict): Mapping of hash values to shard IDs.
    """

    def __init__(self, config, hash_function=None):
        """
        Initialize the HashStrategy.

        Args:
            config (Config): Configuration instance for the strategy.
            hash_function (callable, optional): Custom hash function (default: MD5).
        """
        super().__init__(config)
        self.hash_function = hash_function or (lambda key: hashlib.md5(str(key).encode()).hexdigest())
        self.hash_ring = {}
        self._initialize_hash_ring()

    def _initialize_hash_ring(self):
        """Initialize the hash ring with active shards."""
        for shard_id in self.get_all_shards():
            self.add_shard(shard_id)

    def _hash_key(self, key):
        """
        Apply the hash function to a key.

        Args:
            key: The key to hash.

        Returns:
            int: Hash value as an integer.

        Raises:
            StrategyError: If the key is invalid or unhashable.
        """
        try:
            self.validate_key(key)
            hash_value = int(self.hash_function(key), 16)
            return hash_value
        except Exception as e:
            raise StrategyError("Failed to hash key", context={"key": key, "error": str(e)})

    def _get_shard_id_from_hash(self, hash_value):
        """
        Map a hash value to a shard ID.

        Args:
            hash_value (int): Hash value.

        Returns:
            str: Shard ID.
        """
        sorted_shards = sorted(self.hash_ring.keys())
        for shard_hash in sorted_shards:
            if hash_value <= shard_hash:
                return self.hash_ring[shard_hash]
        return self.hash_ring[sorted_shards[0]]  # Wrap around to the first shard

    def get_shard_for_key(self, key):
        """
        Determine which shard contains the given key.

        Args:
            key: The key to locate.

        Returns:
            str: The shard ID containing the key.
        """
        hash_value = self._hash_key(key)
        return self._get_shard_id_from_hash(hash_value)

    def get_shards_for_query(self, criteria):
        """
        Find all shards needed for a query.

        Args:
            criteria: Query criteria to determine relevant shards.

        Returns:
            list: List of shard IDs.
        """
        # For simplicity, assume all shards are needed for complex queries.
        return self.get_all_shards()

    def create_shard(self, shard_id):
        """
        Initialize a new shard.

        Args:
            shard_id (str): Unique identifier for the new shard.
        """
        # Placeholder for actual shard creation logic.
        self.add_shard(shard_id)

    def add_shard(self, shard_id):
        """
        Add an existing shard to the hash ring.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        shard_hash = self._hash_key(shard_id)
        self.hash_ring[shard_hash] = shard_id
        self._log_strategy_operation("Added shard to hash ring", shard_id=shard_id)

    def remove_shard(self, shard_id):
        """
        Remove a shard from the hash ring.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        shard_hash = self._hash_key(shard_id)
        if shard_hash in self.hash_ring:
            del self.hash_ring[shard_hash]
            self._log_strategy_operation("Removed shard from hash ring", shard_id=shard_id)

    def rebalance_shards(self):
        """
        Redistribute data for even distribution across shards.
        """
        old_ring = dict(self.hash_ring)
        self.hash_ring.clear()
        
        # Calculate target distribution
        total_shards = len(self.get_all_shards())
        target_per_shard = (1 << 160) // total_shards  # Using 160-bit hash space
        
        # Redistribute hash ranges
        for shard_id in self.get_all_shards():
            for i in range(self.config.get("virtual_nodes", 100)):
                hash_key = f"{shard_id}:{i}"
                hash_value = self._hash_key(hash_key)
                self.hash_ring[hash_value] = shard_id
        
        # Log changes in distribution
        changes = {
            shard: len([k for k, v in self.hash_ring.items() if v == shard])
            for shard in self.get_all_shards()
        }
        self._log_strategy_operation("Rebalanced shards", distribution=changes)
