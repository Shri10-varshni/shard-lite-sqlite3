from shard_lite.strategies.base_strategy import BaseStrategy
from shard_lite.exceptions.shard_exceptions import StrategyError

class RangeStrategy(BaseStrategy):
    """
    Range-based sharding strategy for distributing data across shards.

    Attributes:
        ranges (dict): Mapping of ranges to shard IDs.
    """

    def __init__(self, config, ranges=None):
        """
        Initialize the RangeStrategy.

        Args:
            config (Config): Configuration instance for the strategy.
            ranges (dict, optional): Predefined ranges for shards.
        """
        super().__init__(config)
        self.ranges = ranges or {}
        self._validate_ranges()

    def _validate_ranges(self):
        """
        Ensure that ranges do not overlap.

        Raises:
            StrategyError: If overlapping ranges are detected.
        """
        sorted_ranges = sorted(self.ranges.keys())
        for i in range(len(sorted_ranges) - 1):
            if sorted_ranges[i][1] > sorted_ranges[i + 1][0]:
                raise StrategyError("Overlapping ranges detected", context={"ranges": self.ranges})

    def _get_range_for_value(self, value):
        """
        Find the range containing a value.

        Args:
            value: The value to locate.

        Returns:
            tuple: The range containing the value.

        Raises:
            StrategyError: If no range contains the value.
        """
        for range_start, range_end in self.ranges:
            if range_start <= value < range_end:
                return (range_start, range_end)
        raise StrategyError("Value does not fall within any range", context={"value": value})

    def get_shard_for_key(self, key):
        """
        Find the shard containing the key's range.

        Args:
            key: The key to locate.

        Returns:
            str: The shard ID containing the key.
        """
        self.validate_key(key)
        range_ = self._get_range_for_value(key)
        return self.ranges[range_]

    def get_shards_for_query(self, criteria):
        """
        Determine shards for range queries.

        Args:
            criteria: Query criteria to determine relevant shards.

        Returns:
            list: List of shard IDs.
        """
        # For simplicity, assume criteria is a range (start, end).
        start, end = criteria
        shards = set()
        for range_start, range_end in self.ranges:
            if not (end <= range_start or start >= range_end):  # Overlapping ranges
                shards.add(self.ranges[(range_start, range_end)])
        return list(shards)

    def create_shard(self, shard_id):
        """
        Initialize a new shard.

        Args:
            shard_id (str): Unique identifier for the new shard.
        """
        # Placeholder for actual shard creation logic.
        self.add_shard(shard_id)

    def add_shard(self, shard_id, range_start=None, range_end=None):
        """
        Add a shard with a specified range.

        Args:
            shard_id (str): Unique identifier for the shard.
            range_start: Start of the range.
            range_end: End of the range.
        """
        if (range_start, range_end) in self.ranges:
            raise StrategyError("Range already exists", context={"range": (range_start, range_end)})
        self.ranges[(range_start, range_end)] = shard_id
        self._validate_ranges()
        self._log_strategy_operation("Added shard with range", shard_id=shard_id, range=(range_start, range_end))

    def remove_shard(self, shard_id):
        """
        Remove a shard and reassign its range.

        Args:
            shard_id (str): Unique identifier for the shard.
        """
        ranges_to_remove = [r for r, s in self.ranges.items() if s == shard_id]
        for range_ in ranges_to_remove:
            del self.ranges[range_]
        self._log_strategy_operation("Removed shard", shard_id=shard_id)

    def add_range(self, start, end, shard_id):
        """
        Define a new range for a shard.

        Args:
            start: Start of the range.
            end: End of the range.
            shard_id (str): Shard ID to assign the range.
        """
        self.add_shard(shard_id, start, end)

    def split_range(self, range_start, split_point, new_shard_id):
        """
        Split a range into two shards.

        Args:
            range_start: Start of the range to split.
            split_point: Point at which to split the range.
            new_shard_id (str): Shard ID for the new range.
        """
        range_ = self._get_range_for_value(range_start)
        if split_point <= range_[0] or split_point >= range_[1]:
            raise StrategyError("Split point is outside the range", context={"split_point": split_point})
        old_shard_id = self.ranges.pop(range_)
        self.add_shard(old_shard_id, range_[0], split_point)
        self.add_shard(new_shard_id, split_point, range_[1])

    def merge_ranges(self, range1, range2, target_shard_id):
        """
        Combine adjacent ranges into one.

        Args:
            range1: First range to merge.
            range2: Second range to merge.
            target_shard_id (str): Shard ID for the merged range.
        """
        if range1[1] != range2[0]:
            raise StrategyError("Ranges are not adjacent", context={"range1": range1, "range2": range2})
        self.remove_shard(self.ranges[range1])
        self.remove_shard(self.ranges[range2])
        self.add_shard(target_shard_id, range1[0], range2[1])
