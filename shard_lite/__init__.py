"""
Shard-lite: A lightweight SQLite sharding library.

This library provides tools for distributing SQLite databases across multiple shards,
with support for different sharding strategies and CRUD operations.
"""

from typing import Optional, Dict, Any
from shard_lite.core.shard_manager import ShardManager
from shard_lite.utils.config import Config
from shard_lite.strategies import (
    BaseStrategy,
    HashStrategy,
    RangeStrategy,
    DirectoryStrategy
)

__version__ = '0.1.0'

def create_shard_manager(
    config: Optional[Config] = None,
    strategy: str = 'hash',
    **kwargs: Any
) -> ShardManager:
    """
    Create a new ShardManager instance with the specified configuration.

    Args:
        config (Config, optional): Configuration instance.
        strategy (str): Sharding strategy type ('hash', 'range', 'directory').
        **kwargs: Additional configuration parameters.

    Returns:
        ShardManager: Configured shard manager instance.
    """
    return ShardManager(config, strategy_type=strategy, **kwargs)

def get_strategy(
    strategy_type: str,
    config: Optional[Config] = None,
    **kwargs: Any
) -> BaseStrategy:
    """
    Create a sharding strategy instance.

    Args:
        strategy_type (str): Type of strategy ('hash', 'range', 'directory').
        config (Config, optional): Configuration instance.
        **kwargs: Strategy-specific parameters.

    Returns:
        BaseStrategy: Strategy instance.
    """
    strategies = {
        'hash': HashStrategy,
        'range': RangeStrategy,
        'directory': DirectoryStrategy
    }
    strategy_class = strategies.get(strategy_type)
    if not strategy_class:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
    return strategy_class(config or Config(**kwargs))
