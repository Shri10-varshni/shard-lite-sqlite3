from typing import Any, Dict, List, Optional, Type, Union
from shard_lite.utils.config import Config
from shard_lite.utils.logger import Logger
from shard_lite.core.connection_pool import ConnectionPool
from shard_lite.core.query_router import QueryRouter
from shard_lite.core.metadata_manager import MetadataManager
from shard_lite.core.transaction_manager import TransactionManager
from shard_lite.strategies.hash_strategy import HashStrategy
from shard_lite.strategies.range_strategy import RangeStrategy
from shard_lite.strategies.directory_strategy import DirectoryStrategy
from shard_lite.handlers.default_handler import DefaultHandler
from shard_lite.handlers.batch_handler import BatchHandler
from shard_lite.exceptions.shard_exceptions import ShardingError

class ShardManager:
    """
    Central orchestration component and primary API for the sharding library.
    """

    STRATEGY_TYPES = {
        'hash': HashStrategy,
        'range': RangeStrategy,
        'directory': DirectoryStrategy
    }

    HANDLER_TYPES = {
        'default': DefaultHandler,
        'batch': BatchHandler
    }

    def __init__(self, config: Optional[Config] = None, strategy_type: str = 'hash', **kwargs):
        """
        Initialize the ShardManager.

        Args:
            config (Config, optional): Configuration instance.
            strategy_type (str): Type of sharding strategy ('hash' or 'range').
            **kwargs: Additional configuration parameters.
        """
        self.config = config or Config(**kwargs)
        self.logger = Logger()

        # Initialize components
        self.connection_pool = ConnectionPool(self.config, self.logger)
        self.strategy = self._create_strategy(strategy_type)
        self.query_router = QueryRouter(self.connection_pool, self.strategy, self.logger)
        self.metadata_manager = MetadataManager(self.config, self.logger)
        self.transaction_manager = TransactionManager(self.connection_pool, self.logger)
        
        # Initialize handlers
        self._handlers = {}
        self._default_handler = self.get_handler('default')

    def create_shard(self, shard_id: Optional[str] = None) -> str:
        """Create a new shard."""
        shard_id = shard_id or f"shard_{len(self.list_shards()) + 1}"
        self.strategy.create_shard(shard_id)
        self.metadata_manager.register_shard(shard_id, self.strategy.get_shard_file_path(shard_id))
        return shard_id

    def get_shard(self, shard_id: str) -> Dict[str, Any]:
        """Get information about a specific shard."""
        return self.metadata_manager.get_shard_info(shard_id)

    def list_shards(self) -> List[str]:
        """List all available shards."""
        return self.metadata_manager.list_shards()

    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs) -> None:
        """Insert data using the appropriate handler."""
        handler = self.get_handler(kwargs.get('handler_type', 'default'))
        handler.insert(data)

    def select(self, criteria: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """Query data using the appropriate handler."""
        handler = self.get_handler(kwargs.get('handler_type', 'default'))
        return handler.select(criteria)

    def update(self, criteria: Dict[str, Any], data: Dict[str, Any], **kwargs) -> None:
        """Update data using the appropriate handler."""
        handler = self.get_handler(kwargs.get('handler_type', 'default'))
        handler.update(criteria, data)

    def delete(self, criteria: Dict[str, Any], **kwargs) -> None:
        """Delete data using the appropriate handler."""
        handler = self.get_handler(kwargs.get('handler_type', 'default'))
        handler.delete(criteria)

    def execute_transaction(self, operations: List[Dict[str, Any]]) -> None:
        """Execute operations as a transaction."""
        transaction_id = self.transaction_manager.begin_transaction()
        try:
            for operation in operations:
                self.transaction_manager.add_operation(transaction_id, operation)
                getattr(self, operation['type'])(**operation['params'])
            self.transaction_manager.commit_transaction(transaction_id)
        except Exception as e:
            self.transaction_manager.rollback_transaction(transaction_id)
            raise ShardingError("Transaction failed", context={"error": str(e)})

    def get_handler(self, handler_type: str = 'default'):
        """Get a specific CRUD handler."""
        if handler_type not in self._handlers:
            handler_class = self.HANDLER_TYPES.get(handler_type)
            if not handler_class:
                raise ShardingError(f"Unknown handler type: {handler_type}")
            self._handlers[handler_type] = handler_class(
                self.query_router,
                self.connection_pool,
                self.logger
            )
        return self._handlers[handler_type]

    def close(self) -> None:
        """Clean up resources."""
        self.connection_pool.close_all()
        self.logger.info("ShardManager closed")

    def _create_strategy(self, strategy_type: str):
        """Create a sharding strategy instance."""
        strategy_class = self.STRATEGY_TYPES.get(strategy_type)
        if not strategy_class:
            raise ShardingError(f"Unknown strategy type: {strategy_type}")
        
        # Initialize strategy with appropriate configuration
        strategy_config = {
            'hash': lambda: self.config,
            'range': lambda: self.config.update({"ranges": {}}),
            'directory': lambda: self.config.update({"directory_path": "./shards/directory.json"})
        }[strategy_type]()
        
        return strategy_class(strategy_config)
