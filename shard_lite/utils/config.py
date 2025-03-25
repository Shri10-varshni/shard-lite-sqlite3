import json
import yaml
from threading import RLock

class Config:
    """
    Thread-safe and immutable configuration manager for the SQLite sharding library.

    Attributes:
        connection_timeout (int): Timeout for database connections (default: 30).
        pool_size (int): Size of the connection pool (default: 5).
        mutable (bool): Whether the configuration is mutable after initialization (default: False).
    """

    _lock = RLock()

    def __init__(self, config_file=None, **kwargs):
        """
        Initialize the configuration manager.

        Args:
            config_file (str, optional): Path to a JSON or YAML configuration file.
            **kwargs: Direct configuration parameters.

        Raises:
            ValueError: If validation fails for any configuration setting.
        """
        self._config = {
            "connection_timeout": 30,
            "pool_size": 5,
            "mutable": False,
        }
        if config_file:
            self.load_config(config_file)
        self._config.update(kwargs)
        self.validate_config()
        self._immutable = not self._config.get("mutable", False)

    def load_config(self, config_file):
        """
        Load configuration from a JSON or YAML file.

        Args:
            config_file (str): Path to the configuration file.

        Raises:
            ValueError: If the file format is unsupported.
        """
        with open(config_file, "r") as file:
            if config_file.endswith(".json"):
                data = json.load(file)
            elif config_file.endswith((".yaml", ".yml")):
                data = yaml.safe_load(file)
            else:
                raise ValueError("Unsupported configuration file format. Use JSON or YAML.")
        self._config.update(data)

    def validate_config(self):
        """
        Validate the configuration settings.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        if not isinstance(self._config["connection_timeout"], int) or self._config["connection_timeout"] <= 0:
            raise ValueError("connection_timeout must be a positive integer.")
        if not isinstance(self._config["pool_size"], int) or self._config["pool_size"] <= 0:
            raise ValueError("pool_size must be a positive integer.")
        if not isinstance(self._config["mutable"], bool):
            raise ValueError("mutable must be a boolean.")

    def get(self, key, default=None):
        """
        Get a configuration value.

        Args:
            key (str): The configuration key.
            default: The default value if the key is not found.

        Returns:
            The configuration value or the default value.
        """
        with self._lock:
            return self._config.get(key, default)

    def set(self, key, value):
        """
        Update a configuration value if mutable.

        Args:
            key (str): The configuration key.
            value: The new value.

        Raises:
            RuntimeError: If the configuration is immutable.
        """
        if self._immutable:
            raise RuntimeError("Configuration is immutable and cannot be modified.")
        with self._lock:
            self._config[key] = value

    def to_dict(self):
        """
        Convert the configuration to a dictionary.

        Returns:
            dict: The configuration as a dictionary.
        """
        with self._lock:
            return dict(self._config)

    def save(self, config_file):
        """
        Save the configuration to a file.

        Args:
            config_file (str): Path to the output file.

        Raises:
            ValueError: If the file format is unsupported.
        """
        with open(config_file, "w") as file:
            if config_file.endswith(".json"):
                json.dump(self._config, file, indent=4)
            elif config_file.endswith((".yaml", ".yml")):
                yaml.dump(self._config, file)
            else:
                raise ValueError("Unsupported configuration file format. Use JSON or YAML.")
