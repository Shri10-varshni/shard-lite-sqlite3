import logging
import logging.handlers
from threading import RLock
from shard_lite.utils.config import Config
import re
from functools import wraps
import time

class Logger:
    """
    Standardized logging utility for the SQLite sharding library.

    Attributes:
        config (Config): Configuration instance for log settings.
    """

    _lock = RLock()
    _sensitive_data_pattern = re.compile(r"(password|connection_string)", re.IGNORECASE)

    def __init__(self, config=None):
        """
        Initialize the Logger.

        Args:
            config (Config, optional): Configuration instance for log settings.
        """
        self.config = config or Config()
        self._loggers = {}
        self._file_handler = None
        self._console_handler = None
        self._initialize_console_logging()

    def _initialize_console_logging(self):
        """Set up console logging with default settings."""
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [Thread %(thread)d] - %(message)s"
        ))
        self._console_handler = console_handler

    def get_logger(self, component_name):
        """
        Get a logger for a specific component.

        Args:
            component_name (str): Name of the component.

        Returns:
            logging.Logger: Configured logger instance.
        """
        with self._lock:
            if component_name not in self._loggers:
                logger = logging.getLogger(component_name)
                logger.setLevel(self.config.get("log_level", logging.INFO))
                logger.addHandler(self._console_handler)
                if self._file_handler:
                    logger.addHandler(self._file_handler)
                self._loggers[component_name] = logger
            return self._loggers[component_name]

    def set_level(self, level):
        """
        Change the log level dynamically.

        Args:
            level (int): New log level (e.g., logging.DEBUG).
        """
        with self._lock:
            for logger in self._loggers.values():
                logger.setLevel(level)
            self._console_handler.setLevel(level)  # Ensure the console handler respects the new level
            if self._file_handler:
                self._file_handler.setLevel(level)

    def enable_file_logging(self, path):
        """
        Enable logging to a file with rotation.

        Args:
            path (str): Path to the log file.
        """
        with self._lock:
            file_handler = logging.handlers.RotatingFileHandler(
                path, maxBytes=10 * 1024 * 1024, backupCount=5
            )
            file_handler.setFormatter(logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - [Thread %(thread)d] - %(message)s"
            ))
            self._file_handler = file_handler
            for logger in self._loggers.values():
                logger.addHandler(file_handler)

    def disable_file_logging(self):
        """Disable logging to a file."""
        with self._lock:
            if self._file_handler:
                for logger in self._loggers.values():
                    logger.removeHandler(self._file_handler)
                self._file_handler = None

    def log(self, level, message, **context):
        """
        Log a message with the specified level and context.

        Args:
            level (int): Log level (e.g., logging.INFO).
            message (str): Log message.
            **context: Additional context information.
        """
        sanitized_message = self._sanitize_message(message)
        logger = self.get_logger(context.get("component", "default"))
        logger.log(level, sanitized_message)

    def debug(self, message, **context):
        """Log a debug message."""
        self.log(logging.DEBUG, message, **context)

    def info(self, message, **context):
        """Log an info message."""
        self.log(logging.INFO, message, **context)

    def warning(self, message, **context):
        """Log a warning message."""
        self.log(logging.WARNING, message, **context)

    def error(self, message, **context):
        """Log an error message."""
        self.log(logging.ERROR, message, **context)

    def critical(self, message, **context):
        """Log a critical message."""
        self.log(logging.CRITICAL, message, **context)

    def _sanitize_message(self, message):
        """
        Sanitize sensitive data from the log message.

        Args:
            message (str): Original log message.

        Returns:
            str: Sanitized log message.
        """
        return re.sub(r"(password|connection_string)=[^ ]+", r"\1=[REDACTED]", message)

    @staticmethod
    def timing_decorator(func):
        """
        Decorator to log the execution time of a function.

        Args:
            func (callable): Function to be decorated.

        Returns:
            callable: Wrapped function.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            logging.getLogger("performance").info(
                f"Function {func.__name__} executed in {end_time - start_time:.2f} seconds."
            )
            return result
        return wrapper
