import pytest
import logging
import tempfile
import os
from shard_lite.utils.logger import Logger
from shard_lite.utils.config import Config

@pytest.fixture
def temp_log_file():
    with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
        yield f.name
    os.unlink(f.name)

def test_console_logging(caplog):
    # Test logging to the console
    logger = Logger()
    test_message = "Test console logging"
    with caplog.at_level(logging.INFO):
        logger.info(test_message)
        assert test_message in caplog.text

def test_file_logging(temp_log_file):
    # Test enabling and disabling file logging
    logger = Logger()
    logger.enable_file_logging(temp_log_file)
    
    test_message = "Test file logging"
    logger.info(test_message)
    
    with open(temp_log_file, 'r') as f:
        log_content = f.read()
        assert test_message in log_content
    
    # Test disabling file logging
    logger.disable_file_logging()
    logger.info("This should not be in file")
    
    with open(temp_log_file, 'r') as f:
        log_content = f.read()
        assert "This should not be in file" not in log_content

def test_log_levels(caplog):
    # Test changing log levels dynamically
    logger = Logger()
    logger.set_level(logging.DEBUG)
    component_logger = logger.get_logger("test_component")
    assert component_logger.getEffectiveLevel() == logging.DEBUG

    logger.set_level(logging.ERROR)
    assert component_logger.getEffectiveLevel() == logging.ERROR

def test_sanitization():
    # Test sanitization of sensitive data in log messages
    logger = Logger()
    test_message = "password=secret connection_string=db://user:pass@host"
    sanitized = logger._sanitize_message(test_message)
    assert "secret" not in sanitized
    assert "[REDACTED]" in sanitized
    assert "db://user:pass@host" not in sanitized

@pytest.mark.parametrize("execution_time", [0.1, 0.5])
def test_timing_decorator(caplog, execution_time):
    # Test the timing decorator for performance monitoring
    logger = Logger()

    @logger.timing_decorator
    def slow_function():
        import time
        time.sleep(execution_time)
        return True

    with caplog.at_level(logging.INFO):
        result = slow_function()
        assert result is True
        assert "executed in" in caplog.text
        assert "seconds" in caplog.text

def test_component_logging(caplog):
    # Test logging with different components
    logger = Logger()
    with caplog.at_level(logging.INFO):
        logger.info("Test message", component="component1")
        logger.error("Error message", component="component2")
    
    component1_logger = logger.get_logger("component1")
    component2_logger = logger.get_logger("component2")
    
    assert "component1" in logger._loggers
    assert "component2" in logger._loggers
    assert component1_logger != component2_logger
