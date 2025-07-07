#!/usr/bin/env python3
"""
Logging utility functions for weather data project.

Provides setup_logging for consistent logging across modules.
"""
import logging
import os


def setup_logging(log_path: str = "logs/weather.log", logger_name: str = None):
    """
    Configure logging with detailed format.

    Args:
        log_path: Path to log file (default: logs/weather.log)
        logger_name: Name for the logger (default: None, uses __name__)

    Returns:
        Configured logger instance
    """
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(logger_name or __name__)
