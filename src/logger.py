"""
Logging configuration utilities.
"""

import logging
from pathlib import Path
from typing import Optional

class LoggerFactory:
    """Factory for creating configured loggers."""
    
    @staticmethod
    def create_logger(name: str, log_level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
        """
        Create a configured logger.
        
        Args:
            name: Logger name
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional log file path
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Create logs directory if it doesn't exist
        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler (if specified)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
