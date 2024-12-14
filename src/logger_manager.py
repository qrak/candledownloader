import logging
import os
from datetime import datetime
from typing import Optional


class LoggerManager:
    DEFAULT_FORMAT = '%(asctime)s %(message)s'
    DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LOG_DIR = 'logs'

    @staticmethod
    def setup_logger(
            name: str,
            log_to_file: bool = False,
            filename: Optional[str] = None,
            log_format: Optional[str] = None,
            date_format: Optional[str] = None,
            log_level: int = logging.INFO
    ) -> logging.Logger:
        logger = logging.getLogger(name)

        if logger.hasHandlers():
            return logger

        logger.setLevel(log_level)

        formatter = logging.Formatter(
            log_format or LoggerManager.DEFAULT_FORMAT,
            datefmt=date_format or LoggerManager.DEFAULT_DATE_FORMAT
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if log_to_file:
            os.makedirs(LoggerManager.LOG_DIR, exist_ok=True)

            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = name.replace(".", "_")
                filename = os.path.join(
                    LoggerManager.LOG_DIR,
                    f"{safe_name}_{timestamp}.log"
                )
            else:
                filename = os.path.join(LoggerManager.LOG_DIR, filename)

            file_handler = logging.FileHandler(filename)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger
