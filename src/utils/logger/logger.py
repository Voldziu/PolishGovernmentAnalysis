import logging
import sys
from pathlib import Path


def get_logger(name: str, level: int = logging.INFO):
    """
    Creates and returns a logger instance with a default handler and formatter.
    Expected to be called with __file__ as the name, which will be shortened to just the filename.
    """
    # Shorten the name if it's a file path
    name = Path(name).name

    logger = logging.getLogger(name)
    logger.propagate = True

    if not logger.handlers:
        logger.setLevel(level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        logger.propagate = False

    return logger
