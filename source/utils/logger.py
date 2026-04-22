import logging
import sys

logger = None


def get_logger(logger_name="dataprep", level="INFO"):

    global logger

    if logger is None:
        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
        )
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger
