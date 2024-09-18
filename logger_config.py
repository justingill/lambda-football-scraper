import logging
import sys


def get_logger(name: str):
    logging.basicConfig()
    logger = logging.getLogger(name)
    level = logging.INFO
    logger.setLevel(level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    logger.addHandler(ch)
    return logger
