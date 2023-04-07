import logging


def get_logger(name: str, level: int = logging.INFO):
    _log = logging.getLogger(name)
    _log.setLevel(level)
    return _log


if __name__ == "__main__":
    logger = get_logger("test")
    logger.info("hehe")
