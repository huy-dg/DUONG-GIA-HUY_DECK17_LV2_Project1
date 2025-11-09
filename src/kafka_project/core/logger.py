import logging
import logging.config
import os
from kafka_project.core.config import settings

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "debug.log")

LOG_LEVEL = settings.log_level

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] [%(threadName)s] "
            "%(name)s: %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": "INFO",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "filename": str(LOG_FILE),
            "maxBytes": 10 * 1024 * 1024,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8",
            "level": LOG_LEVEL,
        },
    },
    "root": {
        "handlers": ["console", "file"],
        "level": LOG_LEVEL,
    },
}

# logging.config.dictConfig(LOGGING_CONFIG)
# logger = logging.getLogger("kafka_project")


def setup_logger(
    name: str | None = None, level: str | None = None, json_format: bool = False
) -> logging.Logger:
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(name)

    if level:
        logger.setLevel(level.upper())

    return logger


if __name__ == "__main__":
    log = setup_logger(__name__, level="DEBUG", json_format=False)
    log.debug("Debug message")
    log.info("Info message")
    log.warning("Warning message")
    log.error("Error message")
    log.critical("Critical message")
