import logging
import logging.handlers

from app.config import LOGS_DIR

_configured = False


def setup_logging() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    LOGS_DIR.mkdir(exist_ok=True)

    handler = logging.handlers.RotatingFileHandler(
        LOGS_DIR / "db_errors.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logging.getLogger("app").addHandler(handler)
    logging.getLogger("app").setLevel(logging.DEBUG)
