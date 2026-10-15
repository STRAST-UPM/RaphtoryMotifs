"""Logging setup shared by the CEDAR risk tool.

Logger Config :
    .env:
       LOG_LEVEL=INFO              DEBUG / INFO / WARNING / ERROR (default: INFO)
       LOG_FILE=logs/cedar.log     log file (default: logs/cedar.log), rotated at
                                   midnight into cedar.log.YYYY-MM-DD
       LOG_FILE=none               console only, no file
       LOG_BACKUPS=14              keep only the last 14 days, older files are
                                   deleted (default: unset -> every day is kept)
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path

_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"
_ROOT = "upm_raphtory"
_DEFAULT_FILE = "logs/cedar.log"
_configured = False


def setup_logging(level=None, log_file=None):
    """Configure the logger once: console (stderr) and a daily
    rotated file (LOG_FILE, default logs/cedar.log) """
    global _configured
    root = logging.getLogger(_ROOT)
    if _configured:
        return root

    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    root.setLevel(level)
    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)

    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(formatter)
    root.addHandler(console)

    log_file = log_file or os.getenv("LOG_FILE", _DEFAULT_FILE)
    if log_file.lower() not in {"", "none", "off"}:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=int(os.getenv("LOG_BACKUPS") or 0),
            encoding="utf-8")
        fh.setFormatter(formatter)
        root.addHandler(fh)

    root.propagate = False
    _configured = True
    return root


def get_logger(name=None):
    """A child of the  logger. Call setup_logging() once at
    start-up (after load_dotenv) so LOG_LEVEL / LOG_FILE are honoured."""
    if not name or name == "__main__":
        return logging.getLogger(f"{_ROOT}.main")
    if name == _ROOT or name.startswith(_ROOT + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT}.{name}")


def describe_error(e):
    """"ExceptionType: message" -- the error itself, for a one-line log message
    (log.exception still appends the full traceback below it)."""
    return f"{type(e).__name__}: {e}"
