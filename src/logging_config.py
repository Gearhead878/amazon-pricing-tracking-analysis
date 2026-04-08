import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from src.config import LOG_CONFIG

def setup_logging():
    parent_folder = Path(__file__).resolve().parent.parent
    log_dir = parent_folder / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "app.log"

    fmt = logging.Formatter(
        LOG_CONFIG["format"]
    )

    level = logging.getLevelName(LOG_CONFIG["level"])

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        return

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)

    fh = RotatingFileHandler(
        log_file, maxBytes=LOG_CONFIG["max_bytes"], backupCount=LOG_CONFIG["backup_count"], encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)