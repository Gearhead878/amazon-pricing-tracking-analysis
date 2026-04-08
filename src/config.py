from dotenv import load_dotenv
import os
from pathlib import Path

def read_mysql_password():
    BASE_DIR = Path(__file__).resolve().parent.parent
    load_dotenv(BASE_DIR / ".env")
    DB_CONFIG = {
        "host": os.getenv("MYSQL_HOST"),
        "database": os.getenv("MYSQL_DB"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
    }

    return DB_CONFIG

LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    'max_bytes': 2_000_000,
    'backup_count': 5,
}

PRICE_FILE_VALIDATION = {
    "required_columns": ['ASIN#', 'Model No.', 'RRP\nAUD'],
    "date_regex": r"(\d{4}\.\d{2})"
}


