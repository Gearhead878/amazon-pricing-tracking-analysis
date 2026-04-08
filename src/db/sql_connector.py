import mysql.connector
from src.config import read_mysql_password
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_mysql_connection(dictionary=False, autocommit=False):
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**read_mysql_password())
        connection.autocommit = autocommit

        cursor = connection.cursor(dictionary=dictionary)
        yield connection, cursor

        connection.commit()
    except mysql.connector.Error as e:
        if connection:
            connection.rollback()
        logger.error(f"MySQL Connection Error: {e}")
        raise
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Unknown Error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
            logger.debug("Cursor has been closed")
        if connection:
            connection.close()
            logger.debug("Connection has been closed")