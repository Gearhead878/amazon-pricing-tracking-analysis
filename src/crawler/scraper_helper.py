from time import sleep
from functools import wraps
import logging
import random
from selenium.common import WebDriverException, TimeoutException


def extract_text(html, xpath):
    """
    The function to reduce duplicated extraction
    :param html:
    :param xpath: xpath path
    :return:
    """
    try:
        result = html.xpath(xpath)
        return result[0].strip() if result else None
    except (IndexError, AttributeError):
        logging.warning('Could not extract text')
        return None

def retry_on_exceptions(_func=None, *, max_retries=3, delay=2,
                        exceptions = (WebDriverException, TimeoutException)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logging.warning(
                        f"Retry {func.__name__} failed on attempt {i+1} with exception {e}"
                    )

                    if i == max_retries - 1:
                        logging.exception(
                            f" All {max_retries} attempts failed for {func.__name__} function"
                        )
                        raise

                    sleep(random.uniform(delay, delay * (i + 1)))
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)

def sanitize_rows_for_sql(rows, fields):
    sanitized = []
    for row in rows:
        sanitized.append({field: row.get(field) for field in fields})
    return sanitized