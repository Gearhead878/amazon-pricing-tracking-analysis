from src.crawler.scraper_helper import retry_on_exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.crawler.amz_price_selectors import PAGE_READY_XPATH
import logging

logger = logging.getLogger(__name__)

class AmazonPageFetcher:
    def __init__(self, driver):
        self.driver = driver

    @retry_on_exceptions
    def _load_page(self, url, page_ready_xpath):
        driver = self.driver
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, page_ready_xpath))
        )
        return True, driver.page_source

    def _fetch_amazon_page(self, url, page_ready_xpath):
        try:
            return self._load_page(url, page_ready_xpath)
        except Exception:
            logger.exception("Max retries reached for %s", url)
            return False, None

    def fetch(self, url, page_ready_xpath = PAGE_READY_XPATH):
        return self._fetch_amazon_page(url, page_ready_xpath=page_ready_xpath)
