from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


class ChromeDriver:
    def __init__(self, headless=False, window_position='2400,2400', incognito=True, debug=False):
        self.headless = headless
        self.window_position = window_position
        self.incognito = incognito
        self.debug = debug

    def _initial_chrome(self):
        """
        Create Chrome driver with visibility controlled by debug flag.
        """
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        if self.incognito:
            options.add_argument('--incognito')
        if self.debug:
            return options
        if self.headless:
            options.add_argument('--headless')
        else:
            options.add_argument(f'--window-position={self.window_position}')
        return options

    def create_driver(self):
        options = self._initial_chrome()
        driver = webdriver.Chrome(service=Service(), options= options)
        if self.debug:
            driver.maximize_window()
        return driver