from typing import Union
from loguru import logger
from requests_html import HTMLSession
import requests


class BaseScraper:
    def __init__(self):
        self._sess = HTMLSession()
        self.scraped_data = None

    def get(self, url, **kwargs):
        return self._sess.get(url, **kwargs)

    def scrape(self, url: str):
        """
        Fetch URL and store scraped data inside `scraped_data` property
        """
        try:
            self.scraped_data = self.get(url)
            return self
        except requests.exceptions.RequestException as e:
            logger.exception(f"Could not fetch {url}. Error: {e}")
        except Exception as e:
            logger.exception(f"Runtime error: {e}")

    @property
    def scraped_data(self):
        return self._scraped_data

    @scraped_data.setter
    def scraped_data(self, data):
        self._scraped_data = data
