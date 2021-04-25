from flowt import scraper
from flowt.scraper import BaseScraper
import pytest

URL = "https://www.google.com"


@pytest.fixture
def initial_base_scraper():
    return BaseScraper()


class TestBaseScraper:

    def test_get(self, initial_base_scraper):
        assert initial_base_scraper.get(URL) is not None

    def test_scrape(self, initial_base_scraper):
        assert initial_base_scraper.scrape(URL) is not None
        assert initial_base_scraper.scraped_data is not None

