from . base_scraper import BaseScraper


class StaticPageScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()

    def scrape(self):
        ...
