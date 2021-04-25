from flowt.scraper import StaticPageScraper
import pytest
from omegaconf import OmegaConf

curPairsInfo = OmegaConf.load(
    'tests/test_scraper/test_data/curPairInformation.json')


@pytest.fixture
def initial_scraper():
    return StaticPageScraper()


class TestStaticPageScraper:

    def test_initialisation(self, initial_scraper):
        assert initial_scraper is not None

    def test_cur_pairs_info(self):
        assert curPairsInfo.eur_usd.idc.url == "https://www.investing.com/currencies/eur-usd-technical"

    def test_scrape_all(self, initial_scraper):
        initial_scraper.scrape_all([
            curPairsInfo.eur_usd.idc.url,
            curPairsInfo.aud_cad.idc.url
        ])
        assert type(initial_scraper.scraped_data) == list

    def test_scrape_all_find_all(self, initial_scraper):
        data = initial_scraper\
            .scrape_all([curPairsInfo.eur_usd.idc.url, curPairsInfo.aud_cad.idc.url])\
            .find_all([
                [curPairsInfo.eur_usd.idc.html_loc.current_values, curPairsInfo.eur_usd.idc.html_loc.time],
                [curPairsInfo.aud_cad.idc.html_loc.current_values, curPairsInfo.aud_cad.idc.html_loc.time]])

        assert (len(data) == 2)
        for cur_data in data:
            for e in cur_data:
                assert type(e) == str
