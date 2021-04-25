from .base_scraper import BaseScraper

class StaticPageScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()

    def scrape_all(self, urls: list):
        scraped_data_acc = []
        for url in urls:
            scraped_data_acc.append(self.scrape(url).scraped_data)
        self.scraped_data = scraped_data_acc
        return self

    def find_all(self, html_locations: list):
        """
        must be used only after `scrape_all` which populates
        `self.scraped_data` with accumulator
        """
        assert type(
            self.scraped_data) is list, "Please accumulate scraped data first using scrape_all"
        assert len(self.scraped_data) == len(html_locations)

        data = []
        for scraped_data, unique_html_paths in zip(self.scraped_data, html_locations):
            print(scraped_data)
            print(unique_html_paths)
            data.append([self._unique_html_path_to_text(scraped_data, unique_html_path)
                         for unique_html_path in unique_html_paths])

        return data

    def _unique_html_path_to_text(self, response, unique_html_path: list,  text=True):
        """ unique_html_path idx 0 has tag-name/id-name/class-name """
        assert unique_html_path != [], "`unique_html_path` is a list of unique html identifiers"

        # return last unique html identifier's text content
        ret = response.html.find(unique_html_path[0], first=True)
        for src in unique_html_path[1:]:
            ret = ret.find(src, first=True)
        return ret.text if text else ret


if __name__ == '__main__':
    curPairsInfo = OmegaConf.load(
        '../../tests/test_scraper/test_data/curPairInformation.json')

    initial_scraper = StaticPageScraper()

    data = initial_scraper\
        .scrape_all([curPairsInfo.eur_usd.idc.url, curPairsInfo.aud_cad.idc.url])\
        .find_all([
            [curPairsInfo.eur_usd.idc.html_loc.current_values, curPairsInfo.eur_usd.idc.html_loc.time],
            [curPairsInfo.aud_cad.idc.html_loc.current_values, curPairsInfo.aud_cad.idc.html_loc.time]])

    print(data)
