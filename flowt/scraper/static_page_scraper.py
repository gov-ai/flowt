import os
from .base_scraper import BaseScraper

try:
    TERMINAL_WIDTH = int(os.popen('stty size', 'r').read().split()[1])
except:
    TERMINAL_WIDTH = 40

class StaticPageScraper(BaseScraper):
    def __init__(self, verbose=0) -> None:
        """
        :verbose: {int} 0 no logs; 1 important logs; 2 all logs;  
        """
        super().__init__()
        self._v = verbose

    def scrape_all(self, urls: list):
        scraped_data_acc = []
        for url in urls:
            try:
                scraped_data_acc.append(self.scrape(url).scraped_data)
            except:
                print('Could not scrape {url}')
                
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
            data.append([self._unique_html_path_to_text(scraped_data, unique_html_path)
                         for unique_html_path in unique_html_paths])

        return data

    def _unique_html_path_to_text(self, response, unique_html_path: list,  text=True):
        """ unique_html_path idx 0 has tag-name/id-name/class-name """
        assert unique_html_path != [], "`unique_html_path` is a list of unique html identifiers"

        # return last unique html identifier's text content
        ret = response.html.find(unique_html_path[0], first=True)
        
        if self._v > 0:
            print("="*TERMINAL_WIDTH)
            print(response, "::", response.url, "::", "✅", unique_html_path[0], end=" ---> ")
        
        for src in unique_html_path[1:]:
            if ret is None: 
                if self._v > 0: 
                    print("❌", src, end="")            
                break
            
            ret = ret.find(src, first=True)
            
            if self._v > 0: 
                print("✅", src, end=" ---> ")            
        
        if self._v > 0: 
            print()
            print("="*TERMINAL_WIDTH)
            
        if self._v > 1:
            print(ret.text if (text and ret is not None) else ret)

        return ret.text if (text and ret is not None) else ret


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
