## Scraper Interface

- Static Page Scraper

    ```python
    from flowt.scrapers import StaticPageScraper
    from flowt.scraper import CurPairInfo 

    scraper = StaticPageScraper()
    
    URI = CurPairInfo.eurusd.idc.uri
    # URI = "https://www.investing.com/currencies/eur-usd-technical"

    HTML_LOCATION = CurPairInfo.eurusd.idc.techinical_quotes_location
    # HTML_LOCATIONS = [
    #   'current_value_1': ['#quotes_summary_current_data', '.inlineblock', '.top'],
    #   'current_value_2': ['#qquotes_summary_secondary_data'],
    #   'time': ['#quotes_summary_current_data', '.lighterGrayFont', '.bold']
    # ]

    data_dic = scraper
        .scrape(URI)\
        .find(HTML_LOCATIONS)
    ```

    For multiple scraping
    ```python
    URIS = [
        CurPairInfo.eurusd.idc.uri,
        CurPairInfo.gbpusd.idc.uri,
        CurPairInfo.eurnzd.idc.uri
    ]

    LOCATIONS = [

    ]
    
    data_dic = scraper
        .scrape(URIS)\
        .find(HTMLLocation.QUOTES_SUMMARY)
    ```