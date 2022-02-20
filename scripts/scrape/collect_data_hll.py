import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from pprint import pprint
import pandas as pd
import schedule
from flowt.scraper import StaticPageScraper


scraper = StaticPageScraper(verbose=0)


def get_stocks():
    stocks = {
        'HLL': {
            'url': 'https://www.investing.com/equities/hindustan-unilever-technical',
            'html_locations': {
                "current_values": [
                    "#quotes_summary_current_data",
                    ".inlineblock",
                    ".top"
                ],
                "time": [
                    "#quotes_summary_current_data",
                    ".lighterGrayFont",
                    ".bold"
                ],
                "secondary_data": [
                    "#quotes_summary_secondary_data",
                    ".bottomText"
                ],
                "hr1_main_tech_summary": [
                    "#techinalContent",
                    "#techStudiesInnerWrap",
                    ".summary",
                    "span"
                ],
                "hr1_full_tech_summary": [
                    "#techinalContent",
                    "#techStudiesInnerWrap"
                ],
                "hr1_pivot_points": [
                    "#techinalContent",
                    "table"
                ],
                "hr1_tech_indicators": [
                    "#techinalContent",
                    ".float_lang_base_1"
                ],
                "hr1_moving_averages": [
                    "#techinalContent", # extractinh the entire portion (specific portion not coming)
                ],
            },
        }
    }
    return stocks


def generate_lists(stocks):
    # size of all the arrays arrays is same.
    # size of all the elements of i-th index of `STATIC_PAGE_LOCATIONS`
    # is same as size of all the elements of i-th index of  `STATIC_PAGE_LOCATION_NAMES`.
    STATIC_PAGE_SYMBOLS = []
    STATIC_PAGE_URLS = []
    STATIC_PAGE_LOCATIONS = []
    STATIC_PAGE_LOCATION_NAMES = []
    for symbol in sorted(list(stocks.keys())):
        STATIC_PAGE_SYMBOLS.append(symbol)
        STATIC_PAGE_URLS.append(stocks[symbol]['url'])
        url_loc_names = []
        url_locations = []
        for loc_name in sorted(list(stocks[symbol]['html_locations'].keys())):
            url_locations.append(stocks[symbol]['html_locations'][loc_name])
            url_loc_names.append(loc_name)
        STATIC_PAGE_LOCATIONS.append(url_locations)
        STATIC_PAGE_LOCATION_NAMES.append(url_loc_names)
    return STATIC_PAGE_SYMBOLS, STATIC_PAGE_URLS, STATIC_PAGE_LOCATIONS, STATIC_PAGE_LOCATION_NAMES



def write_data(utc_time, data, STATIC_PAGE_SYMBOLS, STATIC_PAGE_URLS, STATIC_PAGE_LOCATIONS, STATIC_PAGE_LOCATION_NAMES, SAVE_DIR):
    for idx in range(len(data)):
        symbol = STATIC_PAGE_SYMBOLS[idx]
        write_path = SAVE_DIR / f'{symbol}.csv'

        raw_texts = data[idx]
        names = STATIC_PAGE_LOCATION_NAMES[idx]
        row = {'utc_time': [utc_time]}
        for name, raw in zip(names, raw_texts):
            row[name] = [raw]
        
        mode, header = 'a', False
        if not write_path.exists():
            mode, header = 'w', True
        pd.DataFrame(row).to_csv(str(write_path), mode=mode, index=False, header=header)


def process(SAVE_DIR=Path('stock_data')):
    utc_time = datetime.utcnow()
    print('scraped:', utc_time)
    stocks = get_stocks()
    STATIC_PAGE_SYMBOLS, STATIC_PAGE_URLS, STATIC_PAGE_LOCATIONS, STATIC_PAGE_LOCATION_NAMES = generate_lists(stocks)
    data = scraper.scrape_all(STATIC_PAGE_URLS).find_all(STATIC_PAGE_LOCATIONS)
    write_data(utc_time, data, STATIC_PAGE_SYMBOLS, STATIC_PAGE_URLS, STATIC_PAGE_LOCATIONS, STATIC_PAGE_LOCATION_NAMES, SAVE_DIR)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--save-dir', type=str, default="stock_data")
    parser.add_argument('--limit-sec', type=int, default=60*5)
    parser.add_argument('--interval-min', type=int, default=1)
    return parser.parse_args()

def main():
    args = parse_arguments()

    SAVE_DIR = Path(args.save_dir)
    SAVE_DIR.mkdir(exist_ok=True, parents=True)

    start_time = datetime.utcnow()
    time_limit = args.limit_sec
    time_to_stop = start_time + timedelta(seconds=time_limit)

    print("###########################################################################################")
    print('Process started @', start_time)
    print('Process will stop @', time_to_stop)
    print("###########################################################################################")

    schedule.every(args.interval_min).minutes.do(process)
    while datetime.utcnow()<time_to_stop:
        # print(datetime.utcnow())
        if datetime.utcnow().second==0:
            schedule.run_pending()
            time.sleep(1)

if __name__=='__main__':
    main()
