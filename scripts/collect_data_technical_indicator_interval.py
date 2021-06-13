# make sure interval is large enough for the process otherwise, it will continue to run even if sc job is cancelled
# usage:  python -m scripts.collect_data_technical_indicator_interval --interval 30 --limit 300 --save-dir scraped-data
# usage-ga: python -m scripts.collect_data_technical_indicator_interval --interval 60 --limit 1080000 --save-dir scraped-data

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm

import pandas as pd

from flowt.scraper import StaticPageScraper
from flowt.utils import load_config_file, get_scheduler
from flowt import scraper

ORDERED_CURPAIR_NAMES = ['eur_usd', 'gbp_usd', 'usd_jpy', 'aud_usd', 'eur_gbp', 'usd_cad',
                         'usd_chf', 'nzd_chf', 'usd_cny', 'usd_hkd', ]

scraper = StaticPageScraper()


def get_data(pairs_info):
    return scraper\
        .scrape_all([pairs_info.eur_usd.idc.url, pairs_info.gbp_usd.idc.url, pairs_info.usd_jpy.idc.url,
                     pairs_info.aud_usd.idc.url, pairs_info.eur_gbp.idc.url, pairs_info.usd_cad.idc.url,
                     pairs_info.usd_chf.idc.url, pairs_info.nzd_chf.idc.url, pairs_info.usd_cny.idc.url,
                     pairs_info.usd_hkd.idc.url, ])\
        .find_all([ # note: locations for all pairs are same as eur-usd so did not replace (except for gbp-usd to show an example)
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.gbp_usd.idc.html_loc.current_values, pairs_info.gbp_usd.idc.html_loc.time, pairs_info.gbp_usd.idc.html_loc.secondary_data,
             pairs_info.gbp_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.gbp_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.gbp_usd.idc.html_loc.hr1_pivot_points, pairs_info.gbp_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time, pairs_info.eur_usd.idc.html_loc.secondary_data,
             pairs_info.eur_usd.idc.html_loc.hr1_main_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_full_tech_summary, pairs_info.eur_usd.idc.html_loc.hr1_pivot_points, pairs_info.eur_usd.idc.html_loc.hr1_tech_indicators],
             # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        ],)



def append_to_df(df, data):
    for idx, (current_values, time, secondary_data, hr1_main_summ, hr1_full_summ, hr1_pivot_points, hr1_tech_indicators) in enumerate(data):
        pair_name = ORDERED_CURPAIR_NAMES[idx]
        df.loc[len(df.index)] = [
            pair_name, current_values, time, str(datetime.now()), secondary_data, hr1_main_summ, hr1_full_summ, hr1_pivot_points, hr1_tech_indicators]
    return df


def parse_arguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--save-dir', type=str, default="scraped-data")
    parser.add_argument('--interval', type=int, default=60*5)
    parser.add_argument('--limit', type=int, default=60*5*2)
    return parser.parse_args()


if __name__ == '__main__':

    args = parse_arguments()
    csv_save_dir = Path(args.save_dir)
    Path.mkdir(csv_save_dir, exist_ok=True)
    
    time_interval = args.interval
    start_time = datetime.now()
    time_limit = args.limit
    time_to_stop = start_time + timedelta(seconds=time_limit)

    print("###########################################################################################")
    print('Process start @', start_time)
    print('Process stop @', time_to_stop)
    print("###########################################################################################")

    df = pd.DataFrame(dict(pairName=[], currentValues=[], currentTime=[], localTime=[], secondaryData=[],
                           hr1MainSumm=[], hr1FullSumm=[], hr1PivotPoints=[], hr1TechIndicators=[],))
    cur_pairs_info = load_config_file("scripts/cur_pair_info_technical_indicator.json")


    def collect_data(sc, tts, df):
        data = get_data(cur_pairs_info)
        df = append_to_df(df, data)

        if datetime.now() < tts:
            sc.enter(time_interval, priority, collect_data, (sc, tts, df))

    s = get_scheduler()
    priority = 1
    s.enter(time_interval, priority, collect_data, (s, time_to_stop, df))
    s.run()
    
    end_time = datetime.now()
    
    print("###########################################################################################")
    print('Process stopped @', end_time)
    print('Process supposed to stop @', time_to_stop)
    print('Number of iterations:', len(df) / len(ORDERED_CURPAIR_NAMES))
    print("###########################################################################################")

    print(df.head())
    print('full shape:', df.shape)

    null_columns=df.columns[df.isnull().any()]
    print("Total null cols:", len(null_columns))
    if len(null_columns) > 0:
        print(df[null_columns].isnull().sum())


    start_time = str(start_time).replace(' ', '-')
    end_time = str(end_time).replace(' ', '-')
    csv_save_name = f'start@{start_time}-end@{end_time}-interval{time_interval}-limit{time_limit}.csv'
    df.to_csv(str(csv_save_dir / csv_save_name), index=False)
