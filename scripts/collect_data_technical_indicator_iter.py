# usage:  python -m scripts.collect_data --save-path data/temp.csv --iters 1

from tqdm import tqdm
import argparse
from datetime import datetime
import pandas as pd

from flowt.scraper import StaticPageScraper
from flowt.utils import load_config_file
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
    parser.add_argument('--save-path', type=str, required=True)
    parser.add_argument('--iters', type=int, default=3)
    return parser.parse_args()


if __name__ == '__main__':

    args = parse_arguments()
    csv_save_path = args.save_path
    n_iters = args.iters

    df = pd.DataFrame(dict(pairName=[], currentValues=[], currentTime=[], localTime=[], secondaryData=[],
                           hr1MainSumm=[], hr1FullSumm=[], hr1PivotPoints=[], hr1TechIndicators=[],))
    cur_pairs_info = load_config_file("scripts/cur_pair_info_technical_indicator.json")

    for _ in tqdm(range(n_iters)):
        data = get_data(cur_pairs_info)
        df = append_to_df(df, data)

    print(df.head())
    
    null_columns=df.columns[df.isnull().any()]
    print("Total null cols:", len(null_columns))
    if len(null_columns) > 0:
        print(df[null_columns].isnull().sum())

    df.to_csv(f'{csv_save_path}', index=False)
