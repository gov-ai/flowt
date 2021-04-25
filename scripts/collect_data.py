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
        .find_all([
            [pairs_info.eur_usd.idc.html_loc.current_values, pairs_info.eur_usd.idc.html_loc.time],
            [pairs_info.gbp_usd.idc.html_loc.current_values, pairs_info.gbp_usd.idc.html_loc.time],
            [pairs_info.usd_jpy.idc.html_loc.current_values, pairs_info.usd_jpy.idc.html_loc.time],
            [pairs_info.aud_usd.idc.html_loc.current_values, pairs_info.aud_usd.idc.html_loc.time],
            [pairs_info.eur_gbp.idc.html_loc.current_values, pairs_info.eur_gbp.idc.html_loc.time],
            [pairs_info.usd_cad.idc.html_loc.current_values, pairs_info.usd_cad.idc.html_loc.time],
            [pairs_info.usd_chf.idc.html_loc.current_values, pairs_info.usd_chf.idc.html_loc.time],
            [pairs_info.nzd_chf.idc.html_loc.current_values, pairs_info.nzd_chf.idc.html_loc.time],
            [pairs_info.usd_cny.idc.html_loc.current_values, pairs_info.usd_cny.idc.html_loc.time],
            [pairs_info.usd_hkd.idc.html_loc.current_values, pairs_info.usd_hkd.idc.html_loc.time]])


def append_to_df(df, data):
    for idx, (current_values, time) in enumerate(data):
        pair_name = ORDERED_CURPAIR_NAMES[idx]
        df.loc[len(df.index)] = [
            pair_name, current_values, time, str(datetime.now())]
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

    df = pd.DataFrame(dict(pairName=[], currentValues=[], currentTime=[], localTime=[]))
    cur_pairs_info = load_config_file("scripts/cur_pair_information.json")

    for _ in tqdm(range(n_iters)):
        data = get_data(cur_pairs_info)
        df = append_to_df(df, data)

    print(df.head())
    df.to_csv(f'{csv_save_path}', index=False)
