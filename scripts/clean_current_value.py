import pandas as pd
import numpy as np

from pprint import pprint
from tqdm import tqdm

def clean(text):
    
    if type(text) is not str:
        return (np.nan, np.nan, np.nan, 1, 1, 1)

    splitted = text.split(' ')
    close_price_missing, change_per1_missing, change_per2_missing = 0, 0, 0

    try:
        close_price = float(splitted[0])
    except:
        close_price = np.nan
        close_price_missing = 1

    try:
        change_per1 = float(splitted[1].replace('%', ''))
    except:
        change_per1 = np.nan
        change_per1_missing = 1

    try:
        change_per2 =  float(splitted[3].replace('%', ''))
    except:
        change_per2 = np.nan
        change_per2_missing = 1

    return (close_price, change_per1, change_per2, 
            close_price_missing, change_per1_missing, change_per2_missing)


def clean_time(cur_time):
    
    if type(cur_time) is not str:
        return [np.nan, np.nan, np.nan, 1, 1, 1]

    if (':' not in cur_time):
        return [np.nan, np.nan, np.nan, 1, 1, 1]
    
    hh, mm, ss = cur_time.split(':')
    hh_missing, mm_missing, ss_missing = 0, 0, 0

    try:
        hh = float(hh)
    except:
        hh = np.nan
        hh_missing = 1
    
    try:
        mm = float(mm)
    except:
        mm = np.nan
        mm_missing = 1

    try:
        ss = float(ss)
    except:
        ss = np.nan
        ss_missing = 1

    return hh, mm, ss, hh_missing, mm_missing, ss_missing


def _filter(tmp_df, cur_pair_col_name, cur_pair_name, colname):
    try:
        ret = tmp_df[tmp_df[cur_pair_col_name]==cur_pair_name][colname].values[0]
    except:
        ret = np.nan

    return ret

def featurize_df(df, filter_features):
    # assert that there is no missing value in cur pair col name
    # note that `unique_cur_pairs` can be strictly mentioned non-programatically.
    cur_pair_col_name = df.columns[0]
    unique_cur_pairs = list(df[cur_pair_col_name].value_counts().keys())
    num_unique_cur_pairs = len(unique_cur_pairs)


    """
    for colname in df.columns:
        if 'Missing' in colname:
            print("===")
            print(colname)
            print("===")
            print(df[colname].value_counts())
    """

    new_rows = []
    for lidx in tqdm(range(0, len(df), num_unique_cur_pairs)):
        # df for all cur pairs at a given instance of time when scraped       
        tmp_df = df.iloc[lidx:lidx+num_unique_cur_pairs, :][[cur_pair_col_name] + filter_features]
        

        tmp_dic = dict([(f'{cur_pair_name}_{colname}', [_filter(tmp_df, cur_pair_col_name, cur_pair_name, colname)])
                            for colname in tmp_df.columns 
                            for cur_pair_name in unique_cur_pairs])
        
        new_rows.append(pd.DataFrame(tmp_dic))

    return pd.concat(new_rows)


def featurize_single_curpair(df, cur_pair, targets, filter_features):
    df = featurize_df(df, filter_features)

    target_cols = []
    for colname in df.columns:
        for t in targets:
            if cur_pair+"_"+t == colname:
                target_cols.append(colname)

    ys_df = df[target_cols]
    df.drop(target_cols, axis=1, inplace=True)

    return df, ys_df