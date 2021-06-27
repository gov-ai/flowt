import os
from pathlib import Path
from tqdm import tqdm

import numpy as np
import pandas as pd

# from sklearn.preprocessing import MinMaxScaler


def process_cat_cols(df):
    # !temp
    # todo: one-hot encoding instead.
    temp_map = {
        # *_main_summary_id
        # *_bear_power_13_action
        'Oversold': 3,
        'Strong Sell': -2,
        'Sell': -1,
        'Neutral': 0,
        'Buy': 1,
        'Strong Buy': 2,
        'Overbought': 3,

        # some outlier
        'BUY': 1,

        # i dont know what i am doing
        'Less Volatility': -10,
        'High Volatility': 10, 
        
        # *_gmt_mo
        'Jan': 1,
        'Feb': 2,
        'Mar': 3,
        'Apr': 4,
        'May': 5,
        'Jun': 6,
        'Jul': 7,
        'Aug': 8,
        'Sep': 9,
        'Oct': 10,
        'Nov': 11,
        'Dec': 12,
    }

    # !temporary
    def remove_am_if_exists(obj):
        if type(obj) is not str:
            return obj

        if 'AM' not in obj:
            return obj
        
        return int(obj.replace('AM', ''))

    cat_vals = []
    for c in df.columns:
        if str(df[c].dtype)=='object':
            # df[c] = df[c].map(temp_map)
            
            # !temp
            df[c] = df[c].apply(remove_am_if_exists)
            df[c] = df[c].map(temp_map)
            
    return df


def to_time_series(df, step_xs=200, step_ys=15):
    # print(df.info())
    # dtypes: float64(530), int64(1160)
    
    # sc = MinMaxScaler(feature_range = (0, 1))
    # df = sc.fit_transform(df)

    xs = []
    ys = []
    for lx in tqdm(range(0, len(df)-step_xs)):
        hx = lx+step_xs
        ly = hx
        hy = ly+step_ys

        chunk_xs = df.iloc[lx:hx,:]
        chunk_ys = df.iloc[ly:hy,:]

        xs.append(chunk_xs.values)
        ys.append(chunk_ys.eur_usd_cur_close.values)

    print('converting to np array ...')
    xs = np.array(xs)
    ys = np.array(ys)

    print('xs shape', xs.shape)
    print('ys shape', ys.shape)

    return xs, ys

        


if __name__ == "__main__":
    DATA_PATH = Path('/Users/mac/Desktop/github/projects/prem/flowt/data/clean/poc_6_days_data_10_pairs.csv')
    SPLIT_RATIO  = 0.8

    df = pd.read_csv(DATA_PATH)
    df = process_cat_cols(df)
    
    xs, ys = to_time_series(df)

    split_test = int(len(xs)*SPLIT_RATIO)
    xs_test = xs[split_test:]
    ys_test = ys[split_test:]

    xs_temp = xs[:split_test]
    ys_temp = ys[:split_test]

    split_train = len(xs_temp)-len(xs_test)
    xs_train = xs_temp[:split_train]
    ys_train = ys_temp[:split_train]
    xs_dev = xs_temp[split_train:]
    ys_dev = ys_temp[split_train:]

    print('train:', xs_train.shape, ys_train.shape)
    print('dev:', xs_dev.shape, ys_dev.shape)
    print('test:', xs_test.shape, ys_test.shape)

