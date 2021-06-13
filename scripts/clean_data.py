import os, sys
import pathlib
import numpy as np
import pandas as pd
import clean_current_value

def collect_dfs(base_dir):
    accumulator = []
    for fname in os.listdir(base_dir):

        path = pathlib.Path(base_dir) / fname
        ext = str(path).split('.')[-1].lower()

        if (not path.exists()) or (ext not in ['csv', 'tgz']):
            continue

        yr, mo, dd, tt, _ = fname.split('-')
        df = pd.read_csv(str(path), compression='gzip')
        
        df['year'] = [yr] * len(df)
        df['month'] = [mo] * len(df)
        df['date'] = [dd] * len(df)
        df['time'] = [tt] * len(df)
        
        accumulator.append(df)
    
    return pd.concat(accumulator)


def clean_df(df, current_values_column='currentValues', current_time_col='currentTime'):

    cur_pair_col_name = df.columns[0]
    unique_scrapes = np.unique(np.array(df[cur_pair_col_name].value_counts()))
    if len(unique_scrapes) != 1:
        raise Exception('All cur pair scrapes must be of same count.')

    df['closedPrice'], df['changePer1'], df['changePer2'], \
    df['closedPriceMissing'], df['changePer1Missing'], df['changePer2Missing'] = \
        zip(*df[current_values_column].map(clean_current_value.clean))

    df['hh'], df['mm'], df['ss'], \
    df['hhMissing'], df['mmMissing'], df['ssMissing'] = \
        zip(*df[current_time_col].map(clean_current_value.clean_time))

    # todo: add day, week, month and year. raw values will be normalized in preprocessing step.
    return clean_current_value\
        .featurize_single_curpair(df, cur_pair='eur_usd',
                                  targets=['closedPrice', 'changePer1', 'changePer2'],
                                  filter_features=['closedPrice', 'changePer1', 'changePer2',
                                                   'closedPriceMissing', 'changePer1Missing', 'changePer2Missing',
                                                   'hh', 'mm', 'ss',
                                                   'hhMissing', 'mmMissing', 'ssMissing'])
    


DATAFRAMES_DIR = sys.argv[1]
SAVE_DIR = sys.argv[2]

os.makedirs(SAVE_DIR, exist_ok=True)

df = collect_dfs(base_dir=DATAFRAMES_DIR)
xsdf, ysdf = clean_df(df)

print('Train data frame shape:', xsdf.shape)
print('Test data frame shape:', ysdf.shape)

xsdf.to_csv(str(pathlib.Path(SAVE_DIR) / 'xsdf.csv'), index=False)
ysdf.to_csv(str(pathlib.Path(SAVE_DIR) / 'ysdf.csv'), index=False)