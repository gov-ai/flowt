import pandas as pd

df = pd.read_csv('flowt/api/helpers/dummy_data.csv')
idx = 0
def get_data(idx):
    return df.iloc[idx]