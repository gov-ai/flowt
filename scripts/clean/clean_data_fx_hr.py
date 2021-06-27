"""
this script cleans collected csv file with hourly technical indicators (minute close price) 
to more organized dsv with technical indicator columns for each cur pair

usage: 
  python -m scripts.clean.clean_data_fx_hr <path/to/tg/csv/files> <csv/write/path>

example:
  python -m scripts.clean.clean_data_fx_hr data/files data/clean/poc_6_days_data_10_pairs.csv
"""

import re, os, sys
from pathlib import Path

import numpy as np
import pandas as pd


def _clean_primary_data(primary_data, pair_name):
  cur_close, cur_per1, cur_per2 = 0, 0, 0
  cur_close_missing, cur_per1_missing, cur_per2_missing = 1, 1, 1

  if len(primary_data.split()) != 3:
    print(f'[{pair_name}] Unexpected primary data! data must be corrupted or source layout might have been changed.'
          'If you see this message more than once, try adding logic to handle unexpected data format\n')
    return cur_close, cur_per1, cur_per2, cur_close_missing, cur_per1_missing, cur_per2_missing 
  
  cur_close_missing, cur_per1_missing, cur_per2_missing = 0,0,0
  cur_close, cur_per1, cur_per2 = primary_data.split()
  cur_per2 = cur_per2.replace('%', '')
  return cur_close, cur_per1, cur_per2, cur_close_missing, cur_per1_missing, cur_per2_missing


def _clean_scraped_time(scraped_time, pair_name):
  scrape_hr, scrape_min, scrape_sec, scrape_hr_missing, scrape_min_missing, scrape_sec_missing = 0, 0, 0, 1, 1, 1
  
  if (type(scraped_time) is str) and (":" in scraped_time):
    scraped_time = scraped_time.split(":")
  else:
    print(f'[{pair_name}] scraped time {scraped_time} is supposed to be str. Replacing with empty string.')
    scraped_time = []

  if (len(scraped_time) != 3):
    print(f'[{pair_name}] Unexpected scraped time! data must be corrupted or source layout might have been changed.'
          'If you see this message more than once, try adding logic to handle unexpected data format.\n')
    return scrape_hr, scrape_min, scrape_sec, scrape_hr_missing, scrape_min_missing, scrape_sec_missing 

  scrape_hr, scrape_min, scrape_sec = scraped_time
  scrape_hr_missing, scrape_min_missing, scrape_sec_missing = 0,0,0
  return scrape_hr, scrape_min, scrape_sec, scrape_hr_missing, scrape_min_missing, scrape_sec_missing


def _clean_time_write_local(time_write_local, pair_name):
  write_hr, write_min, write_sec = 0,0,0
  write_hr_missing, write_min_missing, write_sec_missing = 1,1,1

  if (type(time_write_local) is not str) or (':' not in time_write_local):
    print(f'[{pair_name}] Unexpected local write time: {time_write_local}! data must be corrupted or source layout might have been changed.'
          'If you see this message more than once, try adding logic to handle unexpected data format.\n')

    return write_hr, write_min, write_sec, write_hr_missing, write_min_missing, write_sec_missing 
  
  write_hr, write_min, write_sec = time_write_local.split()[1].split(":")
  write_hr_missing, write_min_missing, write_sec_missing = 0, 0, 0
  return write_hr, write_min, write_sec, write_hr_missing, write_min_missing, write_sec_missing


def _clean_secondary_data(secondary_data, pair_name):
  prev_close, bid, ask, day_max, day_min = 0, 0, 0, 0, 0
  prev_close_missing, bid_missing, ask_missing, day_max_missing, day_min_missing = 1,1,1,1,1

  splitted = secondary_data.split()
  if len(splitted) != 12:
    print(f'[{pair_name}] Unexpected secondary_data: {secondary_data}! data must be corrupted or source layout might have been changed.'
          'If you see this message more than once, try adding logic to handle unexpected data format.\n')
    return (prev_close, bid, ask, day_max, day_min,
            prev_close_missing, bid_missing, ask_missing, day_max_missing, day_min_missing)

  _, _, prev_close, _, bid, _, ask, _, _, day_max, _,  day_min = splitted
  return (prev_close, bid, ask, day_max, day_min,
            prev_close_missing, bid_missing, ask_missing, day_max_missing, day_min_missing)


# should not include 0 (0 is for missing/corrupted data)
summary_text_to_id = {
    'strong sell': 1,
    'strong buy': 2,
    'buy': 3,
    'sell': 4,
    'neutral': 5,
}

""" SAMPLE SPLITTED TEXT BY \n
['Summary:Strong Sell',
 'Moving Averages:Strong SellBuy (0)Sell (12)',
 'Technical Indicators:Strong SellBuy (1)Sell (8)']
"""
def _clean_tech_summary_main(main_summary, pair_name):
  main_summary_id, ma_summary_id, ma_tot_sell, ma_tot_buy, tech_ind_summary_id, tech_ind_tot_sell, tech_ind_tot_buy = 0,0,0,0,0,0,0
  main_summary_id_missing, ma_summary_id_missing, ma_tot_sell_missing, ma_tot_buy_missing, tech_ind_summary_id_missing, tech_ind_tot_sell_missing, tech_ind_tot_buy_missing = 1,1,1,1,1,1,1

  splitted = main_summary.split("\n")
  if len(splitted) != 3:
    print(f'[{pair_name}] Unexpected main_summary table format: {main_summary}! data must be corrupted or source layout might have been changed.'
      'If you see this message more than once, try adding logic to handle unexpected data format.\n')
    return (main_summary_id, ma_summary_id, ma_tot_sell, ma_tot_buy, tech_ind_summary_id, tech_ind_tot_sell, tech_ind_tot_buy,
            main_summary_id_missing, ma_summary_id_missing, ma_tot_sell_missing, ma_tot_buy_missing, tech_ind_summary_id_missing, tech_ind_tot_sell_missing, tech_ind_tot_buy_missing)

  # groups
  main_summary, ma_data, tech_ind_data = splitted


  # main summary
  main_summary = main_summary.replace("Summary:", "")
  try:
    main_summary_id = main_summary # summary_text_to_id[main_summary.lower()]
    main_summary_id_missing = 0
  except:
    print(f'[{pair_name}] Unexpected main_summary: {main_summary}! data must be corrupted or source layout might have been changed.')
    main_summary_id = 0
    main_summary_id_missing = 1


  # moving average data
  splitted = re.findall('[A-Z][^A-Z]*', ma_data)
  
  try:
    ma_tot_sell = splitted[-1].split()[-1].replace("(", "").replace(")", "")
    ma_tot_sell_missing = 0
  except:
    print(f'[{pair_name}] (ma_tot_sell) unexpected splitted: {splitted}')
    ma_tot_sell = 0
    ma_tot_sell_missing = 1

  try:
    ma_tot_buy = splitted[-2].split()[-1].replace("(", "").replace(")", "")
    ma_tot_buy_missing = 0
  except:
    print(f'[{pair_name}] (ma_tot_buy) unexpected splitted: {splitted}')
    ma_tot_buy = 0
    ma_tot_buy_missing = 1

  try:
    ma_summary_text= ''.join(splitted[2:-2])
    ma_summary_id = ma_summary_text #summary_text_to_id[ma_summary_text.lower()]
    ma_summary_id_missing = 0
  except:
    print(f'[{pair_name}] (ma_summary_id) unexpected splitted: {splitted}')
    ma_summary_id = 0
    ma_summary_id_missing = 1


  # tech indicator data
  splitted = re.findall('[A-Z][^A-Z]*', tech_ind_data)
  
  try:
    tech_ind_tot_sell = splitted[-1].split()[-1].replace("(", "").replace(")", "")
    tech_ind_tot_sell_missing = 0
  except:
    print(f'[{pair_name}] (tech_ind_tot_sell) unexpected splitted: {splitted}')
    tech_ind_tot_sell = 0
    tech_ind_tot_sell_missing = 1

  try:
    tech_ind_tot_buy = splitted[-2].split()[-1].replace("(", "").replace(")", "")
    tech_ind_tot_buy_missing = 0
  except:
    print(f'[{pair_name}] (tech_ind_tot_buy) unexpected splitted: {splitted}')
    tech_ind_tot_buy = 0
    tech_ind_tot_buy_missing = 1

  try:
    tech_ind_summary_text = ''.join(splitted[2:-2])
    tech_ind_summary_id = tech_ind_summary_text #summary_text_to_id[tech_ind_summary_text.lower()]
    tech_ind_summary_id_missing = 0
  except:
    print(f'[{pair_name}] (tech_ind_summary_id) unexpected: {tech_ind_summary_text} splitted: {splitted}')
    tech_ind_summary_id = 0
    tech_ind_summary_id_missing = 1

  return (main_summary_id, ma_summary_id, ma_tot_sell, ma_tot_buy, tech_ind_summary_id, tech_ind_tot_sell, tech_ind_tot_buy, 
          main_summary_id_missing, ma_summary_id_missing, ma_tot_sell_missing, ma_tot_buy_missing, tech_ind_summary_id_missing, tech_ind_tot_sell_missing, tech_ind_tot_buy_missing)


def _clean_pivot_points_table(pivot_points_table, pair_name):
  
  pivot_classic_s3, pivot_classic_s2, pivot_classic_s1, pivot_classic_points, pivot_classic_r1, pivot_classic_r2, pivot_classic_r3 = 0,0,0,0,0,0,0
  pivot_classic_s3_missing, pivot_classic_s2_missing, pivot_classic_s1_missing, pivot_classic_points_missing, pivot_classic_r1_missing, pivot_classic_r2_missing, pivot_classic_r3_missing = 1,1,1,1,1,1,1

  pivot_fibonacci_s3, pivot_fibonacci_s2, pivot_fibonacci_s1, pivot_fibonacci_points, pivot_fibonacci_r1, pivot_fibonacci_r2, pivot_fibonacci_r3 = 0,0,0,0,0,0,0
  pivot_fibonacci_s3_missing, pivot_fibonacci_s2_missing, pivot_fibonacci_s1_missing, pivot_fibonacci_points_missing, pivot_fibonacci_r1_missing, pivot_fibonacci_r2_missing, pivot_fibonacci_r3_missing = 1,1,1,1,1,1,1
  
  pivot_camarilla_s3, pivot_camarilla_s2, pivot_camarilla_s1, pivot_camarilla_points, pivot_camarilla_r1, pivot_camarilla_r2, pivot_camarilla_r3 = 0,0,0,0,0,0,0
  pivot_camarilla_s3_missing, pivot_camarilla_s2_missing, pivot_camarilla_s1_missing, pivot_camarilla_points_missing, pivot_camarilla_r1_missing, pivot_camarilla_r2_missing, pivot_camarilla_r3_missing = 1,1,1,1,1,1,1

  pivot_woodies_s3, pivot_woodies_s2, pivot_woodies_s1, pivot_woodies_points, pivot_woodies_r1, pivot_woodies_r2, pivot_woodies_r3 = 0,0,0,0,0,0,0
  pivot_woodies_s3_missing, pivot_woodies_s2_missing, pivot_woodies_s1_missing, pivot_woodies_points_missing, pivot_woodies_r1_missing, pivot_woodies_r2_missing, pivot_woodies_r3_missing = 1,1,1,1,1,1,1

  pivot_demarks_s3, pivot_demarks_s2, pivot_demarks_s1, pivot_demarks_points, pivot_demarks_r1, pivot_demarks_r2, pivot_demarks_r3 = 0,0,0,0,0,0,0
  pivot_demarks_s3_missing, pivot_demarks_s2_missing, pivot_demarks_s1_missing, pivot_demarks_points_missing, pivot_demarks_r1_missing, pivot_demarks_r2_missing, pivot_demarks_r3_missing = 1,1,1,1,1,1,1
  
  
  pivot_points_list = pivot_points_table.split()
  if len(pivot_points_list) != 49:
    print(f'[{pair_name}] Unexpected pivot_points_table size: {len(pivot_points_list)}! data must be corrupted or source layout might have been changed.')
    return (
        pivot_classic_s3, pivot_classic_s2, pivot_classic_s1, pivot_classic_points, pivot_classic_r1, pivot_classic_r2, pivot_classic_r3,
        pivot_classic_s3_missing, pivot_classic_s2_missing, pivot_classic_s1_missing, pivot_classic_points_missing, pivot_classic_r1_missing, pivot_classic_r2_missing, pivot_classic_r3_missing,
        pivot_fibonacci_s3, pivot_fibonacci_s2, pivot_fibonacci_s1, pivot_fibonacci_points, pivot_fibonacci_r1, pivot_fibonacci_r2, pivot_fibonacci_r3,
        pivot_fibonacci_s3_missing, pivot_fibonacci_s2_missing, pivot_fibonacci_s1_missing, pivot_fibonacci_points_missing, pivot_fibonacci_r1_missing, pivot_fibonacci_r2_missing, pivot_fibonacci_r3_missing,
        pivot_camarilla_s3, pivot_camarilla_s2, pivot_camarilla_s1, pivot_camarilla_points, pivot_camarilla_r1, pivot_camarilla_r2, pivot_camarilla_r3,
        pivot_camarilla_s3_missing, pivot_camarilla_s2_missing, pivot_camarilla_s1_missing, pivot_camarilla_points_missing, pivot_camarilla_r1_missing, pivot_camarilla_r2_missing, pivot_camarilla_r3_missing,       
        pivot_woodies_s3, pivot_woodies_s2, pivot_woodies_s1, pivot_woodies_points, pivot_woodies_r1, pivot_woodies_r2, pivot_woodies_r3,
        pivot_woodies_s3_missing, pivot_woodies_s2_missing, pivot_woodies_s1_missing, pivot_woodies_points_missing, pivot_woodies_r1_missing, pivot_woodies_r2_missing, pivot_woodies_r3_missing,
        pivot_demarks_s3, pivot_demarks_s2, pivot_demarks_s1, pivot_demarks_points, pivot_demarks_r1, pivot_demarks_r2, pivot_demarks_r3,
        pivot_demarks_s3_missing, pivot_demarks_s2_missing, pivot_demarks_s1_missing, pivot_demarks_points_missing, pivot_demarks_r1_missing, pivot_demarks_r2_missing, pivot_demarks_r3_missing,
    ) 
  
  lidx = 9
  hidx = lidx+8
  ret = []
  while hidx < len(pivot_points_list)+1:
    data = pivot_points_list[lidx:hidx]
    # print(data)
    # ['Classic', '1.2108', '1.2111', '1.2115', '1.2118', '1.2122', '1.2125', '1.2129']
    # ['Fibonacci', '1.2111', '1.2114', '1.2115', '1.2118', '1.2121', '1.2122', '1.2125']
    # ['Camarilla', '1.2116', '1.2117', '1.2117', '1.2118', '1.2119', '1.2119', '1.2120']
    # ["Woodie's", '1.2108', '1.2111', '1.2115', '1.2118', '1.2122', '1.2125', '1.2129']
    # ["DeMark's", '-', '-', '1.2116', '1.2119', '1.2123', '-', '-']

    pivot_name = data[0]
    values_text = data[1:]

    acc_pivot_values = []
    acc_is_missing = []
    for pv in values_text:
      try:
        pivot_val = float(pv)
        acc_pivot_values.append(pivot_val)
        acc_is_missing.append(0)
      except:
        # temporarily commented. logic is same.
        print(f'[{pair_name}] {pivot_name} value: {pv} corrupted / not available')
        pivot_val = 0
        acc_pivot_values.append(pivot_val)
        acc_is_missing.append(1)

    ret.extend(acc_pivot_values)
    ret.extend(acc_is_missing)

    lidx = hidx
    hidx = hidx+8

  return ret


def _clean_tech_ind_table(tech_ind_table, pair_name):
  rsi_14_val, rsi_14_action = 0, 0
  rsi_14_val_missing, rsi_14_action_missing = 1,1 
  
  stoch_9_6_val, stoch_9_6_action = 0,0
  stoch_9_6_val_missing, stoch_9_6_action_missing =1,1
  
  stochrsi_14_val, stochrsi_14_action=0,0
  stochrsi_14_val_missing, stochrsi_14_action_missing=1,1
  
  macd_12_26_val, macd_12_26_action=0,0
  macd_12_26_val_missing, macd_12_26_action_missing=1,1
  
  adx_14_val, adx_14_action=0,0
  adx_14_val_missing, adx_14_action_missing=1,1
  
  williams_per_r_val, williams_per_r_action=0,0
  williams_per_r_val_missing, williams_per_r_action_missing=1,1
  
  cci_14_val, cci_14_action=0,0
  cci_14_val_missing, cci_14_action_missing=1,1
  
  atr_14_val, atr_14_action=0,0
  atr_14_val_missing, atr_14_action_missing=1,1
  
  highs_lows_14_val, highs_lows_14_action=0,0
  highs_lows_14_val_missing, highs_lows_14_action_missing=1,1
  
  ultimate_oscillator_val, ultimate_oscillator_action=0,0
  ultimate_oscillator_val_missing, ultimate_oscillator_action_missing=1,1
  
  roc_val, roc_action=0,0
  roc_val_missing, roc_action_missing=1,1
  
  bull_bear_power_13_val, bull_bear_power_13_action=0,0
  bull_bear_power_13_val_missing, bull_bear_power_13_action_missing=1,1

  gmt_mo, gmt_dd, gmt_yy, gmt_hh, gmt_mm = 0,0,0,0,0
  gmt_mo_missing, gmt_dd_missing, gmt_yy_missing, gmt_hh_missing, gmt_mm_missing = 1,1,1,1,1

  tech_ind_list = tech_ind_table.split('\n')
  if len(tech_ind_list)!=46:
    print(f'[{pair_name}] Unexpected tech_ind_table text length: {tech_ind_list}! expected 46. data must be corrupted or source layout might have been changed.'
           'If you see this message more than once, try adding logic to handle unexpected data format.\n')
    return (rsi_14_val, rsi_14_action, rsi_14_val_missing, rsi_14_action_missing,
       stoch_9_6_val, stoch_9_6_action, stoch_9_6_val_missing, stoch_9_6_action_missing,
       stochrsi_14_val, stochrsi_14_action, stochrsi_14_val_missing, stochrsi_14_action_missing,
       macd_12_26_val, macd_12_26_action, macd_12_26_val_missing, macd_12_26_action_missing,
       adx_14_val, adx_14_action, adx_14_val_missing, adx_14_action_missing,
       williams_per_r_val, williams_per_r_action, williams_per_r_val_missing, williams_per_r_action_missing,
       cci_14_val, cci_14_action, cci_14_val_missing, cci_14_action_missing,
       atr_14_val, atr_14_action, atr_14_val_missing, atr_14_action_missing,
       highs_lows_14_val, highs_lows_14_action, highs_lows_14_val_missing, highs_lows_14_action_missing,
       ultimate_oscillator_val, ultimate_oscillator_action, ultimate_oscillator_val_missing, ultimate_oscillator_action_missing,
       roc_val, roc_action, roc_val_missing, roc_action_missing,
       bull_bear_power_13_val, bull_bear_power_13_action, bull_bear_power_13_val_missing, bull_bear_power_13_action_missing,)
    

  ret = []

  try:
    time_gmt_text = tech_ind_list[0].replace('Technical Indicators', '').replace(',', '').replace(':', ' ').replace('PM', '').replace('GMT', '')
    gmt_mo, gmt_dd, gmt_yy, gmt_hh, gmt_mm = time_gmt_text.split()
    gmt_mo_missing, gmt_dd_missing, gmt_yy_missing, gmt_hh_missing, gmt_mm_missing = 0,0,0,0,0

  except:
    print(f'[{pair_name}] unexpected time_gmt_text: {time_gmt_text}. skipping....')
    gmt_mo, gmt_dd, gmt_yy, gmt_hh, gmt_mm = [0]*5
    gmt_mo_missing, gmt_dd_missing, gmt_yy_missing, gmt_hh_missing, gmt_mm_missing = [1]*5

  ret.append(gmt_mo)
  ret.append(gmt_dd)
  ret.append(gmt_yy)
  ret.append(gmt_hh)
  ret.append(gmt_mm)
  ret.append(gmt_mo_missing)
  ret.append(gmt_dd_missing)
  ret.append(gmt_yy_missing)
  ret.append(gmt_hh_missing)
  ret.append(gmt_mm_missing)
  
  lidx = 4
  hidx = lidx+3

  while hidx < len(tech_ind_list)-3:
    data = tech_ind_list[lidx:hidx]
    ind_name = data[0].lower().replace(')', '').replace('(', '_',).replace(',', '_').replace(' ', '_').replace('%', 'per_').replace('/', '_')
    ind_value = data[1]
    ind_action = data[2]
    # print(ind_name, ind_value, ind_action)
    # rsi_14 43.931 Sell
    # stoch_9_6 43.798 Sell
    # stochrsi_14 13.926 Oversold
    # macd_12_26 0.000 Neutral
    # adx_14 33.120 Sell
    # williams_per_r -76.745 Sell
    # cci_14 -121.4158 Sell
    # atr_14 0.0008 High Volatility
    # highs_lows_14 -0.0001 Sell
    # ultimate_oscillator 53.917 Buy
    # roc -0.066 Sell
    # bull_bear_power_13 -0.0003 Sell

    try:
      float(ind_value)
      ret.append(ind_value)
      ret.append(ind_action)
      ret.append(0)
      ret.append(0)
    except:
      print(f'[{pair_name}] ind_name: {ind_value} unexpected. could not convert to float. skipping ...')
      ret.append(0)
      ret.append(0)
      ret.append(1)
      ret.append(1)


    lidx = hidx
    hidx = hidx+3

  return ret

def clean_csv(fpath, num_pairs=10):
  df = pd.read_csv(str(fpath))
  arr = df.values

  nrow, ncol = arr.shape
  arr = arr.reshape(-1, 10, ncol)
  print(f'reshaped ({nrow}, {ncol}) ---> {arr.shape}')

  n_scrapes, n_pairs, n_cols = arr.shape
  
  # all pairs' information for a given point of time
  all_pair_info = dict()
  
  for idx_scrape in range(n_scrapes):
    for data_pair in arr[idx_scrape]:
      pair_name = data_pair[0]
      primary_data = data_pair[1]
      scraped_time = data_pair[2]
      time_write_local = data_pair[3]
      secondary_data = data_pair[4]
      tech_summary_main = data_pair[6]
      pivot_points_table = data_pair[7]
      tech_ind_table = data_pair[8]


      cur_close, cur_per1, cur_per2, cur_close_missing, cur_per1_missing, cur_per2_missing = _clean_primary_data(primary_data, pair_name)
      scrape_hr, scrape_min, scrape_sec, scrape_hr_missing, scrape_min_missing, scrape_sec_missing = _clean_scraped_time(scraped_time, pair_name)
      write_hr, write_min, write_sec, write_hr_missing, write_min_missing, write_sec_missing = _clean_time_write_local(time_write_local, pair_name)

      (prev_close, bid, ask, day_max, day_min, prev_close_missing, bid_missing, ask_missing, 
          day_max_missing, day_min_missing) = _clean_secondary_data(secondary_data, pair_name)

      (main_summary_id, ma_summary_id, ma_tot_sell, ma_tot_buy, tech_ind_summary_id, tech_ind_tot_sell, tech_ind_tot_buy,
            main_summary_id_missing, ma_summary_id_missing, ma_tot_sell_missing, 
            ma_tot_buy_missing, tech_ind_summary_id_missing, tech_ind_tot_sell_missing, 
            tech_ind_tot_buy_missing) = _clean_tech_summary_main(tech_summary_main, pair_name)

      (pivot_classic_s3, pivot_classic_s2, pivot_classic_s1, pivot_classic_points, pivot_classic_r1, pivot_classic_r2, pivot_classic_r3,
       pivot_classic_s3_missing, pivot_classic_s2_missing, pivot_classic_s1_missing, pivot_classic_points_missing, pivot_classic_r1_missing, pivot_classic_r2_missing, pivot_classic_r3_missing,
       pivot_fibonacci_s3, pivot_fibonacci_s2, pivot_fibonacci_s1, pivot_fibonacci_points, pivot_fibonacci_r1, pivot_fibonacci_r2, pivot_fibonacci_r3,
       pivot_fibonacci_s3_missing, pivot_fibonacci_s2_missing, pivot_fibonacci_s1_missing, pivot_fibonacci_points_missing, pivot_fibonacci_r1_missing, pivot_fibonacci_r2_missing, pivot_fibonacci_r3_missing,
       pivot_camarilla_s3, pivot_camarilla_s2, pivot_camarilla_s1, pivot_camarilla_points, pivot_camarilla_r1, pivot_camarilla_r2, pivot_camarilla_r3,
       pivot_camarilla_s3_missing, pivot_camarilla_s2_missing, pivot_camarilla_s1_missing, pivot_camarilla_points_missing, pivot_camarilla_r1_missing, pivot_camarilla_r2_missing, pivot_camarilla_r3_missing,       
       pivot_woodies_s3, pivot_woodies_s2, pivot_woodies_s1, pivot_woodies_points, pivot_woodies_r1, pivot_woodies_r2, pivot_woodies_r3,
       pivot_woodies_s3_missing, pivot_woodies_s2_missing, pivot_woodies_s1_missing, pivot_woodies_points_missing, pivot_woodies_r1_missing, pivot_woodies_r2_missing, pivot_woodies_r3_missing,
       pivot_demarks_s3, pivot_demarks_s2, pivot_demarks_s1, pivot_demarks_points, pivot_demarks_r1, pivot_demarks_r2, pivot_demarks_r3,
       pivot_demarks_s3_missing, pivot_demarks_s2_missing, pivot_demarks_s1_missing, pivot_demarks_points_missing, pivot_demarks_r1_missing, pivot_demarks_r2_missing, pivot_demarks_r3_missing,
      ) =  _clean_pivot_points_table(pivot_points_table, pair_name)


      (gmt_mo, gmt_dd, gmt_yy, gmt_hh, gmt_mm, gmt_mo_missing, gmt_dd_missing, gmt_yy_missing, gmt_hh_missing, gmt_mm_missing,
       rsi_14_val, rsi_14_action, rsi_14_val_missing, rsi_14_action_missing,
       stoch_9_6_val, stoch_9_6_action, stoch_9_6_val_missing, stoch_9_6_action_missing,
       stochrsi_14_val, stochrsi_14_action, stochrsi_14_val_missing, stochrsi_14_action_missing,
       macd_12_26_val, macd_12_26_action, macd_12_26_val_missing, macd_12_26_action_missing,
       adx_14_val, adx_14_action, adx_14_val_missing, adx_14_action_missing,
       williams_per_r_val, williams_per_r_action, williams_per_r_val_missing, williams_per_r_action_missing,
       cci_14_val, cci_14_action, cci_14_val_missing, cci_14_action_missing,
       atr_14_val, atr_14_action, atr_14_val_missing, atr_14_action_missing,
       highs_lows_14_val, highs_lows_14_action, highs_lows_14_val_missing, highs_lows_14_action_missing,
       ultimate_oscillator_val, ultimate_oscillator_action, ultimate_oscillator_val_missing, ultimate_oscillator_action_missing,
       roc_val, roc_action, roc_val_missing, roc_action_missing,
       bull_bear_power_13_val, bull_bear_power_13_action, bull_bear_power_13_val_missing, bull_bear_power_13_action_missing,) =  _clean_tech_ind_table(tech_ind_table, pair_name)

      fts = [cur_close, cur_per1, cur_per2, cur_close_missing, cur_per1_missing, cur_per2_missing,
             scrape_hr, scrape_min, scrape_sec, scrape_hr_missing, scrape_min_missing, scrape_sec_missing,
             write_hr, write_min, write_sec, write_hr_missing, write_min_missing, write_sec_missing,
             prev_close, bid, ask, day_max, day_min, prev_close_missing, bid_missing, ask_missing, day_max_missing, day_min_missing, 
             main_summary_id, ma_summary_id, ma_tot_sell, ma_tot_buy, tech_ind_summary_id, tech_ind_tot_sell, tech_ind_tot_buy, main_summary_id_missing, ma_summary_id_missing, ma_tot_sell_missing, ma_tot_buy_missing, tech_ind_summary_id_missing, tech_ind_tot_sell_missing,
             
             pivot_classic_s3, pivot_classic_s2, pivot_classic_s1, pivot_classic_points, pivot_classic_r1, pivot_classic_r2, pivot_classic_r3,
             pivot_classic_s3_missing, pivot_classic_s2_missing, pivot_classic_s1_missing, pivot_classic_points_missing, pivot_classic_r1_missing, pivot_classic_r2_missing, pivot_classic_r3_missing,
             pivot_fibonacci_s3, pivot_fibonacci_s2, pivot_fibonacci_s1, pivot_fibonacci_points, pivot_fibonacci_r1, pivot_fibonacci_r2, pivot_fibonacci_r3,
             pivot_fibonacci_s3_missing, pivot_fibonacci_s2_missing, pivot_fibonacci_s1_missing, pivot_fibonacci_points_missing, pivot_fibonacci_r1_missing, pivot_fibonacci_r2_missing, pivot_fibonacci_r3_missing,
             pivot_camarilla_s3, pivot_camarilla_s2, pivot_camarilla_s1, pivot_camarilla_points, pivot_camarilla_r1, pivot_camarilla_r2, pivot_camarilla_r3,
             pivot_camarilla_s3_missing, pivot_camarilla_s2_missing, pivot_camarilla_s1_missing, pivot_camarilla_points_missing, pivot_camarilla_r1_missing, pivot_camarilla_r2_missing, pivot_camarilla_r3_missing,       
             pivot_woodies_s3, pivot_woodies_s2, pivot_woodies_s1, pivot_woodies_points, pivot_woodies_r1, pivot_woodies_r2, pivot_woodies_r3,
             pivot_woodies_s3_missing, pivot_woodies_s2_missing, pivot_woodies_s1_missing, pivot_woodies_points_missing, pivot_woodies_r1_missing, pivot_woodies_r2_missing, pivot_woodies_r3_missing,
             pivot_demarks_s3, pivot_demarks_s2, pivot_demarks_s1, pivot_demarks_points, pivot_demarks_r1, pivot_demarks_r2, pivot_demarks_r3,
             pivot_demarks_s3_missing, pivot_demarks_s2_missing, pivot_demarks_s1_missing, pivot_demarks_points_missing, pivot_demarks_r1_missing, pivot_demarks_r2_missing, pivot_demarks_r3_missing,
             
             gmt_mo, gmt_dd, gmt_yy, gmt_hh, gmt_mm, gmt_mo_missing, gmt_dd_missing, gmt_yy_missing, gmt_hh_missing, gmt_mm_missing,
             rsi_14_val, rsi_14_action, rsi_14_val_missing, rsi_14_action_missing,
             stoch_9_6_val, stoch_9_6_action, stoch_9_6_val_missing, stoch_9_6_action_missing,
             stochrsi_14_val, stochrsi_14_action, stochrsi_14_val_missing, stochrsi_14_action_missing,
             macd_12_26_val, macd_12_26_action, macd_12_26_val_missing, macd_12_26_action_missing,
             adx_14_val, adx_14_action, adx_14_val_missing, adx_14_action_missing,
             williams_per_r_val, williams_per_r_action, williams_per_r_val_missing, williams_per_r_action_missing,
             cci_14_val, cci_14_action, cci_14_val_missing, cci_14_action_missing,
             atr_14_val, atr_14_action, atr_14_val_missing, atr_14_action_missing,
             highs_lows_14_val, highs_lows_14_action, highs_lows_14_val_missing, highs_lows_14_action_missing,
             ultimate_oscillator_val, ultimate_oscillator_action, ultimate_oscillator_val_missing, ultimate_oscillator_action_missing,
             roc_val, roc_action, roc_val_missing, roc_action_missing,
             bull_bear_power_13_val, bull_bear_power_13_action, bull_bear_power_13_val_missing, bull_bear_power_13_action_missing,]
      
      ft_names = ['cur_close', 'cur_per1', 'cur_per2', 'cur_close_missing', 'cur_per1_missing', 'cur_per2_missing',
                  'scrape_hr', 'scrape_min', 'scrape_sec', 'scrape_hr_missing', 'scrape_min_missing', 'scrape_sec_missing',
                  'write_hr', 'write_min', 'write_sec', 'write_hr_missing', 'write_min_missing', 'write_sec_missing',
                  'prev_close', 'bid', 'ask', 'day_max', 'day_min', 'prev_close_missing', 'bid_missing', 'ask_missing', 'day_max_missing', 'day_min_missing',
                  'main_summary_id', 'ma_summary_id', 'ma_tot_sell', 'ma_tot_buy', 'tech_ind_summary_id', 'tech_ind_tot_sell', 'tech_ind_tot_buy', 'main_summary_id_missing', 'ma_summary_id_missing', 'ma_tot_sell_missing', 'ma_tot_buy_missing', 'tech_ind_summary_id_missing', 'tech_ind_tot_sell_missing',
                  
                  'pivot_classic_s3', 'pivot_classic_s2', 'pivot_classic_s1', 'pivot_classic_points', 'pivot_classic_r1', 'pivot_classic_r2', 'pivot_classic_r3',
                  'pivot_classic_s3_missing', 'pivot_classic_s2_missing', 'pivot_classic_s1_missing', 'pivot_classic_points_missing', 'pivot_classic_r1_missing', 'pivot_classic_r2_missing', 'pivot_classic_r3_missing',
                  'pivot_fibonacci_s3', 'pivot_fibonacci_s2', 'pivot_fibonacci_s1', 'pivot_fibonacci_points', 'pivot_fibonacci_r1', 'pivot_fibonacci_r2', 'pivot_fibonacci_r3',
                  'pivot_fibonacci_s3_missing', 'pivot_fibonacci_s2_missing', 'pivot_fibonacci_s1_missing', 'pivot_fibonacci_points_missing', 'pivot_fibonacci_r1_missing', 'pivot_fibonacci_r2_missing', 'pivot_fibonacci_r3_missing',
                  'pivot_camarilla_s3', 'pivot_camarilla_s2', 'pivot_camarilla_s1', 'pivot_camarilla_points', 'pivot_camarilla_r1', 'pivot_camarilla_r2', 'pivot_camarilla_r3',
                  'pivot_camarilla_s3_missing', 'pivot_camarilla_s2_missing', 'pivot_camarilla_s1_missing', 'pivot_camarilla_points_missing', 'pivot_camarilla_r1_missing', 'pivot_camarilla_r2_missing', 'pivot_camarilla_r3_missing',       
                  'pivot_woodies_s3', 'pivot_woodies_s2', 'pivot_woodies_s1', 'pivot_woodies_points', 'pivot_woodies_r1', 'pivot_woodies_r2', 'pivot_woodies_r3',
                  'pivot_woodies_s3_missing', 'pivot_woodies_s2_missing', 'pivot_woodies_s1_missing', 'pivot_woodies_points_missing', 'pivot_woodies_r1_missing', 'pivot_woodies_r2_missing', 'pivot_woodies_r3_missing',
                  'pivot_demarks_s3', 'pivot_demarks_s2', 'pivot_demarks_s1', 'pivot_demarks_points', 'pivot_demarks_r1', 'pivot_demarks_r2', 'pivot_demarks_r3',
                  'pivot_demarks_s3_missing', 'pivot_demarks_s2_missing', 'pivot_demarks_s1_missing', 'pivot_demarks_points_missing', 'pivot_demarks_r1_missing', 'pivot_demarks_r2_missing', 'pivot_demarks_r3_missing',
                  
                  'gmt_mo', 'gmt_dd', 'gmt_yy', 'gmt_hh', 'gmt_mm', 'gmt_mo_missing', 'gmt_dd_missing', 'gmt_yy_missing', 'gmt_hh_missing', 'gmt_mm_missing',
                  'rsi_14_val', 'rsi_14_action', 'rsi_14_val_missing', 'rsi_14_action_missing',
                  'stoch_9_6_val', 'stoch_9_6_action', 'stoch_9_6_val_missing', 'stoch_9_6_action_missing',
                  'stochrsi_14_val', 'stochrsi_14_action', 'stochrsi_14_val_missing', 'stochrsi_14_action_missing',
                  'macd_12_26_val', 'macd_12_26_action', 'macd_12_26_val_missing', 'macd_12_26_action_missing',
                  'adx_14_val', 'adx_14_action', 'adx_14_val_missing', 'adx_14_action_missing',
                  'williams_per_r_val', 'williams_per_r_action', 'williams_per_r_val_missing', 'williams_per_r_action_missing',
                  'cci_14_val', 'cci_14_action', 'cci_14_val_missing', 'cci_14_action_missing',
                  'atr_14_val', 'atr_14_action', 'atr_14_val_missing', 'atr_14_action_missing',
                  'highs_lows_14_val', 'highs_lows_14_action', 'highs_lows_14_val_missing', 'highs_lows_14_action_missing',
                  'ultimate_oscillator_val', 'ultimate_oscillator_action', 'ultimate_oscillator_val_missing', 'ultimate_oscillator_action_missing',
                  'roc_val', 'roc_action', 'roc_val_missing', 'roc_action_missing',
                  'bull_bear_power_13_val', 'bull_bear_power_13_action', 'bull_bear_power_13_val_missing', 'bull_bear_power_13_action_missing',]
                  
      
      for ft, ft_name in zip(fts, ft_names):
        k = pair_name+'_'+ft_name
        if k not in all_pair_info:
          all_pair_info[k] = [ft]
        else:
          all_pair_info[k].append(ft)
    
  return pd.DataFrame(all_pair_info)


def main():

    TELEGRAM_FILES_DIR = sys.argv[1]
    CSV_SAVE_PATH = sys.argv[2]

    TELEGRAM_FILES_DIR = Path(TELEGRAM_FILES_DIR)
    scraped_files = sorted([f for f in TELEGRAM_FILES_DIR.iterdir() if f.is_file()])

    df_clean_all = []
    for fpath in scraped_files:

        print("#############################################################################")
        print("processing:", fpath)
        print("##############################################################################")
        df_clean = clean_csv(fpath)
        df_clean_all.append(df_clean)


    minute_data = pd.concat(df_clean_all)
    print('shape:', minute_data.shape)
    print('first ten columns:')
    print(minute_data.head(10))


    # other than the numerical variable, there are categorical variables
    # for every currency pair
    minute_data.to_csv(CSV_SAVE_PATH, index=False)


if __name__ == '__main__':
    main()