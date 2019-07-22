from utilsbigdata.statistic_utils import useful_functs as uf
import pandas as pd

import requests
import ast
import math
import time
import numpy as np
import glob
import os

def read_tt_data(tt_dir):

    cols_tt = ['name','time','historictime','updatetime']

    types = {
        'name': 'category',
        'time': 'int',
        'historictime': 'int',
        'updatetime': 'str'
    }

    df_tt = pd.read_csv(tt_dir, sep = ';', usecols = cols_tt, dtype = types, encoding = 'latin-1')
    
    initial_length = len(df_tt.index)    
    print('The original number of rows is: ' + str(initial_length))
    return df_tt

def read_r_data(r_dir):    

    cols_r = ['name','start_date','line','length']
    df_r = pd.read_csv(r_dir, sep = ';', usecols = cols_r, encoding = 'latin-1')

    return df_r

def read_dict(github_user, github_token):
    url = 'https://raw.githubusercontent.com/bigdata-sectra/documents-hub/master/waze/dicc-tramos-waze.csv'
    dict_tramos_r = requests.get(url , auth=(github_user, github_token))
    dict_tramos_r.encoding = 'latin-1'
    dict_tramos_df = pd.read_csv(pd.compat.StringIO(dict_tramos_r.text), sep=';')
    return dict_tramos_df

def read_meteorological_data(meteo_dir):
    water_cols = ['momento','rrInst']
    temperature_cols = ['momento','ts']

    water_dir = meteo_dir / 'agua'
    water_files = glob.glob(os.path.join(water_dir, "*.csv"))
    water_df = pd.concat((pd.read_csv(f, sep=";", usecols = water_cols) for f in water_files))

    temperature_dir = meteo_dir / 'temperatura'
    temperature_files = glob.glob(os.path.join(temperature_dir, "*.csv"))
    temperature_df = pd.concat((pd.read_csv(f, sep=";", usecols = temperature_cols) for f in temperature_files))

    return water_df, temperature_df

def process_meteorological_data(water_df, temp_df, freq):
    water_df['momento'] = uf.lookup(water_df['momento'])
    water_df['date'] = water_df['momento'].dt.date
    water_df['floor_hour'] = water_df['momento'].dt.floor(freq).dt.time
    grouped_water_df = water_df.groupby(by = ['date','floor_hour']).agg({'rrInst':sum}).reset_index()
    
    temperature_df['momento'] = uf.lookup(temperature_df['momento'])
    temperature_df['date'] = temperature_df['momento'].dt.date    
    temperature_df['floor_hour'] = temperature_df['momento'].dt.floor(freq).dt.time
    grouped_temperature_df = temperature_df.groupby(by = ['date','floor_hour']).agg({'ts':mean}).reset_index()

    return [grouped_water_df, grouped_temperature_df]

def filter_by_project(df_tt,dict_tramos_df,project):
    initial_length = len(df_tt.index)
    routes = dict_tramos_df.loc[(dict_tramos_df['project'] == project)&(dict_tramos_df['deprecated'] != 1),'name'].to_list()
    df_tt = df_tt[df_tt['name'].isin(routes)]
    final_length = len(df_tt.index)
    print('The number of deleted rows when filtering by project is: ' + str(initial_length - final_length))
    print('The current number of rows is: ' + str(final_length))
    return df_tt

def drop_duplicates(df_tt):
    initial_length = len(df_tt.index)
    df_tt.drop_duplicates(subset=['name','time','updatetime'], keep='first', inplace=True)
    df_tt.drop_duplicates(subset=['name','updatetime'], keep=False, inplace=True)
    final_length = len(df_tt.index)
    print('The number of deleted duplicated rows is: ' + str(initial_length - final_length))
    print('The current number of rows is: ' + str(final_length))    
    return df_tt

def parse_and_process_dates(df_tt, freq):
    df_tt['updatetime'] = df_tt['updatetime'].astype(str).str[:-3] #Getting rid of tz info.
    df_tt['updatetime'] = uf.lookup(df_tt['updatetime'])

#    df_tt['floor_update_time'] = df_tt['updatetime'].dt.floor(freq)
    df_tt['floor_hour'] = df_tt['updatetime'].dt.floor(freq).dt.time

    df_tt['daytype'] = df_tt['updatetime'].apply(uf.day_type)
    df_tt['weekday'] = df_tt['updatetime'].dt.weekday
    df_tt['date'] = df_tt['updatetime'].dt.date
    df_tt['hour_of_day'] = df_tt['updatetime'].dt.time

    return df_tt

def delete_no_flow_periods(df_tt,dict_df):
    initial_length = len(df_tt.index)
    df_tt = df_tt.merge(dict_df[['name','main_street','sense','no_flow_periods']], on = ['name'], how = 'left')
    df_tt['no_flow_boolean'] = df_tt.apply(lambda row: uf.boolean_from_no_flow(row['hour_of_day'], row['daytype'], row['no_flow_periods']), axis=1)
    df_tt['time'] = np.where(df_tt['no_flow_boolean'], df_tt['time'], np.nan)    
    df_tt = df_tt.loc[pd.notnull(df_tt['time']),:]
    final_length = len(df_tt.index)
    print('The number of deleted rows with no-flow is: ' + str(initial_length - final_length))
    print('The final number of rows is: ' + str(final_length))
    return df_tt

def compute_delay_velocity(df_tt, df_r):    
    df_tt = df_tt.merge(df_r[['name', 'length']], on = 'name', how = 'left')
    df_tt['[s/km]'] = (df_tt['time'] / (df_tt['length']))*1000
    df_tt['[km/h]'] = (df_tt['length'] / (df_tt['time']))*3.6

    return df_tt

def flag_with_iqr(df_tt, iqr_distance, agg_type):
    grouped_df_tt = df_tt.groupby(['name', agg_type, 'floor_hour']).agg({'[s/km]':['count', uf.q1, uf.q3]})
    grouped_df_tt.columns = ['_'.join(col).strip() for col in grouped_df_tt.columns.values]
    grouped_df_tt.reset_index(inplace=True)
    grouped_df_tt['iqr'] = grouped_df_tt['[s/km]_q3'] - grouped_df_tt['[s/km]_q1']
    grouped_df_tt.rename(columns={'[s/km]_count':'group_size_outlier'},inplace=True)

    df_tt = df_tt.merge(grouped_df_tt, on = ['name', agg_type, 'floor_hour'], how='left')

    df_tt['outlier_U'] = (df_tt['[s/km]'] > df_tt['[s/km]_q3'] + iqr_distance*df_tt['iqr'])
    df_tt['outlier_L'] = (df_tt['[s/km]'] < df_tt['[s/km]_q1'] - iqr_distance*df_tt['iqr'])
    df_tt['outlier_iqr'] = (df_tt['outlier_U'] | df_tt['outlier_L']).astype(int)

    return df_tt

def flag_with_mad_z(df_tt, agg_type):    
    grouped_df_tt = df_tt.groupby(['name', agg_type, 'floor_hour']).agg({'[s/km]':[np.median,
                                                                                    uf.mean_absolute_deviation_y,
                                                                                    uf.median_absolute_deviation_y]})
    grouped_df_tt.columns = ['_'.join(col).strip() for col in grouped_df_tt.columns.values]
    grouped_df_tt.reset_index(inplace=True)
    df_tt = df_tt.merge(grouped_df_tt, on = ['name', agg_type, 'floor_hour'], how='left')

    df_tt['outlier_z_score'] = df_tt.apply(lambda row: uf.outliers_modified_z_score(row['[s/km]'],
                                                                                row['[s/km]_median'],
                                                                                row['[s/km]_mean_absolute_deviation_y'],
                                                                                row['[s/km]_median_absolute_deviation_y']), axis=1)
    return df_tt
