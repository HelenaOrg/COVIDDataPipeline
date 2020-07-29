import pandas as pd
from datetime import datetime as dt, timedelta
import dask.dataframe as dd
from dask.multiprocessing import get
# import censusgeocode as cg
import timeit
import boto3
import os
import gzip
import shutil
from itertools import groupby, chain
from collections import Counter
from ast import literal_eval
import time
from tqdm import tqdm
import re
import ciso8601
from multiprocessing import Pool

def post_processing(data_file,
                    file_out, # where to write the data
                    start_date=dt.strptime("01-22-2020", "%m-%d-%Y"),
                    shift_label_by_days=7, # The number of days latency from predicted var
                    growth_rate_days=[1], # the number of days counted in growth (ie, day-over-day -> 1, week-over-week -> 7)
                    interpolate_method=['barycentric'], # nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘spline’, 
                                                      # ‘barycentric’, ‘polynomial’, ‘krogh’, ‘piecewise_polynomial’, ‘spline’, 
                                                      # ‘pchip’, ‘akima’:
                    exp_weighted_mean=True, # whether to take the weighted mean
                    exp_mean_val=5, # value corresponding to span of EWM
                    census='./data/groomed_data/county_census.csv' ,
                    degree = None# leaving this commented out for now because it's not implemented './data/groomed_data/degree/in_out_degree.csv' 
                   ):
    
    
    data = pd.read_csv(data_file,  dtype={'county_id':str}, parse_dates=['date'])
    data['date'] = pd.to_datetime(data['date'])
    
    # rename columns
    data.columns = ['date', 'county', 'state', 'county_id', 'Confirmed', 'Deaths']

    # Kill bad data
    data = data[data['county_id'].isna()==False]

    # Robust load of county_id
    data['county_id'] = data['county_id'].apply(lambda s: str(int(s)).rjust(5, '0'))

    # Why is there even duplicate data I did inner joins
    data = data.drop_duplicates()

    # Pandas DateTime index
    data = data.reset_index(drop=True)

    def add_dataset(df, file_name, dtype, join_key, parse_dates):
        df2 = pd.read_csv(file_name, dtype=dtype, parse_dates=parse_dates)
        return df.merge(df2, how='left', on=join_key, )
    
    if census:
        data = add_dataset(data, census, dtype={'county_id':str}, join_key='county_id')

    if degree:
        degree_df = pd.read_csv(degree, dtype={'county_id':str, }, parse_dates=['date'])
        degree_df['date'] = pd.to_datetime(degree_df['date'])
        degree_df['county_id'] = degree_df.county_id.astype(str)
        data['county_id'] = data.county_id.astype(str)
        degree_df = degree_df.groupby(['date', 'county_id']).first().reset_index()
        data = data.merge(degree_df, how='left', on=['county_id', 'date'])

        
    start_list_df = []
    counties = data.groupby('county_id').first().iterrows()
    
    ## Iterate through county cases and set the first historical date
    county_cases = data.set_index('date')
    for county in counties:
        initialized_vals= {'date': pd.to_datetime(start_date),
                     'date_start': pd.to_datetime(start_date),
                     'date_end': pd.to_datetime(start_date),
                     'county_id': county[0],
                     'Deaths': 0.00001,
                     'Confirmed': 0.00001,
                     'county': county[1]['county'],
                     'state': county[1]['state']
                    }
        empty_row = { **initialized_vals,
                      **{
                          col: np.nan for col in set(county_cases.columns) - set(initialized_vals.keys())
                      }
        }
        start_list_df.append(empty_row)       

    # 
    county_df = pd.DataFrame(start_list_df)
#     county_df['date'] = county_df['date'].apply(pd.to_datetime, meta=pd.datetime)
    county_cases= county_df.append(data)
#     print(county_cases.head())
    county_cases['date'] = pd.to_datetime(county_cases['date']) #.dt.date


    def linear_growth(val, val_offset):
        return (val - val_offset + 0.01) / (val_offset + 0.01)
    
    ### Calculate the 1 day change in raw case data
    for i in growth_rate_days:
        county_cases = calculate_growth_rate(county_cases, ['Confirmed', 'Deaths'], i)

        
    ### Fill data and interpolate
    multiple_interpolate_dfs = []
    interpolate_cols = ['Confirmed', 'Deaths']
    for i in interpolate_method:
        county_cases = interpolator(county_cases, i, interpolate_cols)

    
    ###########################
    ### MISSING: data
    ###########################
    
    def ewm(ts, emv=exp_mean_val):
        return ts.sort_index(ascending=False).ewm(emv).mean().drop(['county_id'], axis=1)
    
    def exp_weighted_mean(df, columns, exp_mean_val):
        if df.index.name:
            df = df.reset_index() # get rid of the index if there is one
        new_columns = [c + '_' + 'ewm' for c in columns]
        cases_grouped = df.set_index('date')[columns + ['county_id']].groupby('county_id') 
        print( cases_grouped.apply(lambda x: ewm(x, exp_mean_val)))
        df[new_columns] = cases_grouped.apply(lambda x: ewm(x, exp_mean_val)).reset_index(drop=True)
        
        return df
    
    
    
    ### Interpolate data
    for i in growth_rate_days:
        county_cases = calculate_growth_rate(county_cases, ['Confirmed', 'Deaths'], i)

    ### Difference (Linear Growth Rate) Interpolated Data
    columns = [c for c in county_cases.columns if 'Interpolate' in c]
    for g in growth_rate_days:
        county_cases = calculate_growth_rate(county_cases, columns, g)
    
    ### Exponential Moving Average on Interpolated data
    if exp_weighted_mean:
        columns = [c for c in county_cases.columns if 'Interpolate' in c]
        county_cases = exp_weighted_mean(county_cases, columns, exp_mean_val)
        
        ### Calculate Linear Growth Rate on Smoothed Data
        columns = [c for c in county_cases.columns if 'Interpolate' in c]
        for g in growth_rate_days:
            county_cases = calculate_growth_rate(county_cases, columns, g)

    ## Forward shift the label so that we're predicting n days out
    ## must be last step
    for county in counties:
        county_cases[county_cases['county_id']==county]['Confirmed'] = \
                      county_cases[county_cases['county_id']==county]['Confirmed'].shift(shift_label_by_days)

    county_cases.to_csv(file_out)






### Loads in social distance data into a dd file 
def load_social_distance_all(index=None):
    path = './data/social-distancing/'
#     print(list(os.walk(path)))
    file_list = []
    for root, directory, files in os.walk(path):
        print(files)
        if len(files)==2:
            for file in files:
                if file[-3:] == 'csv':
                    file_list.append(root+'/'+file)

    return file_list

def unzip_gz(filepath, output=None):
    # This unzips all .gz files downloaded from S3 and replaces them with csv's
    if filepath[-2:] == 'gz':
        print(filepath)
        with gzip.open(filepath, 'rb') as f_in:
            if output: 
                path = output + filepath.split('/')[-1]
                path = path[:-3]
            else:
                path = filepath[:-3]
            with open(path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
def unzip_all():
    # This iterates through the whole file tree unzipping files
    paths = ['./data/weekly_patterns/', './data/us_places/', './data/social-distancing/']
    for path in paths:
        for root, directory, files in os.walk(path):
            if len(files)==1:
                print(root+'/'+files[0], root+'/')
                unzip_gz(root+'/'+files[0], root+'/')
            else:
                for file in files:
                    if file[-3:]=='.gz' and file[:-3] not in files:
                        print(root+'/'+file, root+'/')
                        unzip_gz(root+'/'+file, root+'/')




###################################
# UTILS
###################################

def cbgs_to_county_fips(cbgs):
    return str(cbgs).rjust(12, '0')[:5]

def split_cbgs_tuple(r):
    print(r)
    r['destination'] = r['destination_cbgs'][0]
    r['device'] = r['destination_cbgs'][1]
    return r

# Subfunction for dict_flatten
def accumulate(l):
    it = itertools.groupby(l, operator.itemgetter(0))
    for key, subiter in it:
        yield key, sum(item[1] for item in subiter) 

# Subfunction for dict_flatten
def accumulate(l):
    it = groupby(l, operator.itemgetter(0)) #iter groupby
    agg_sum = [k for k in [(key, sum(item[1] for item in subiter)) for key, subiter in it]] #iter agg
    agg_sum.sort()
    return pd.Series(agg_sum)


# takes a series of cbgs dict strings and groups and sums them
def cbgs_dict_flatten(x, n):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals (# devices)
    l = [(i[:5], j) for i, j in zip(keys, vals)] # create a list of lists, convert cbgs to fips
    z = list(itertools.islice(accumulate(l), n)) # groupby, sum
    return list(sum(z, ())) # flatten the list

# takes a series of cbgs dict strings and groups and sums them
def cbgs_dict_flatten2(x, n=None):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals (# devices)

    l = [(i[:5], j) for i, j in zip(keys, vals)] # create a list of lists, convert cbgs to fips
    
    if n:
        z = accumulate(l)[:n] # groupby, sum
        return pd.Series(sum(z, ())) # flatten the list
    else:
        return accumulate(l)

###################################
# UTILS
###################################

def cbgs_to_county_fips(cbgs):
    return str(cbgs).rjust(12, '0')[:5]

def split_cbgs_tuple(r):
    print(r)
    r['destination'] = r['destination_cbgs'][0]
    r['device'] = r['destination_cbgs'][1]
    return r

# Subfunction for dict_flatten
import operator
def accumulate(l):
    it = groupby(l, operator.itemgetter(0)) #iter groupby
    agg_sum = [k for k in [(key, sum(item[1] for item in subiter)) for key, subiter in it]] #iter agg
    agg_sum.sort()
    return pd.Series(agg_sum)


# takes a series of cbgs dict strings and groups and sums them
def cbgs_dict_flatten(x, n):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals (# devices)
    l = [(i[:5], j) for i, j in zip(keys, vals)] # create a list of lists, convert cbgs to fips
    z = list(itertools.islice(accumulate(l), n)) # groupby, sum
    return list(sum(z, ())) # flatten the list

# takes a series of cbgs dict strings and groups and sums them
def cbgs_dict_flatten2(x, n=None):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals (# devices)
#     print(keys)
#     print(vals)
    l = [(i[:5], j) for i, j in zip(keys, vals)] # create a list of lists, convert cbgs to fips
    
    if n:
        z = accumulate(l)[:n] # groupby, sum
        return pd.Series(sum(z, ())) # flatten the list
    else:
        return accumulate(l)


def reverse_date(date):
    dates = date.split('-')
    return '-'.join([dates[2],dates[0],  dates[1]])


# Takes a set of generic fixed-length of n dict strings and sums them and retturns a dict
def dict_flatten(x, n):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals

    l = [(i, j) for i, j in zip(keys, vals)] # create a list of lists, convert cbgs to fips
    z = list(itertools.islice(accumulate(l), n)) # groupby, sum
    return dict(z)   

# Takes a set of generic fixed-length of n dict strings and sums them and returns an ordered list 
def dict_flatten2(x, cols, names=None):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals
    c = len(cols)
    k = len(keys)
    l = [sum([vals[j] if cols[i] == keys[j] else 0 for j in range(k)]) for i in range(c)]
    if names:
        return pd.Series(l).reindex(names, fill_value=0)
    else:
        return pd.Series(l)
          
# Takes a set of generic fixed-length of n dict strings and sums them and returns an ordered list 
def dict_flatten3(x, cols, names=None):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals
    data = {}
    for key, val in zip(keys, vals): 
        data[key] = data.get(key, 0) + val
    return pd.Series(data)

def reverse_date(date):
    dates = date.split('-')
    return '-'.join([dates[2],dates[0],  dates[1]])


# ###################################
# # Process Social Distancing
# ###################################
#     print(cbgs_out)


def process_social(file): 
   # Create the output file; one for every day
    cbgs_out = output_folder + file.split('/')[-1]
    
    if os.path.exists(cbgs_out):
        return None
    
    
    print('loading: ' + file) 
    # Load in Dask DF
#     dtype={'distance_traveled_from_home': 'float64', }
    social_df = dd.read_csv(file, error_bad_lines=False, dtype=start_dtype)
#     social_df = dd.from_pandas(pd.read_csv(file, nrows=10), npartitions=1)
#     social_df = social_df.fillna(method='ffill')

    # Create date and origin_fips cols
    social_df['date_start'] = social_df['date_range_start'].map(lambda x: x[:10]) 
    social_df['date_end'] = social_df['date_range_end'].map(lambda x: x[:10]) 
    
    social_df['origin_fips'] = social_df['origin_census_block_group'].apply(lambda x: cbgs_to_county_fips(x), 
                                                                    meta=('origin_census_block_group', str)) 

    # Groupby and Sum
    agg_dict = {x: ['min', 'max', 'sum', 'prod', 'mean', 'std'] for x in agg_cols}
    bucket_agg = dd.Aggregation('join', 
                                 lambda x: x.agg(''.join),
                                lambda x0: x0.agg(''.join),
                               )
    bucket_agg2 = dd.Aggregation('new', 
                                 lambda x: x.agg( lambda x: dict_flatten2(x, num_fips)),
                                lambda x0: x0.agg(''.join),
                               )
    
    bucket_dict = {x: bucket_agg for x in bucket_cols + home_cols + destination_cols}
    
    for col in bucket_cols:
        social_df[col] = social_df[col].astype(str)
        
    social_df = social_df[groupby_cols + bucket_cols + agg_cols + home_cols + destination_cols] \
            .groupby(groupby_cols) \
            .agg(dict(agg_dict, **bucket_dict)) # most efficient way to add two dicts
#     print(social_df.compute())
    
    # Kill the dreaded MultiIndex
    social_df.columns = ['_'.join(col).strip() for col in social_df.columns.values]
    social_df = social_df.reset_index()
    
    
    
    # Redo MetaData
    for col in bucket_cols:
#         print(col)
        social_df[col + '_join'] = social_df[col + '_join'].astype(str)

    for bucket in bucket_cols:
        cols = bucket_col_dict[bucket]
        raw_cols = raw_bucket_col_dict[bucket]
        col = bucket + '_join'
        social_df[cols] = social_df.map_partitions(lambda x: x[col].apply(lambda z: dict_flatten3(z, raw_cols, cols)))
        social_df = social_df.drop(col, axis=1)
        
    social_df = social_df.compute()

   
    social_df[destination_fips_cols] = social_df['destination_cbgs_join'].apply(lambda z: cbgs_dict_flatten2(z, num_fips))
    print(social_df)
    print('uploading 10000000 lines of data')
    social_df.to_csv(cbgs_out #,single_file = True
#           chunksize=chunksize
    )    


#postprocess data and make final.csv

    
##############################
##.  UTILS
##############################

def interpolator(mini_df, interpolate_method, columns):
    if mini_df.index.name:
        mini_df = mini_df.reset_index()
    
    def run_interpolator(df, i):
        hi = df.apply(lambda y: interpolator_inner(y, i), axis=0).drop(['county_id'], axis=1)
        return hi
    
    def interpolator_inner(col, i):
        return col.resample('D').interpolate(i).ffill()
    
    
    interpolate = mini_df[columns + ['date', 'county_id']].set_index('date').groupby('county_id')
    interpolate = interpolate.apply(lambda x: run_interpolator(x, interpolate_method))
    
    interpolate = interpolate.rename(columns = {col: col + '_Interpolate_' + str(interpolate_method) 
                                for col in columns})
    
    mini_df = mini_df.merge(interpolate, on=['county_id','date']).reset_index()
    return mini_df

## V1 of a calculating growth rate. Only works on 1 day windows
def growth_rate(x_t, i):
    (t, county) = i
    try:
        new_time = dt.strftime(t-timedelta(days=1), "%Y-%m-%d")
        x_t_1 = float(county_cases.loc[new_time, county]['Confirmed'])
        return (x_t_1 - float(x_t)+1)/ (x_t_1 + 1)
    except:
        return 0
    
### Calculate Linear Growth Rate on a datafrome with named columns
def calculate_growth_rate(df, columns, days): # make sure to pass df with no index

    ### Calculate Linear Growth Rate for a series
    def linear_growth(val, val_offset):
        return (val - val_offset + 0.001) / (val_offset + 0.001)
 
    df2 = df.set_index('date').shift(days, freq='D') # make the offset column
    
    offset_columns = [c + "_offset" for c in columns]
    df2 = df2.rename(columns = {c: c + "_offset" for c in columns}) #rename cols for join
    
    
    df2 = df2.reset_index().set_index(['county_id', 'date'])
    df = df.reset_index().set_index(['county_id', 'date'])

    print(df2)
    print(df)
    
    ## join table with the offset columns
    df = pd.concat([df,df2[offset_columns]], axis=1)
    
    ## Calculate growth rate for each column
    for col in columns:
        df[col + '_growth' + str(days)] = linear_growth(df[col], df[col + '_offset'])
        df = df.drop([col + "_offset"], axis = 1) 
        
    df = df.reset_index()
    df = column_cleanup(df)

    return df

## Gets rid of nasty columns from resetting index
def column_cleanup(df):
    for i in ['index', 'level_0', 'Unnamed: 0', 'Unnamed: 0.1']:
        if i in df.columns:
            df = df.drop(i, axis=1)
    return df


##########################
# data preprocessing
######### AKA Purgatory #




###########################
### Data Transforms
###########################

###########################
# Functions live above here
###########################

# Start by creading the county timeseries dataset
in_file = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
out_file = './data/groomed_data/county_timeseries.csv'
print('Creating county time series')
post_processing(in_file, out_file, census=False, shift_label_by_days=-7)



###################################
# CONSTANTS
###################################

start_cols = ['origin_census_block_group', 'date_range_start', 'date_range_end', 
              'device_count', 'distance_traveled_from_home', 'bucketed_distance_traveled', 
              'median_dwell_at_bucketed_distance_traveled', 'completely_home_device_count', 
              'median_home_dwell_time', 'bucketed_home_dwell_time', 'at_home_by_each_hour', 
              'part_time_work_behavior_devices', 'full_time_work_behavior_devices', 
              'destination_cbgs', 'delivery_behavior_devices', 'median_non_home_dwell_time', 
              'candidate_device_count', 'bucketed_away_from_home_time', 
              'median_percentage_time_home', 'bucketed_percentage_time_home']



### Preload some things
unzip_all()
social = load_social_distance_all()
output_folder = './data/social_distancing_processed/'

### Number of FIPS cols to keep
### Create columns for all them FIPS buckets
num_fips = 19
destination_fips_cols = [j for i in range(num_fips) for j in['destination_fips' + str(i), 'devices' + str(i)]] 


### Define aggregation columns
agg_cols = [
       'device_count', 'distance_traveled_from_home',
       'completely_home_device_count', 'median_home_dwell_time',
       'part_time_work_behavior_devices', 'full_time_work_behavior_devices',
       'delivery_behavior_devices',
       'median_non_home_dwell_time', 'candidate_device_count',
       'median_percentage_time_home',
]
bucket_cols = [
        'bucketed_distance_traveled', 'bucketed_home_dwell_time', 
        'median_dwell_at_bucketed_distance_traveled',
        'bucketed_away_from_home_time', 
        'bucketed_percentage_time_home'
]
home_cols = ['at_home_by_each_hour']
destination_cols = ['destination_cbgs']
groupby_cols = ['origin_fips', 'date_start', 'date_end']



text_cols = ['median_dwell_at_bucketed_distance_traveled_join',
'bucketed_away_from_home_time_join',
'bucketed_percentage_time_home_join',
'at_home_by_each_hour_join',
'destination_cbgs_join',
'bucketed_distance_traveled_join', 
         'bucketed_home_dwell_time_join']

### Define the column names the bucket columns will be split into
file = pd.read_csv(social[0], error_bad_lines=False, nrows=2)

def create_columns(col_prefix, x):
    return [col_prefix + '_' + cat for cat in re.findall(r'[\"\[\(]([0-9-\>\<]+?)\"', x)]

def create_raw_columns(x):
    return [cat for cat in re.findall(r'[\"\[\(]([0-9-\>\<]+?)\"', x)]

bucket_col_dict = {}
raw_bucket_col_dict = {}
for bucket in bucket_cols:
    col_prefix = bucket.replace('bucketed_', '')
    bucket_col_dict[bucket] = file[bucket].apply(lambda x: create_columns(col_prefix, x))[0]
    bucket_col_dict[bucket].sort()
    raw_bucket_col_dict[bucket] = file[bucket].apply(lambda x: create_raw_columns(x))[0]
    raw_bucket_col_dict[bucket].sort()

agg_dict = {x: ['min', 'max', 'sum', 'prod', 'mean', 'std'] for x in agg_cols}
bucket_dict = {x: lambda y: ''.join(str(y)) for x in bucket_cols + home_cols + destination_cols}

start_dtype = dict([(col, float) for col in agg_cols] + 
                   [(col, str) for col in bucket_cols + home_cols + destination_cols])

out_dtype = dict([(col, float) for col in agg_cols] + 
                   [(col, str) for col in bucket_cols + home_cols + destination_cols] +
                 [('devices' + str(n), 'float64') for n in range(num_fips)] +
                 [('destination_fips' + str(n), 'float64') for n in range(num_fips)]
                )



import numpy as np
start_date = dt.strptime("01-22-2020", "%m-%d-%Y")
end_date = dt.today().strftime('%Y-%m-%d')

text_cols = ['median_dwell_at_bucketed_distance_traveled_join',
 'bucketed_away_from_home_time_join',
 'bucketed_percentage_time_home_join',
 'at_home_by_each_hour_join',
 'destination_cbgs_join',
  'bucketed_distance_traveled_join', 
             'bucketed_home_dwell_time_join']

dtype={'origin_fips': 'str', 'date_start': 'str'}
in_url = './data/social_distancing_processed/{}-social-distancing.csv'
out_url = './data/groomed_data/final.csv'

cases_df = pd.read_csv('./data/groomed_data/county_timeseries.csv', dtype=out_dtype)
cases_df['county_id'] = cases_df['county_id'].apply(str)
cases_df = cases_df.drop('Unnamed: 0', axis=1)
# cases_df['rev_date'] = cases_df['date'].apply(reverse_date)
cases_df['join_key'] = cases_df.apply(lambda d:  str(d['county_id']) +  d['date'], axis=1)
# print(cases_df)

days = [day for day in pd.date_range(start=start_date, end=end_date)]

for i, day in enumerate(days):
    
    print(day)
    file_in = in_url.format(day.strftime('%Y-%m-%d'))
    if day.strftime('%Y-%m-%d') in ["2020-02-02", "2020-02-03"]:
        print('hi')
        continue
    file_out = out_url

    
    social_df = dd.read_csv(file_in, error_bad_lines=False, dtype=out_dtype)

    new_cols = [s for s in social_df.columns if s not in text_cols]
    social_df = social_df[new_cols]
#    print(social_df.head())
    
    social_df = social_df.map_partitions( lambda df: df.assign(join_key=df.origin_fips.astype(str) + df.date_start.astype(str)))

    df2 = social_df.merge(cases_df, how='inner', left_on='join_key', right_on='join_key') #do the merge
    df2 = df2.drop('join_key', axis=1)
#     df2 = df2.drop('Unnamed: 0', axis=1)
#     df2 = df2.drop('FIPS', axis=1)
#     df2 = df2.drop('origin_fips', axis=1)
    df2.reset_index(drop=True)
    df2 = df2.compute()
#     print(df2.columns)
    for col in df2.columns.to_list():
        if 'fips' in col:
            df2 = df2.fillna(0)
            df2[col] = df2[col].astype(int)
            df2[col] = df2[col].apply(lambda x: str(x).rjust(5, '0'))
#     print(df2.head())
    if i==0:
        df2.to_csv(file_out,mode='w')#, single_file = True)
    else:
        df2.to_csv(file_out, mode='a', header=False)#, single_file = True)
    
