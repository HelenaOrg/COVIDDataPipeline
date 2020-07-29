import pandas as pd
import numpy as np
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
import operator

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

def cbgs_to_county_fips(cbgs):
    return str(cbgs).rjust(12, '0')[:5]

def split_cbgs_tuple(r):
    print(r)
    r['destination'] = r['destination_cbgs'][0]
    r['device'] = r['destination_cbgs'][1]
    return r

def dict_flatten3(x, cols, names=None):
    keys = re.findall(r'\"(.+?)\"',x) # collect keys (cbgs) from string
    vals = [int(i) for i in re.findall(r'\:([0-9]+?)[\,\}]', x)] # collect vals
    data = {}
    for key, val in zip(keys, vals): 
        data[key] = data.get(key, 0) + val
    return pd.Series(data)


def load_social_distance_all(index=None):
    path = './data/social-distancing/'
    file_list = []
    for root, directory, files in os.walk(path):
        for file in files:
            if file[-3:] == 'csv':
                file_list.append(root+'/'+file)

    return file_list

def process_social(file): 
    # Create the output file; one for every day
    output_folder = './data/processed_data'
    cbgs_out = os.path.join(output_folder, file.split('/')[-1])

    # don't run if the file's already processed
    if os.path.exists(cbgs_out):
        return None
    
    
    print('loading: ' + file) 
    social_df = dd.read_csv(file, error_bad_lines=False, dtype=start_dtype)

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

    
    # Kill the dreaded MultiIndex
    social_df.columns = ['_'.join(col).strip() for col in social_df.columns.values]
    social_df = social_df.reset_index()
    
    
    
    # Redo MetaData
    for col in bucket_cols:
        social_df[col + '_join'] = social_df[col + '_join'].astype(str)

    for bucket in bucket_cols:
        cols = bucket_col_dict[bucket]
        raw_cols = raw_bucket_col_dict[bucket]
        col = bucket + '_join'
        social_df[cols] = social_df.map_partitions(lambda x: x[col].apply(lambda z: dict_flatten3(z, raw_cols, cols)))
        social_df = social_df.drop(col, axis=1)
        
    social_df = social_df.compute()


    social_df[destination_fips_cols] = social_df['destination_cbgs_join'].apply(lambda z: cbgs_dict_flatten2(z, num_fips))
    print('uploading 10000000 lines of data')
    social_df.to_csv(cbgs_out
                    #,single_file = True
                    #chunksize=chunksize
    )

                


def create_county_timeseries_dataset(data_file_path,
                                    out_file_path, 
                                    degree_file_path=None,
                                    interpolate_methods=['barycentric']
                                    ):

    data = pd.read_csv(data_file_path,  dtype={'county_id':str}, parse_dates=['date'])
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

    # if we have a path for a degree file, import and merge it
    if degree_file_path:
        degree_df = pd.read_csv(degree, dtype={'county_id':str, }, parse_dates=['date'])
        degree_df['date'] = pd.to_datetime(degree_df['date'])
        degree_df['county_id'] = degree_df.county_id.astype(str)
        data['county_id'] = data.county_id.astype(str)
        degree_df = degree_df.groupby(['date', 'county_id']).first().reset_index()
        data = data.merge(degree_df, how='left', on=['county_id', 'date'])

    # build our output dataframe
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

    county_df = pd.DataFrame(start_list_df)
    county_cases= county_df.append(data)
    county_cases['date'] = pd.to_datetime(county_cases['date']) #.dt.date
    
    # right now, not forward filling cases or deaths, only 5 counties have day-level discontinuities in their reporting
    # these discontinuities ought to be handled at the dataset level
    county_cases.to_csv(out_file_path, index=False)

def create_columns(col_prefix, x):
    return [col_prefix + '_' + cat for cat in re.findall(r'[\"\[\(]([0-9-\>\<]+?)\"', x)]

def create_raw_columns(x):
    return [cat for cat in re.findall(r'[\"\[\(]([0-9-\>\<]+?)\"', x)]


# Refactored data pipeline

unzip_all()

start_date = dt.strptime("01-22-2020", "%m-%d-%Y")
end_date = dt.today().strftime('%Y-%m-%d') 

# Create county timeseries dataset
in_file = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
out_file = './data/county_data/county_timeseries.csv'
print('Creating county time series')
county_ts = create_county_timeseries_dataset(in_file, out_file)

# Load social distancing data
print('Building social distancing data')

social = load_social_distance_all()

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
pool = Pool(14)
pool.map(process_social, social)


print('Combining social distancing data with county timeseries data')

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
in_file_path = './data/processed_data/{}-social-distancing.csv'
out_file_path = './data/output_data/final.csv'

out_dtype['county_id'] = str
cases_df = pd.read_csv('./data/county_data/county_timeseries.csv', dtype=out_dtype)
cases_df = cases_df.drop('Unnamed: 0', axis=1)
cases_df['join_key'] = cases_df.apply(lambda d:  str(d['county_id']) +  d['date'], axis=1)

days = [day for day in pd.date_range(start=start_date, end=end_date)]

for i, day in enumerate(days):
    
    print(day)
    file_in = in_file_path.format(day.strftime('%Y-%m-%d'))
    if day.strftime('%Y-%m-%d') in ["2020-02-02", "2020-02-03"]:
        print('hi')
        continue

    
    social_df = dd.read_csv(file_in, error_bad_lines=False, dtype=out_dtype)

    new_cols = [s for s in social_df.columns if s not in text_cols]
    social_df = social_df[new_cols]
    
    social_df = social_df.map_partitions( lambda df: df.assign(join_key=df.origin_fips.astype(str) + df.date_start.astype(str)))

    df2 = social_df.merge(cases_df, how='inner', left_on='join_key', right_on='join_key') #do the merge
    df2 = df2.drop('join_key', axis=1)

    df2.reset_index(drop=True)
    df2 = df2.compute()

    for col in df2.columns.to_list():
        if 'fips' in col:
            df2 = df2.fillna(0)
            df2[col] = df2[col].astype(int)
            df2[col] = df2[col].apply(lambda x: str(x).rjust(5, '0'))
    if i==0:
        df2.to_csv(out_file_path,mode='w')#, single_file = True)
    else:
        df2.to_csv(out_file_path, mode='a', header=False)#, single_file = True)
    

