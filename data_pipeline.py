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
import shapefile
from shapely.geometry import Point # Point class
from shapely.geometry import shape
import rapidjson
import random
import zipfile
print('Building POI data')

places_df = pd.read_csv('./data/us_places/2020/07/core_poi-part1.csv.gz', usecols=['safegraph_place_id', 'latitude', 'longitude', 'top_category'])
for i in range(2, 6):
    print('Reading places DF: {}'.format(i))
    places_df = pd.concat([places_df, pd.read_csv('./data/us_places/2020/07/core_poi-part{}.csv.gz'.format(i), usecols=['safegraph_place_id', 'top_category'])])

place_categories = [p for p in set(list(places_df['top_category'])) if p is not np.nan]
place_to_category = dict(zip(places_df['safegraph_place_id'], places_df['top_category']))

print(len(place_to_category))

def unzip_us_places():
    for fp, dirs, files in os.walk('./data/us_places'):
        for f in files:
            if f[-4:] == '.zip':
                zf = zipfile.ZipFile(os.path.join(fp, f))
                zf.extractall(fp)


def unzip_all():
    # This iterates through the whole file tree unzipping files
    paths = ['./data/poi/weekly_patterns/', './data/social-distancing/']
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


# Utils for building the poi data

def parse_week_old_format(f): #takes filepath,
    global place_to_category
    global place_categories
    global places_df
    date_to_stats = {}
    week = f[:10] # week is START day
    temp_out = './data/processed_data/poi'
    outfile_name = os.path.join(temp_out, week + '_poi.csv')
    if os.path.exists(outfile_name):
        return None
    else:
        print(f)
        fp = os.path.join('./data/poi/weekly_patterns/main-file/', f)
        weekly_df = pd.read_csv(fp, usecols=['safegraph_place_id', 'visits_by_day', 'median_dwell', 'poi_cbg'], dtype={'poi_cbg' : str})
        print('Weekly df loaded')
        start_date = dt.strptime(week, '%Y-%m-%d')
        norm_df = pd.read_csv('./data/poi/weekly_patterns/normalization-stats/{}-normalization-stats.csv'.format(week))
        norm_df['sort_index'] = norm_df['year']*400 + norm_df['month']*40 + norm_df['day']
        norm_df = norm_df.sort_values('sort_index')
        week_dates = []
        for weekday in range(7):
            date = start_date + timedelta(days=weekday)
            date = date.strftime('%Y-%m-%d')
            week_dates += [date]
            date_to_stats[date] = {
                'county_category_data' : {},
            }
            
        for i in range(weekly_df.shape[0]):
            if (i%100000) == 0:
                print('{}: {}/{}'.format(week, i, weekly_df.shape[0]))
            row = weekly_df.iloc[i]
            place_id = row['safegraph_place_id']
            visits_by_day = eval(row['visits_by_day'])
            cbg = row['poi_cbg']
            if type(cbg) == str:
                county_id = cbg[:5]
                if place_id in place_to_category:
                    category = place_to_category[place_id]
                    if county_id and type(category) == str:
                        if county_id not in date_to_stats[week_dates[weekday]]['county_category_data']:
                            for weekday in range(7):
                                date_to_stats[week_dates[weekday]]['county_category_data'][county_id] = {c : [] for c in place_categories}
                        for weekday in range(7):
                            elt = {
                                'count' : visits_by_day[weekday],
                                'median_dwell' : row['median_dwell']
                            }

                            date_to_stats[week_dates[weekday]]['county_category_data'][county_id][category] += [elt]
                            date_to_stats[week_dates[weekday]]['total_devices_seen'] = norm_df['total_devices_seen'].iloc[weekday]
        out_rows = []
        for d in date_to_stats:
            for cid in date_to_stats[d]['county_category_data']:
                row = {
                    'date' : d,
                    'county_id' : cid,
                    'normalizaton_count': date_to_stats[d]['total_devices_seen']
                } 
                for category in place_categories:
                    if category in date_to_stats[d]['county_category_data'][cid]:
                        day_data = date_to_stats[d]['county_category_data'][cid][category]
                        row[category + '_count_sum'] = sum([elt['count'] for elt in day_data])
                        row[category + '_median_dwell_weighted_avg'] = sum(elt['count'] * elt['median_dwell'] for elt in day_data)/max(1, row[category + '_count_sum'])
                    else:
                        row[category + '_count_sum'] = 0
                        row[category + '_median_dwell_weighted_avg'] = 0
                out_rows += [row]
        out_df = pd.DataFrame(out_rows)
        out_df.to_csv(outfile_name, index=False)            
        return None


def parse_week_new_format(fp): #takes path to file dir, e.g. './data/weekly-patterns-1/patterns/2020/07/08' filepath indicates END date, not START date
    global place_to_category
    global place_categories
    global places_df
    date_to_stats = {}
    week = '-'.join(fp[-13:-3].split('/')) 
    temp_out = './data/processed_data/poi'
    outfile_name = os.path.join(temp_out, week + '_poi.csv')
    print('ofname')
    print(outfile_name)
    if os.path.exists(outfile_name):
        return None 
    else:
        norm_df = pd.read_csv(os.path.join(fp, 'normalization_stats.csv').replace('/patterns', '/normalization_stats'))
        norm_df['sort_index'] = norm_df['year']*400 + norm_df['month']*40 + norm_df['day']
        norm_df = norm_df.sort_values('sort_index')
        start_date = dt(year=norm_df.iloc[0]['year'], month=norm_df.iloc[0]['month'], day=norm_df.iloc[0]['day'])
        print('Starting {}'.format(start_date.strftime('%Y-%m-%d')))
        
        dirfiles = [f for f in os.listdir(fp) if f[-7:] == '.csv.gz']
        weekly_df = pd.read_csv(os.path.join(fp, dirfiles[0]), usecols=['safegraph_place_id', 'visits_by_day', 'median_dwell', 'poi_cbg'], dtype={'poi_cbg' : str})
        for f in dirfiles[1:]:
            weekly_df = pd.concat([weekly_df, pd.read_csv(os.path.join(fp, f), usecols=['safegraph_place_id', 'visits_by_day', 'median_dwell', 'poi_cbg'], dtype={'poi_cbg' : str})], axis=0)
        
        week_dates = []
        for weekday in range(7):
            date = start_date + timedelta(days=weekday)
            date = date.strftime('%Y-%m-%d')
            week_dates += [date]
            date_to_stats[date] = {
                'county_category_data' : {},
            }
        for i in range(weekly_df.shape[0]):
            if (i%100000) == 0:
                print('{}: {}/{}'.format(week, i, weekly_df.shape[0]))
            row = weekly_df.iloc[i]
            place_id = row['safegraph_place_id']
            visits_by_day = eval(row['visits_by_day'])
            cbg = row['poi_cbg']
            if type(cbg) == str:
                county_id = cbg[:5]
                if place_id in place_to_category:
                    category = place_to_category[place_id]
                    if county_id and type(category) == str:
                        if county_id not in date_to_stats[week_dates[weekday]]['county_category_data']:
                            for weekday in range(7):
                                date_to_stats[week_dates[weekday]]['county_category_data'][county_id] = {c : [] for c in place_categories}
                        for weekday in range(7):
                            elt = {
                                'count' : visits_by_day[weekday],
                                'median_dwell' : row['median_dwell']
                            }

                            date_to_stats[week_dates[weekday]]['county_category_data'][county_id][category] += [elt]
                            date_to_stats[week_dates[weekday]]['total_devices_seen'] = norm_df['total_devices_seen'].iloc[weekday]

        out_rows = []
        for d in date_to_stats:
            for cid in date_to_stats[d]['county_category_data']:
                row = {
                    'date' : d,
                    'county_id' : cid,
                    'normalizaton_count': date_to_stats[d]['total_devices_seen']
                } 
                for category in place_categories:
                    if category in date_to_stats[d]['county_category_data'][cid]:
                        day_data = date_to_stats[d]['county_category_data'][cid][category]
                        row[category + '_count_sum'] = sum([elt['count'] for elt in day_data])
                        row[category + '_median_dwell_weighted_avg'] = sum(elt['count'] * elt['median_dwell'] for elt in day_data)/max(1, row[category + '_count_sum'])
                    else:
                        row[category + '_count_sum'] = 0
                        row[category + '_median_dwell_weighted_avg'] = 0
                out_rows += [row]
        out_df = pd.DataFrame(out_rows)
        out_df.to_csv(outfile_name, index=False)            
        return None







if __name__ == '__main__':



    # Refactored data pipeline


    unzip_all()
    unzip_us_places()

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

    def process_social(file): 
        # Create the output file; one for every day
        global start_dtype
        output_folder = './data/processed_data/social_distancing'
        cbgs_out = os.path.join(output_folder, file.split('/')[-1])

        # don't run if the file's already processed
        if os.path.exists(cbgs_out):
            print('Done: ' + file)
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

    pool = Pool(60)



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
    in_file_path = './data/processed_data/social_distancing/{}-social-distancing.csv'
    out_file_path = './data/processed_data/assembly/temp.csv'

    out_dtype['county_id'] = str
    
    cases_df = pd.read_csv('./data/county_data/county_timeseries.csv', dtype=out_dtype)
    cases_df['join_key'] = cases_df.apply(lambda d:  str(d['county_id']) +  d['date'], axis=1)

    days = [day for day in pd.date_range(start=start_date, end=end_date)]

    for i, day in enumerate(days):
        
        print(day)
        file_in = in_file_path.format(day.strftime('%Y-%m-%d'))
        try:
            social_df = dd.read_csv(file_in, error_bad_lines=False, dtype=out_dtype)
        except:
            break
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
            df2.to_csv(out_file_path,mode='w', index=False)#, single_file = True)
        else:
            df2.to_csv(out_file_path, mode='a', header=False, index=False)#, single_file = True)



    rootdir_old = './data/poi/weekly_patterns/main-file'
    rootdir_new = './data/poi/weekly_patterns_v2/patterns'
    temp_out = './data/processed_data/poi'

    weeks = []
    for f in os.listdir(rootdir_old):
        if f[:4] == '2020' and f[-4:] == '.csv':
            weeks += [f]

    print('Initializing pool')
    print('Parsing old POI data')
    complete_counter = 0
    output = {}
    o_to_write = []
    for res in pool.map(parse_week_old_format, weeks):
        complete_counter += 1
        print('{}/{} files complete'.format(complete_counter, len(weeks)))
        

    dirpaths = []
    for fp, dirs, files in os.walk(rootdir_new):
        if len([fn for fn in files if 'patterns-part' in fn and fn[-7:] == '.csv.gz']) > 0: 
            dirpaths += [fp]
    print('Parsing new POI data')
    print(dirpaths)
    complete_counter = 0
    for res in pool.map(parse_week_new_format, dirpaths):
        complete_counter += 1
        print('{}/{} files complete'.format(complete_counter, len(dirpaths)))

    print('Assembling POI dataframe')
    is_first = True
    for fp, dirs, files in os.walk('./data/processed_data/poi'): 
        for f in files:
            print(f)
            if is_first:
                poi_df = pd.read_csv(os.path.join(fp, f), dtype={'county_id' : str})
                is_first = False
            else:
                poi_df2 = pd.read_csv(os.path.join(fp, f), dtype={'county_id' : str})
                poi_df = pd.concat([poi_df, poi_df2], axis=0)

    dtype_dict = out_dtype
    dtype_dict['county_id'] = str
    county_time_series = pd.read_csv('./data/processed_data/assembly/temp.csv', dtype=dtype_dict)

    county_time_series = county_time_series.set_index(['county_id', 'date'])

    poi_df = poi_df.set_index(['county_id', 'date'])
    print('Joining poi and county_time_series')

    output_df = county_time_series.join(poi_df, on=['county_id', 'date'], how='outer')
    output_df = output_df.reset_index()
    print('Writing final.csv')
    output_df = output_df.drop(columns=['Unnamed: 0', 'date_start_x', 'date_start_y'])
    output_df = output_df.dropna(how='any') 
    output_df.to_csv('./data/output_data/final.csv', index=False)

    