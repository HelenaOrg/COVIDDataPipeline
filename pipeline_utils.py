####################################
### Imports
####################################

import pandas as pd
import logging as lo
# import tqdm
import json
# import censusgeocode as cg
from datetime import datetime as dt, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from constants import *
from zipfile import ZipFile, ZipInfo
import time
import functools
from io import BytesIO, StringIO
import requests
import dask.dataframe as dd
import numpy as np
import math
from scipy.spatial import cKDTree
from scipy import inf
from scipy.stats import percentileofscore
import censusdata
import re

####################################
### Decorators
####################################

def get_runtime(func):
    ## standard timer wrapper
    @functools.wraps(func)
    def timer(*args, **kwargs):
        start_time = time.time()
        v = func(*args, **kwargs)
        print(f"{func.__name__} elapsed runtime: {(time.time() - start_time)}")
        return v
    return timer


def county_pipeline(func):
    ## handles common county df functions, appends new data to county df
    @functools.wraps(func)
    def county_logic(*args, **kwargs):
        print("########################################################")
        print(f"Running function {func.__name__} in the county pipeline")
        print("########################################################")
        return func(*args, **kwargs).set_index('county_id')
        #return pd.concat([county_df, df], axis=1)
    return county_logic



verbose=True
def df_logging(verbose=True):
    ## returns verbose description of dataframe
    def inner(func):
        @functools.wraps(func)
        def return_info(*args, **kwargs):
            df = func(*args, **kwargs)
            if verbose:
                print("##### RETURNING FUNCTION STATISTICS #####")
                print(f"### Function {func.__name__} preview:\n {df.head(5)}")
                print(f"### Function {func.__name__} stats:\n {df.describe()}")
                print(f"### Function {func.__name__} datatypes:\n {df.dtypes}")
            return df
        return return_info
    return inner

####################################
### General Utils
####################################

def merger(df, df2):
    df = df.loc[~df.index.duplicated(keep='first')]
    df2 = df2.loc[~df2.index.duplicated(keep='first')]
    return df.join(df2, how='outer')

def write_files(prefix, df, index=None):
    if not index:
        df = df.reset_index(drop=True)
    if index:
        df = df.reset_index().rename(columns={'index': index})
        df[index] = df[index].str.rjust(5, '0')
    df.to_csv(prefix + '.csv')
    with open(prefix + '.json', 'w') as f:
        f.write(json.dumps(json.loads(df.to_json(orient='records'))))
    return df

def state_name_to_abbr(statename):
    states_df = pd.read_csv(city_zips)[['state_id', 'state_name']].drop_duplicates()
    states_dict = states_df.set_index('state_name')['state_id'].to_dict()
    return states_dict[statename]
 
def cbgs_to_county_fips(cbgs):
    return str(cbgs).rjust(12, '0')[:5]

def latlong_to_cbgs(lat, long):
    try:
        coords = cg.coordinates(y=lat, x=long)
        coords = coords['Census Tracts']['GEOID']
        return coords
    except:
        print("Unable to map latlong to an existing point in US geography")
        return None

def latlong_to_county_fips(lat, long):
    try:
        coords = cg.coordinates(y=lat, x=long)
        coords = coords['Census Tracts']['GEOID']
        return cbgs_to_county_fips(coords)
    except:
        print("Unable to map latlong to an existing point in US geography")
        return None
    
def zips_to_fips(zip):
    zips_df = pd.read_csv(city_zips)[['zip', 'county_id']].drop_duplicates()
    zips_dict = zips_df.set_index('zip')['county_id'].to_dict()
    return zips_dict[zip]
    
    
####################################
### Post-processing
####################################

@get_runtime
def validations(df): 
    print(df.describe())
    print(df.isna().count())
    plt.figure(figsize=(15, 10))
    sns.heatmap(df.corr(), annot=True)

@get_runtime
def build_percentiles(columns, df):
    if columns:
        for col in columns:
            df[col + '_pct'] = df[col].rank(pct=True)
    else:
        for col in df.columns:
            df[col + '_pct'] = df[col].rank(pct=True)
    return df

@get_runtime
def write_to_geojson(df, file, county=True, fips='FIPS', county_fips=None):
    lo.info("Creating GEOJSON file from dataframe and writing to %f", file)
    
    ## Read in County geojson file
    with open(county_fips, 'r') as f:
        lo.info("Loading in GeoJSON file from ./geojson_fips.json")
        geojson = json.loads(f.read())

    ## Flip county geojson file 
    inverted_counties = {}
    for county in tqdm(geojson['features']):
        inverted_counties[county['id']] = county
    
    ## Add Properties to county geojson from df
    features = []
    for i, row in tqdm(df.iterrows()):
        print(row, fips, row[fips])
        try:
            inverted_counties[row[fips]]['properties'].update(row.to_dict())
            features.append(inverted_counties[row[fips]])
            
        except KeyError as e:
            print("Could not find key %k in geojson", row[fips])

    geojson = {"type": "FeatureCollection", "features": features}
    
    with open(file, 'w') as f:
        f.write(json.dumps(geojson))
    return geojson

  
####################################
### County Operations
####################################

@df_logging(verbose)
@get_runtime
def load_county(df):
    ## returns all counties and general info
    counties = pd.read_csv(df, encoding='latin-1')
    counties['county_id'] = counties['STATE'].map(lambda x: str(x).rjust(2, '0')) +\
                        counties['COUNTY'].map(lambda x: str(x).rjust(3, '0'))

    ## Old and new column names
    print(counties.columns)
    columns = ['county_id', 'STNAME', 'CTYNAME', 'POPESTIMATE2018']
    columns_new = ['county_id', 'state', 'county', 'population']
    counties = counties[columns]
    counties.columns = columns_new
    
    return counties.set_index('county_id')

@df_logging(verbose)
@get_runtime
@county_pipeline
def load_county_poverty(df):
    ## extracts poverty numbers
    poverty = pd.read_excel(df, skiprows=4)
    poverty['county_id'] = poverty['FIPStxt'].map(lambda x: str(x).rjust(5, '0'))
    poverty['poverty'] = poverty['PCTPOVALL_2018'] / 100.0
    poverty = poverty[['county_id', 'poverty']]
    return poverty


@df_logging(verbose)
@get_runtime
@county_pipeline
def load_county_cms(df):
    ### return HCC score and total costs
    columns = ['Average HCC Score', 'county_id'] #, 'Total Actual Costs'
    columns_new = ['hcc_score',  'county_id'] #'total_costs',
    # cms = pd.read_excel(df, header=1)
    cms = pd.read_csv(df)
    cms = cms[cms['State and County FIPS Code'].notnull()]
    cms['county_id'] = cms['State and County FIPS Code'].map(lambda x: str(int(x)).rjust(5, '0'))
    cms = cms[columns]
    cms.columns = columns_new
    #cms['total_costs'] = cms['total_costs'].map(lambda x: 0 if x=="*" else float(x))
    cms['hcc_score'] = cms['hcc_score'].map(lambda x: 0 if x=="*" else float(x))
    return cms

@df_logging(False)
@get_runtime
def clean_county_cases(cases, day, date=False):
    ## This is a subfunction used by get_all_county_cases to load in data
#     print("Loading in all current county COVID case data from %d", cases)

    columns = ['county_id', 'Confirmed', 'Deaths', 'Recovered', 'Active']   
    columns_new = ['county_id', 'cases', 'deaths', 'recovered', 'active'] 
    try:
        print(day)
        county_cases = pd.read_csv(cases + day + '.csv')

    except:
        print('skipping date: ' + day)
        day_str = (dt.today() - timedelta(days=2)).strftime('%m-%d-%Y')
        county_cases = pd.read_csv(cases + day_str + '.csv')


    ## ignore the datasets without county level data
    if 'FIPS' not in county_cases.columns:
        return None

    ## return only US cases, then reformat FIPS codes
    county_cases = county_cases[county_cases['Country_Region']=='US']
    county_cases = county_cases[county_cases['FIPS'].notnull()]
    county_cases['county_id'] = county_cases['FIPS'].map(lambda x: str(int(x)).rjust(5, '0'))
#     county_cases = county_cases[columns]
#     county_cases.columns = columns_new

    if date:
        county_cases['date']=day
    return county_cases

@df_logging(False)
@get_runtime
def load_hist_county_cases(cases, snapshot=True): 
    ## Write all historical county cases to file
    print("Loading in all historical county COVID case data from {}{}".format(cases, (dt.today() - timedelta(days=1)).strftime('%m-%d-%Y')))

    today = (dt.today()- timedelta(days=1)).strftime('%m-%d-%Y')
    start = "03-22-2020" # the first date we have complete data 
    cases_df = clean_county_cases(cases, today)

    if not snapshot:
        cases_df = clean_county_cases(cases, today, True)
        for d in pd.date_range(start=start, end=today):
            print(d, d.strftime('%m-%d-%Y'))
            df = clean_county_cases(cases, d.strftime('%m-%d-%Y'), True)
            cases_df = cases_df.append(df)
    cases_df['county_id'] = cases_df['county_id'].apply(str)
    cases_df = cases_df.set_index('county_id')
    return cases_df

@df_logging(False)
@get_runtime
def load_state_testing(snapshot=True): 
    ## Write all historical county cases to file
    cases = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/'
    
    print("Loading in all historical county COVID testing from {}{}".format(cases, (dt.today() - timedelta(days=1)).strftime('%m-%d-%Y')))

    today = (dt.today() - timedelta(days=2)).strftime('%m-%d-%Y')
    start = "04-13-2020" # the first date we have complete data 
    print(today)
    cases_df = pd.read_csv(cases + start + '.csv')
    if not snapshot:
#         cases_df = clean_county_cases(cases, today, True)
        for d in pd.date_range(start=start, end=today):
            
            day_str = d.strftime('%m-%d-%Y')
            print(cases + day_str + '.csv')
            df = pd.read_csv(cases + day_str + '.csv')
            cases_df = cases_df.append(df)
#     cases_df['county_id'] = cases_df['county_id'].apply(str)
    return cases_df


@df_logging(verbose)
@get_runtime
@county_pipeline
def load_stanford_forecasts(idx, sd, date=False):
    state = ZipFile(stanford_forecasts, 'r')
    counties = [s for s in state.namelist() if 
                s.find('results/county/')==0 and s.find(f'{sd}.json')>1]
    print(json.loads(state.read(counties[0]).decode("utf-8"))[idx])
    date = json.loads(state.read(counties[0]).decode("utf-8"))[idx][1]
    rows = []
    print(len(counties))
    for county in counties:

        c = json.loads(state.read(county).decode("utf-8"))
        county_id = str(county[18:23])
        if date:
            rows.append([county_id, date] + list(map(float, c[idx][9:13])))
            cases_df = pd.DataFrame(rows)
            cases_df.columns = ['county_id', 'date', 'predicted_icu', 'predicted_cases', 'predicted_total_deaths', 'predicted_beds']
    
        else:
            rows.append([county_id] + list(map(float, c[idx][9:13])))
            cases_df = pd.DataFrame(rows)
          
        cases_df.columns = ['county_id','predicted_icu', 'predicted_cases', 'predicted_total_deaths', 'predicted_beds']
        #print([county_id, case_pred, death_pred])

    
    cases_df['predicted_percent_beds_full'] = cases_df['predicted_icu'] /cases_df['predicted_beds']
    return cases_df

def clean_stanford_forecasts(df):
    df['predicted_total_deaths'] = df[['predicted_total_deaths', 'deaths']].values.max(axis=1)
    df['deaths_delta'] =  df['predicted_total_deaths']  - df['deaths'] 
    df['cases_delta'] =  df['predicted_cases']  - df['cases'] 
    return df


def load_forecasts(forecasts):
    cols = ['allbed_mean', 'ICUbed_mean',  'InvVen_mean', 'totdea_mean', 'deaths_mean']

    zipped = BytesIO(requests.get(forecasts).content)
    state = ZipFile(zipped).open(ZipFile(zipped).namelist()[1]).read()
    state = pd.read_csv(BytesIO(state))
    state = state[state[cols].all(axis='columns')]
    return state

@df_logging(verbose)
@get_runtime
@county_pipeline
def load_county_ihme_forecasts(forecasts):
    print("Loading in all county COVID forecasts from %d", forecasts)

    # Open Zip file in order to read in forecasts
    state = load_forecasts(forecasts)

    # Calculate State statistics from county stats
    statepop = county[['state','population', 'cases']].groupby('state').sum().reset_index()
    statepop.columns = ['state', 'statepop', 'statecases']

    # Compute county level forecasts
    county = county[~county.county_id.str.contains('000')]
    joined = state.merge(county, left_on='location_name', right_on='state')  
    joined = joined.merge(statepop, on='state')
    print(joined.columns)
    joined.columns = joined.columns.map(lambda x: x.lower())
    joined.set_index('county_id', inplace=True)
    return joined

@df_logging(verbose)
@get_runtime
@county_pipeline
def load_county_ihme_now(df):
    # Open Zip file in order to read in forecasts
    
    df = load_snapshot_county_forecasts(df, days_out=0)

    # Rename columns to match present numbers
    # Need to do this because we're reusing the forecast function 
    # to generate data about the present. 
    swaps = {'predicted_cases': 'cases', 'predicted_total_deaths': 'deaths'}
    for col_old, col_new in swaps.items():
        df[col_new] = df[col_old]
        df = df.drop(col_old, axis=1)
    df = df.drop(['cases_delta', 'deaths_delta'], axis=1)
    print(df.columns)
    
    return df.reset_index().rename(columns={'index': 'county_id'})


@df_logging(verbose)
@get_runtime
@county_pipeline
def load_snapshot_county_forecasts(df, days_out=7):
    ## compute county forecasts from UW State Forecasts
    
    # load constants
    date = (dt.today() + timedelta(days=days_out)).strftime('%Y-%m-%d')   
    print("Loading in all county COVID forecasts from %d", forecasts_url, date)

    # Open Zip file in order to read in forecasts
    state = load_forecasts(forecasts_url)
    
    # preprocessing: join tables
    df = df.reset_index().rename(columns={'index': 'county_id'})
    df['county_id'] = df['county_id'].str.rjust(5, '0')
    joined = state.merge(df, left_on='location_name', right_on='state') 
   
    # Compute state deaths and state cases 
    states = joined[joined.county_id.str.contains('000')] # this gets data for state FIPS
    joined = joined[joined['date']==date]
    states['cases_delta'] = states['est_infections_mean'] - states['cases']
    states['deaths_delta'] = states['totdea_mean'] - states['deaths']
    states['cases_states'] = states['est_infections_mean']
    states['deaths_states'] = states['totdea_mean']

    # Join in computed values and compute the county level predictions
    joined = joined.merge(states[['state', 'cases_delta', 'deaths_delta', 'cases_states', 'deaths_states']], on='state')
    joined['predicted_cases'] = joined['cases_delta'] * joined['cases'] / joined['cases_states'] + joined['cases']
    joined['predicted_total_deaths'] = joined['deaths_delta'] * joined['deaths'] / joined['deaths_states'] + joined['deaths']
    joined = joined.drop(['cases_delta', 'deaths_delta', 'cases_states', 'deaths_states'], axis=1)
    joined['deaths_delta'] =  joined['predicted_total_deaths']  - joined['deaths'] 
    joined['cases_delta'] =  joined['predicted_cases']  - joined['cases']     
    joined = joined[['county_id', 'predicted_cases', 'predicted_total_deaths', 'cases_delta', 'deaths_delta']]
    print(joined.shape)
    df = df.merge(joined, on='county_id', how='outer')
    print(df.columns)
    return df

def clean_ihme_forecasts(df):
    df['predicted_total_deaths'] = df['deaths_mean'] 
    df['predicted_cases'] = df['predicted_cases']
    df['deaths_delta'] =  df['predicted_deaths']  - df['deaths'] 
    df['cases_delta'] =  df['predicted_cases']  - df['cases'] 
    return df


@df_logging(verbose)
@get_runtime
@county_pipeline
def load_social_distance(df):
    social = pd.read_csv(df)
    social = social[social['date']==social['date'].max()]
    social['county_id'] = social['fips'].map(lambda x: str(x).rjust(5, '0'))
    social = social[['county_id', 'avg_dist_traveled', 'avg_home_dwell_time']]
    social.columns= ['county_id', 'social_distance_score2', 'social_distance_score1']
    return social

@df_logging(verbose)
@get_runtime
# @county_pipeline
def county_cleanup(df):
    print(df.head())
    # drop unneeded columns and get rid of states
    df = df.drop(['recovered','date', 'active'], 1, errors='ignore')
    df['county_name'] = df['county'] + ', ' + df['state']
    df = df[~df.index.str.contains('000')]
    df = df.rename_axis('county_id').reset_index()
    df['county_id'] = df['county_id'].str.rjust(5, '0')
    ### Choose final columns to upload
    final = df[['county_id', 'social_distance_score2', 'social_distance_score1',
           'hcc_score',  #'allbed_mean', 'ICUbed_mean', 'InvVen_mean', 
           'poverty', 'cases', 'deaths', 'state',
           'county', 'population', #'percent_confirmed',
           #'percent_deaths', 'BEDS', 'predicted_percent_beds_full',
           'predicted_cases', #'predicted_deaths', 
                'predicted_total_deaths',
           'deaths_delta', 'cases_delta', 'population_pct', 'cases_pct',
           'deaths_pct', 'poverty_pct', 'hcc_score_pct',
           'social_distance_score2_pct', 'social_distance_score1_pct',
           'predicted_cases_pct', 'predicted_total_deaths_pct',
           #'predicted_percent_beds_full_pct', 
                          'deaths_delta_pct',
           'cases_delta_pct', 'county_name']]

    final.columns = ['county_id', 'social_distance_score2', 'social_distance_score1',
           'hcc_score',  #'predicted_beds', 'predicted_icu', 'predicted_vents', 
            'poverty', 'cases', 'deaths', 'state',
           'county', 'population', #'percent_confirmed',
           #'percent_deaths', 'beds', 'predicted_percent_beds_full',
           'predicted_cases', #'predicted_deaths', 
                     'predicted_total_deaths',
           'deaths_delta', 'cases_delta', 'population_pct', 'cases_pct',
           'deaths_pct', 'poverty_pct',  'hcc_score_pct',
           'social_distance_score2_pct', 'social_distance_score1_pct',
           'predicted_cases_pct', 'predicted_total_deaths_pct',
           #'predicted_percent_beds_full_pct', 
                     'deaths_delta_pct',
           'cases_delta_pct', 'county_name']
    return final

def get_CAN_predictions(df, get="county", days=7):    
    # Add CAN predictions
    #print(df['index'])
    week =  dt.strftime(dt.today() + timedelta(days=days), '%Y-%m-%d')
    
    if get=='county':
        county_url = "https://data.covidactnow.org/latest/us/counties/{}.OBSERVED_INTERVENTION.timeseries.json".format(df['index'])
        #
        
#         print(week, county_url)
        try:
            predictions = requests.get(county_url, timeout=10)
#             print(predictions.content)
            predictions = json.loads(predictions.content)
            predictions = predictions['timeseries']
            results = [prediction for prediction in predictions if prediction['date']==week][0]
#             print(week, county_url,results['cumulativeInfected'], results['cumulativeDeaths'])
            df['predicted_cases'], df['predicted_total_deaths']= results['cumulativeInfected'], results['cumulativeDeaths']
        except Exception as e: 
            df['predicted_cases'], df['predicted_total_deaths']= 0,0
            
    if get=='state':
        state_url = 'https://data.covidactnow.org/latest/us/states/{}.OBSERVED_INTERVENTION.timeseries.json'.format(df['state_abbr'])
        try:
            predictions = json.loads(requests.get(state_url, timeout=30).content)['timeseries']
            results = [prediction for prediction in predictions if prediction['date']==week][0]
            df['predicted_state_cases'], df['predicted_state_total_deaths']= results['cumulativeInfected'], results['cumulativeDeaths']
        except Exception as e: 
            df['predicted_state_cases'], df['predicted_state_total_deaths']= 0,0

    return df

def fill_CAN_missing_predictions(df):
    # COVIDACTNow "censors" counties with too few cases. Here we're gonna fill them in to the best of our ability
    
    # Create a column for censored cases and censored deaths, only non-null when they are censored
    df = df.rename_axis('county_id').reset_index()
    df['censored_cases'] = df.apply(lambda r: r['cases'] if r['predicted_cases']==0 else 0, axis=1)
    df['censored_deaths'] = df.apply(lambda r: r['deaths'] if r['predicted_total_deaths']==0 else 0, axis=1)
    df = df[~df['county_id'].apply(lambda x: x[2:]=="000")]
    
    # Compute the sums at the state level for number of state cases, state deaths, state sum predicted cases, predicted sum deaths,
    # and the state sum of censored cases and censored deaths. 
    states = df.groupby('state').agg({'cases': sum, 'deaths': sum, 'predicted_cases': sum, 'predicted_total_deaths':sum, 'censored_cases':sum, 'censored_deaths':sum}).reset_index()
    
    # Grab the state abbreviation code and use it to hit the CAN state APi to bring down the true state level predictions
    states['state_abbr'] = states['state'].apply(state_name_to_abbr)
    states = states.apply(lambda x: get_CAN_predictions(x, 'state'), axis=1) 
    
    # Calculate the growth factor on states
    states['cases_rt'] =  states['predicted_state_cases'] / states['cases']
    states['deaths_rt'] = states['predicted_state_total_deaths'] / states['deaths']
    print(states['predicted_state_cases'],  states['predicted_cases'], states['cases_rt'])

    # Join in computed values and compute the county level predictions
    df = df.drop(['censored_cases', 'censored_deaths'], axis=1)
    joined = df.merge(states[['state', 'cases_rt', 'deaths_rt', 'censored_cases', 'censored_deaths']], on='state')
    

    # Calculate imputed cases as rt* observed  
    joined['predicted_cases_impute'] = np.maximum((joined['cases'] * joined['cases_rt']), joined['cases'])
    joined['predicted_deaths_impute'] = np.maximum((joined['deaths'] * joined['deaths_rt']), joined['deaths'])
    print(joined[['predicted_cases', 'cases_rt', 'predicted_cases_impute']])
    joined['predicted_cases'] = joined['predicted_cases'].mask(joined['predicted_cases']==0, joined['predicted_cases_impute'])
    joined['predicted_total_deaths'] = joined['predicted_total_deaths'].mask(joined['predicted_total_deaths']==0, joined['predicted_deaths_impute'])
    print(joined['predicted_cases'])
    
    ## drop all the unneeded columns
    joined = joined.drop(['censored_cases', 'censored_deaths','cases_rt', 'deaths_rt', 'predicted_cases_impute', 'predicted_deaths_impute'], axis=1) #, 'predicted_state_cases', 'predicted_state_total_deaths'], axis=1)
    joined['deaths_delta'] =  joined['predicted_total_deaths']  - joined['deaths'] 
    joined['cases_delta'] =  joined['predicted_cases']  - joined['cases'] 
    joined =joined.set_index('county_id').rename_axis('index')
    return joined
    




    
def add_stanford_forecasts2(df, days=7):
    print(df.index)
    def map_CAN_predictions(df_chunk, days=7):
        # dask subfunction for CAN
        df_chunk = df_chunk.apply(lambda x: get_CAN_predictions(x, days=days), axis=1)
        return df_chunk
    
    # add stanford forecasts, uses dask so it doesn't take 5-ever
    df = dd.from_pandas(df.reset_index(), npartitions=500)
    df = df.map_partitions(lambda x: map_CAN_predictions(x, days=days)).compute()
    if 'index' in df.columns:
        df = df.set_index('index')
    else:
        df = df.set_index('county_id')#.rename_axis('county_id')
    print(df)
    df = fill_CAN_missing_predictions(df)
    print(df)
    if days==0:
        df['cases'] = df['predicted_cases']
        df['deaths'] = df['predicted_total_deaths']
        df = df.drop(['predicted_cases', 'predicted_total_deaths', 'cases_delta', 'deaths_delta'], axis=1)
    
    return df
    

table_ids = {
    'B15003': 'educational attainment',
    'B23025': 'employment status',
    'B03002': 'hispanic_latino',
    #'B02001': 'race', # Will comment: this table is actually redundant with the hispanic table, so commenting it away
    'B17001': 'pov'
}
@df_logging(verbose)
@get_runtime
# @county_pipeline
def get_census_demo():
    variables = [i for tid in table_ids.keys() for i in censusdata.censustable('acs5', 2015, tid)]
    data_dict = {}
    
    for tid in table_ids.keys():
        for (elt, v) in censusdata.censustable('acs5', 2015, tid).items():
            data_dict[elt] = v['concept'] + ' | ' + v['label'] 

    county_data = censusdata.download('acs5', 2015, censusdata.censusgeo([('state', '*'), ('county', '*')]), variables)
    
    def get_fips(county):
        # concat state and county fips from string
        state_fips = county.split('state:')[1].split('>')[0]
        county_fips = county.split('> county:')[1]
        return state_fips + county_fips
    
    def get_county_names(county):
        # extract county names from string
        return str(county).split(':')[0]
    county_data['county_name'] = [get_county_names(str(x)) for x in county_data.index] 
    county_data['county_id'] = [get_fips(str(x)) if x else 'NaN' for x in county_data.index] 
    
    county_data = county_data.reset_index(drop=True).set_index('county_id')
    
    
    county_data.to_csv('./data/groomed_data/county_census.csv', index=False)
    with open('./data/groomed_data/county_census_data_dict.json', 'w') as f:
        f.write(json.dumps(data_dict))
    
    return county_data   
    
####################################
### Hospital Operations
####################################

def load_nursing_homes(df, beds):
     ## Add in nursing homes above n beds
    nh = pd.read_csv(df)
    nh['hospital_id'] = nh['ID'].map(lambda x: int('9999' + str(x)) if len(str(x)) < 6 else x)
    nh = nh[nh['BEDS']>beds]
    return nh
    
def create_address_fields(df, cols=['ADDRESS', 'CITY', 'STATE', 'ZIP']):
    df['address'] = df[cols].apply(lambda row: 
                (', '.join(row.values.astype(str))).replace("''",''), axis=1) 
    return df
    import json


def latlong_to_cartesian(lat, long, radius_earth=3958): # radius is in miles
    phi = (90.0 - lat)*math.pi/180.0
    theta = (180.0 + long)*math.pi/180.0
    
    x = radius_earth*math.sin(phi)*math.cos(theta)
    y = radius_earth*math.sin(phi)*math.sin(theta)
    z = radius_earth*math.cos(phi)
    
    return (x, y, z)

def create_neighbor_dict(object_ids, latlongs, radius_miles):
    coords_cart = [latlong_to_cartesian(coord[0], coord[1]) for coord in latlongs]
    
    tree = cKDTree(coords_cart)

    neighbors = {}
    for i in range(len(coords_cart)):
        obj_id = object_ids[i]
        coord = coords_cart[i]
        distances, indices = tree.query(coord, len(coords_cart), p=2, distance_upper_bound=radius_miles)
        obj_neighbors = []
        for idx, dist in zip(indices, distances):
            if dist == inf:
                break
            if dist < radius_miles:
                obj_neighbors += [object_ids[idx]]
        neighbors[obj_id] = obj_neighbors        
    return neighbors



def create_need_scores(hospitals, counties, radius_miles=20):
    
    county_info = {}
    for i in range(counties.shape[0]):
        row = counties.iloc[i]
        cid = row['county_id']
        cases = row['cases']
        population = row['population']
        county_info[cid] = {'cases' : cases, 'population': population}
    
    # Run for hospitals, then long term care
    hospital_types = [
         'CHILDREN',
         'CHRONIC DISEASE',
         'CRITICAL ACCESS',
         'GENERAL ACUTE CARE',
         'MILITARY',
         'PSYCHIATRIC',
         'SPECIAL',
         'WOMEN'
    ]
    
    hospital_facilities = hospitals[hospitals['type'].isin(hospital_types)]
    hospital_ids = list(hospital_facilities['hospital_id'])
    hospital_coords = list(zip(list(hospital_facilities['lat']), list(hospital_facilities['long'])))
    hospital_neighbors = create_neighbor_dict(hospital_ids, hospital_coords, radius_miles)
    
    county_to_hospitals = {}
    hospital_to_beds = {}
    for i in range(hospital_facilities.shape[0]):
        row = hospital_facilities.iloc[i]
        cid = row['county_id']
        hid = row['hospital_id']
        beds = row['beds']
        hospital_to_beds[hid] = beds
        if cid not in county_to_hospitals:
            county_to_hospitals[cid] = []
        county_to_hospitals[cid] += [hid]
    
    hospital_to_risk_score = {}
    for i in range(hospital_facilities.shape[0]):
        row = hospital_facilities.iloc[i]
        hid = row['hospital_id']
        neighbors = hospital_neighbors[hid]
        
        total_beds = hospital_to_beds[hid] + sum([hospital_to_beds[h] for h in hospital_neighbors])
        try:
            hospital_county_info = county_info[row['county_id']]
            hospital_to_risk_score[hid] = hospital_county_info['cases']/total_beds
        except:
            print('Cannot find county: {}'.format(row['county_id']))
    
    hospital_risk_score_list = sorted(list(hospital_to_risk_score.values()))
    hospital_to_risk_score = {elt[0] : percentileofscore(hospital_risk_score_list, elt[1]) for elt in hospital_to_risk_score.items()}
    
    # run for long term care
    
    long_term_care_types = [
        'ASSISTED CARE',
        'ASSISTED LIVING',
        'CONTINUING CARE RETIREMENT COMMUNITY',
        'LONG TERM CARE',
        'NURSING HOME',
    ]
    
    ltc_facilities = hospitals[hospitals['type'].isin(long_term_care_types)]
    ltc_ids = list(ltc_facilities['hospital_id'])
    ltc_coords = list(zip(list(ltc_facilities['lat']), list(ltc_facilities['long'])))
    ltc_neighbors = create_neighbor_dict(ltc_ids, ltc_coords, radius_miles)
    
    
    county_to_ltcs = {}
    ltc_to_beds = {}
    for i in range(ltc_facilities.shape[0]):
        row = ltc_facilities.iloc[i]
        cid = row['county_id']
        hid = row['hospital_id']
        beds = row['beds']
        ltc_to_beds[hid] = beds
        if cid not in county_to_ltcs:
            county_to_ltcs[cid] = []
        county_to_ltcs[cid] += [hid]
    
    ltc_to_risk_score = {}
    for i in range(ltc_facilities.shape[0]):
        row = ltc_facilities.iloc[i]
        hid = row['hospital_id']
        neighbors = ltc_neighbors[hid]
        
        total_beds = ltc_to_beds[hid] + sum([ltc_to_beds[h] for h in ltc_neighbors])
        try:
            ltc_county_info = county_info[row['county_id']]
            ltc_to_risk_score[hid] = ltc_county_info['cases']/total_beds
        except:
            print('Cannot find county: {}'.format(row['county_id']))
    
    ltc_risk_score_list = sorted(list(ltc_to_risk_score.values()))
    ltc_to_risk_score = {elt[0] : percentileofscore(ltc_risk_score_list, elt[1]) for elt in ltc_to_risk_score.items()}
    
    risk_scores = []
    for i in range(hospitals.shape[0]):
        row = hospitals.iloc[i]
        hid = row['hospital_id']
        if hid in hospital_to_risk_score:
            risk_scores += [hospital_to_risk_score[hid]]
        elif hid in ltc_to_risk_score: 
            risk_scores += [ltc_to_risk_score[hid]]
        else:
            risk_scores += [np.nan]
    hospitals['risk_score'] = risk_scores
            
    
    return hospitals
        
