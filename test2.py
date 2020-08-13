import pandas as pd 
data_file_path = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

data = pd.read_csv(data_file_path,  dtype={'county_id':str}, parse_dates=['date'])
data['date'] = pd.to_datetime(data['date'])

# rename columns
data.columns = ['date', 'county', 'state', 'county_id', 'Confirmed', 'Deaths']
print('A')
print(data.shape)
# fix new york city
data.loc[(data['state'] == 'New York') & (data['county'] == 'New York City'), 'county_id'] = 'new_york_city'
print(data.shape)
# Kill bad data
data = data[data['county_id'].isna()==False]
print(data.shape)
# Robust load of county_id
#data['county_id'] = data['county_id'].apply(lambda s: str(int(s)).rjust(5, '0'))

# Why is there even duplicate data I did inner joins
data = data.drop_duplicates()
print(data.shape)

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

print(data.shape)
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