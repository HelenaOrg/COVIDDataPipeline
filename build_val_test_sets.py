from datetime import datetime as dt, timedelta
import pandas as pd 
import json

df = pd.read_csv('./data/output_data/final_timeseries.csv', dtype={'county_id' : str})
df['date'] = pd.to_datetime(df['date'])

df_static =  pd.read_csv('./data/output_data/final_static.csv', dtype={'county_id' : str})

print(df['date'].max())
last_day = df['date'].max() - timedelta(days=2*7) # add in 7 days of buffer so we can iterate forward a week, and 7 days so we can ensure we're operating on up-to-date data (poi data updates weekly)

prediction_day_start = last_day - timedelta(days=7*4) # 4 weeks is the longest prediction that we make

print(prediction_day_start)
print(last_day)

evaluation_days = {}
for n_prediction_weeks in range(1, 5):
    task_set = []
    for prediction_offset in range(7):
        prediction_day = prediction_day_start + timedelta(days=prediction_offset)
        target_day = prediction_day + timedelta(days=n_prediction_weeks*7)
        task_set += [{
            'prediction_date' : prediction_day.strftime('%Y-%m-%d'),
            'target_date' : target_day.strftime('%Y-%m-%d')
            }]
    evaluation_days[str(n_prediction_weeks) + '_week_task'] = task_set

end_counties = set(df[df['date'] == last_day]['county_id']) # use the timedelta to add in buffer
start_counties = set(df[df['date'] == prediction_day_start]['county_id']) # use the timedelta to add in buffer
static_counties = set(df_static['county_id'])

print('Missing in static')
print(sorted(list(end_counties - static_counties)))

print('Missing in time series')
print(sorted(list(static_counties - end_counties)))

counties = list(start_counties.intersection(end_counties).intersection(static_counties))

cutoff = int(round(len(counties)/2.0))
test_counties = counties[cutoff:]
val_counties = counties[:cutoff]

with open('./data/output_data/val_test_task_descriptions.json', 'w') as f:
    f.write(json.dumps(
        {
            'dates' : evaluation_days,
            'val_counties' : val_counties,
            'test_counties' : test_counties
        }
    ))

