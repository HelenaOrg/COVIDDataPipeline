from datetime import datetime as dt, timedelta
import pandas as pd 
import json

df = pd.read_csv('./data/output_data/final.csv')
df['date'] = pd.to_datetime(df['date'])

last_day = df['date'].max() - timedelta(days=7)

prediction_day_start = last_day - timedelta(days=7*4)

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

counties = list(set(df[df['date'] == last_day]['county_id']))
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

