aws s3 sync s3://sg-c19-response/social-distancing/v2/ ./data/social-distancing/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/transactions-facteus/ ./data/transactions/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/weekly-patterns/v2/ ./data/weekly_patterns/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/weekly-patterns-delivery/weekly/ ./data/weekly_patterns/v2/ --endpoint https://s3.wasabisys.com
python -m pip install censusgeocode ciso8601 tqdm;
python -m pip install "dask[dataframe]" --upgrade ;
python ./social_distancing.py