mkdir -p data/transactions
mkdir -p data/weekly_patterns
mkdir -p data/groomed_data
mkdir -p data/output_data
mkdir -p data/processed_data
aws s3 sync s3://sg-c19-response/social-distancing/v2/ ./data/social-distancing/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/transactions-facteus/ ./data/transactions/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/weekly-patterns/v2/ ./data/weekly_patterns/ --endpoint https://s3.wasabisys.com ;
aws s3 sync s3://sg-c19-response/weekly-patterns-delivery/weekly/ ./data/weekly_patterns/v2/ --endpoint https://s3.wasabisys.com
aws s3 sync s3://sg-c19-response/core/ ./data/us_places --endpoint https://s3.wasabisys.com
sudo pip3 install censusgeocode ciso8601 tqdm;
sudo pip3 install "dask[dataframe]" --upgrade ;
python3 ./data_pipeline.py