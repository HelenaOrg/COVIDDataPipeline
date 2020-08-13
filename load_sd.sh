mkdir -p data/transactions
mkdir -p data/poi/weekly_patterns
mkdir -p data/poi/weekly_patterns_v2
mkdir -p data/groomed_data
mkdir -p data/output_data
mkdir -p data/processed_data/social_distancing
mkdir -p data/processed_data/poi
mkdir -p data/processed_data/assembly
aws s3 sync s3://sg-c19-response/social-distancing/v2/ ./data/social-distancing/ --endpoint https://s3.wasabisys.com --profile safegraph;
aws s3 sync s3://sg-c19-response/transactions-facteus/ ./data/transactions/ --endpoint https://s3.wasabisys.com --profile safegraph;
aws s3 sync s3://sg-c19-response/weekly-patterns/v2/ ./data/poi/weekly_patterns/ --endpoint https://s3.wasabisys.com --profile safegraph;
aws s3 sync s3://sg-c19-response/weekly-patterns-delivery/weekly/ ./data/poi/weekly_patterns_v2/ --endpoint https://s3.wasabisys.com --profile safegraph;
aws s3 sync s3://sg-c19-response/core/ ./data/us_places --endpoint https://s3.wasabisys.com --profile safegraph;
sudo pip3 install censusgeocode ciso8601 tqdm;
sudo pip3 install "dask[dataframe]" --upgrade ;
sudo pip3 install pyshp Shapely geopandas ijson python-rapidjson;
python3 ./data_pipeline.py
python3 ./build_val_test_sets.py
aws s3 cp ./data/output_data/final_timeseries.csv s3://helena-processed-data/data-pipeline/final_timeseries.csv
aws s3 cp ./data/output_data/final_static.csv s3://helena-processed-data/data-pipeline/final_static.csv
aws s3 cp ./data/output_data/val_test_task_descriptions.json s3://helena-processed-data/data-pipeline/val_test_task_descriptions.json