# PRO335 - Taxi Data Project

# Tools Required:
```
 powershell 7
 postgresql
 datagrip
 python (3.x)
```
create a directory for your project
```
mkdir taxi-data-project
cd taxi-data-project
```
DOWNLOAD a taxi record to do basic test with
```
.\3_download_taxi_file.ps1
```
run this powershell script to init your python venv
```
.\1_init_venv.ps1
```
verify your python application works from the command line
```
python app.py process
```
uncomment these lines when ready to process the row data
```
  #for index, row in df.iterrows():
  #  print(row["index"])
```
TESTING your postgresql connection
> if this works, you will have a table and a row of data inside postgres. you should be able to see it in datagrip
```
.\2_init_test_pgsql.ps1
```
To RUN your process with a custom taxi file
```
python app.py process --file yellow_tripdata_2018-04.parquet
```
when you write your own custom methods, put them in this section of the file:
```
############################

def custom_method():
    return True





############################
```

