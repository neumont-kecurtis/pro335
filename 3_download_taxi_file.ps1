$url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-04.parquet"
$dest = "yellow_tripdata_2018-04.parquet"
rm $dest
Invoke-WebRequest -Uri $url -OutFile $dest