# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

### Question 1 Answer:
>My answer:
```
'3.3.2'
```



> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

### Question 2 Answer:
>My code:
```
output_path = f'data/pq/yellow/2024/10/'
df_yellow_202410 \
    .repartition(4) \
    .write.parquet(output_path)
```

>My answer:
```
I get 4 parquet files with size 24.2 MB, so 25MB is the answer.
```


## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

### Question 3 Answer:
>My code:
```
spark.sql("""
SELECT
    count(*) AS total_trips
FROM
    yellow_data
WHERE
    CAST(pickup_datetime AS DATE) = '2024-10-15' AND CAST(dropoff_datetime AS DATE) = '2024-10-15'
""").show()
```

>My answer:
```
125,567
```


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

### Question 4 Answer:
>My code:
```
spark.sql("""
SELECT 
    MAX((unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 3600) AS max_trip_length
FROM yellow_data
""").show()
```

>My answer:
```
+------------------+
|   max_trip_length|
+------------------+
|162.61777777777777|
+------------------+
```


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Question 5 Answer:
>My answer:
```
4040
```



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

### Question 6 Answer:
>My code:
```
spark.sql("""
SELECT 
    yellow_data.PULocationID,
    zone_data.Zone,
    count(1) AS zone_freq
FROM yellow_data left join zone_data on yellow_data.PULocationID = zone_data.LocationID
GROUP BY
    1, 2
ORDER BY 
    zone_freq 
""").show()
```

>My answer:
```
Governor's Island/Ellis Island/Liberty Island
```


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
