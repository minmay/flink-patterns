# Average Time Series

## Run with Docker

1. ```git clone https://github.com/minmay/flink-patterns.git```
2. ```cd flink-patterns```
3. ```docker build -t mvillalobos/flink-patterns/average-time-series:latest  average-time-series```
4. ```docker run mvillalobos/flink-patterns/average-time-series:latest ```

## Run with JDK 11

1. ```git clone https://github.com/minmay/flink-patterns.git```
2. ```cd flink-patterns```
3. ```./gradlew :average-time-series:run```

## Business Logic

This Flink job loads file: average-time-series/timeseries.csv
as source DataStream.

The input file contains CSV with columns ```name:String, value: double, event_time: Instant.```

It will save the raw input in an embedded Apache Derby database.

Then it will compute the average of the time series with a 15 minute tumbling event time window and upsert the results
into an Apache Derby database.

After the Flink Job finishes, this application will print the results of the values upserted into the time_series table.

As seen in the code, this code uses a ProcessWindowFunction because watermarks are not separated by key.

This code demonstrates the following:

1. how to sink output to a JDBCUpsert table.
2. how to compute the average on a key within a tumbling window

## Goal: Perform forward fill

1. perform a forward fill.
    Given input:
    ```
    a,70,2020-06-22T00:56:30.0000000Z
    a,75,2020-06-22T00:57:30.0000000Z
    a,80,2020-06-22T00:58:30.0000000Z
    a,10,2020-06-23T00:01:30.0000000Z
    a,15,2020-06-23T00:02:30.0000000Z
    a,20,2020-06-23T00:03:30.0000000Z
    b,25,2020-06-23T00:03:30.0000000Z
    b,30,2020-06-23T00:02:30.0000000Z
    b,35,2020-06-23T00:01:30.0000000Z
    b,35,2020-06-23T00:16:30.0000000Z
    a,50,2020-06-23T00:36:30.0000000Z
    a,55,2020-06-23T00:37:30.0000000Z
    a,60,2020-06-23T00:38:30.0000000Z
    ```
   so that the average are
   ```
    a,75,2020-06-22 00:45:00.0
                               <-- b did not exist yet    
    a,15,2020-06-23 00:00:00.0
    b,30,2020-06-23 00:00:00.0
    a,15,2020-06-23 00:15:00.0 <-- forward fill
    b,35,2020-06-23 00:15:00.0   
    a,15,2020-06-23 00:30:00.0
    b,35,2020-06-23 00:30:00.0 <-- forward fill   
   ```
   Notice that although no value arrived "a" in the second quarter, the previous average was inserted.

 
