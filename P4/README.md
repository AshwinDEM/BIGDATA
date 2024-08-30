
-- Group by Year and dump the result in a bag 
```
data = LOAD 'prg124.csv' USING PigStorage(',')
       AS (CityName:chararray, Location:chararray, Year:int, Temperature:float, State:chararray, GasPresent:chararray, QuantityofGas:float);
grouped_by_year = GROUP data BY Year;
year_bag = FOREACH grouped_by_year GENERATE group AS Year, TOBAG(data) AS DataBag;
DUMP year_bag;
```


-- Write a pig script to find the maximum temperature
```
data = LOAD 'prg124.csv' USING PigStorage(',')
       AS (CityName:chararray, Location:chararray, Year:int, Temperature:float, State:chararray, GasPresent:chararray, QuantityofGas:float);
filtered_data = FILTER data BY Temperature IS NOT NULL;
max_temperature = FOREACH (GROUP filtered_data ALL) GENERATE MAX(filtered_data.Temperature) AS MaxTemp;
DUMP max_temperature;
```


-- Write a pig Script to find the average temperature of a state for 3 years and store the result in  
HDFS 
```
data = LOAD 'prg124.csv' USING PigStorage(',')
       AS (CityName:chararray, Location:chararray, Year:int, Temperature:float, State:chararray, GasPresent:chararray, QuantityofGas:float);

filtered_data = FILTER data BY (Year >= 2010 AND Year <= 2012);

valid_temperature_data = FILTER filtered_data BY Temperature IS NOT NULL;

grouped_by_state = GROUP valid_temperature_data BY State;

avg_temperature = FOREACH grouped_by_state GENERATE group AS State, AVG(valid_temperature_data.Temperature) AS AvgTemp;

STORE avg_temperature INTO '/outputp4c' USING PigStorage(',');
```
