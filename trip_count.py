"""
'VendorID', 
'tpep_pickup_datetime', 
'tpep_dropoff_datetime',
'passenger_count', 
'trip_distance', 
'RatecodeID', 
'store_and_fwd_flag',
'PULocationID', 
'DOLocationID', 
'payment_type', 
'fare_amount', 'extra',
'mta_tax', 
'tip_amount', 
'tolls_amount', 
'improvement_surcharge',
'total_amount', 
'congestion_surcharge', 
'airport_fee',
"""
from pyspark import SparkConf, SparkContext
import os 
import pandas as pd

conf = SparkConf().setMaster("local").setAppName("trip_data")
sc = SparkContext(conf=conf)



filename = "trip_data.csv"
directory = f"{os.getcwd()}/data"

textfile = sc.textFile(f"file:///{directory}/{filename}")
header = textfile.first()
filtered_line = textfile.filter(lambda row: row != header)

count_passenger = filtered_line.map(lambda row: row.split(",")[1].split(" ")[0])
count = count_passenger.countByValue()


pd.Series(count, name="trip").to_csv("test.csv")