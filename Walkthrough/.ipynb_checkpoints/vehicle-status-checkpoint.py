from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}

# TO-DO: create a spark session, with an appropriately named application name

#TO-DO: set the log level to WARN

#TO-DO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

# TO-DO: create a temporary streaming view called "VehicleStatus" 
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from VehicleStatus

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       4382|     Georgia|          508|           null|
# |       8347|   Louisiana|          328|           null|
# |       4516|South Dakota|          372|           null|
# |       6618|     Georgia|           13|           null|
# +-----------+------------+-------------+---------------+


