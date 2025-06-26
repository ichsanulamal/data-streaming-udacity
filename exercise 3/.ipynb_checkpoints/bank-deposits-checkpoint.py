from pyspark.sql import SparkSession

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}

# TO-DO: create a spark session, with an appropriately named application name

#TO-DO: set the log level to WARN

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

#TO-DO: using chaining, invoke the withColumn method, overloading the value column with the from_json function

# TO-DO: create a temporary streaming view called "ATMVisits" based on the streaming dataframe
# it can later be queried with spark.sql

#TO-DO: using spark.sql, select * from ATMVisits

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +---------+-----+
# |      key|value|
# +---------+-----+
# |241325569|Syria|
# +---------+-----+


