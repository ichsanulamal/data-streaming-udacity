from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType
# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date or the amount in sql calculations, we are going
# to cast them as strings
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
vehicleStatusSchema = StructType (
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", StringType()),
        StructField("odometerReading", StringType())     
    ]   
)

# {"reservationId":"1601485848310","locationName":"New Mexico","truckNumber":"3944","status":"In"}
vehicleCheckinSchema = StructType (
    [
        StructField("reservationId", StringType()),
        StructField("locationName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("status", StringType())     
    ]
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("vehicle-checkin").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

vehicleStatusRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","vehicle-status")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
vehicleStatusStreamingDF = vehicleStatusRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
vehicleStatusStreamingDF.withColumn("value",from_json("value",vehicleStatusSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("VehicleStatus")

# Using spark.sql we can select any valid select statement from the spark view
vehicleStatusSelectStarDF=spark.sql("select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from VehicleStatus")

vehicleCheckinRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","check-in")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
vehicleCheckinStreamingDF = vehicleCheckinRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
vehicleCheckinStreamingDF.withColumn("value",from_json("value",vehicleCheckinSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("VehicleCheckin")

# Using spark.sql we can select any valid select statement from the spark view
vehicleCheckinSelectStarDF=spark.sql("select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin")

# Join the bank deposit and customer dataframes on the accountNumber fields
checkinStatusDF = vehicleStatusSelectStarDF.join(vehicleCheckinSelectStarDF, expr("""
    statusTruckNumber = checkinTruckNumber
"""                                                                                 
))

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+

checkinStatusDF.writeStream.outputMode("append").format("console").start().awaitTermination()

