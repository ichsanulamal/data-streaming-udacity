from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType
# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the odometer or miles from shop in sql calculations, we are going
# to cast them as strings
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
kafkaMessageSchema = StructType (
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", StringType()),
        StructField("odometerReading", StringType())        
    ]
    
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("vehicle-status").getOrCreate()
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
vehicleStatusStreamingDF.withColumn("value",from_json("value",kafkaMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("VehicleStatus")

# Using spark.sql we can select any valid select statement from the spark view
vehicleStatusSelectStarDF=spark.sql("select * from VehicleStatus")

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       4382|     Georgia|          508|           null|
# |       8347|   Louisiana|          328|           null|
# |       4516|South Dakota|          372|           null|
# |       6618|     Georgia|           13|           null|
# +-----------+------------+-------------+---------------+

vehicleStatusSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination()

