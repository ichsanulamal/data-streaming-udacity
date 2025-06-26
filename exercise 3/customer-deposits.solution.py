from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType
# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date or the amount in sql calculations, we are going
# to cast them as strings
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
depositKafkaMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", StringType()),
        StructField("dateAndTime", StringType())
    ]   
)

# {"customerName":"Trevor Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068","location":"Togo"}
customerKafkaMessageSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("customerLocation", StringType())        
    ]
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("bank-deposits").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

bankDepositsRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-deposits")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
bankDepositsStreamingDF = bankDepositsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
bankDepositsStreamingDF.withColumn("value",from_json("value",depositKafkaMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("BankDeposits")

# Using spark.sql we can select any valid select statement from the spark view
bankDepositsSelectStarDF=spark.sql("select * from BankDeposits")

customersRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-customers")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
customersStreamingDF = customersRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
customersStreamingDF.withColumn("value",from_json("value",customerKafkaMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("BankCustomers")

# Using spark.sql we can select any valid select statement from the spark view
customerSelectStarDF=spark.sql("select customerName, accountNumber as customerNumber from BankCustomers")

# Join the bank deposit and customer dataframes on the accountNumber fields
customerWithDepositDF = bankDepositsSelectStarDF.join(customerSelectStarDF, expr("""
    accountNumber = customerNumber
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

customerWithDepositDF.writeStream.outputMode("append").format("console").start().awaitTermination()

