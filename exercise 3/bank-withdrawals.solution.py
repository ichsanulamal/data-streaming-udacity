from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType
# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date or the amount in sql calculations, we are going
# to cast them as strings
# {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
bankWithdrawalsMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", StringType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", StringType())
    ]   
)

# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}
atmWithdrawalsMessageSchema = StructType (
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", StringType()),
        StructField("atmLocation", StringType()),
    ]
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("bank-withdrawals").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

bankWithdrawalsRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-withdrawals")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
bankWithdrawalsStreamingDF = bankWithdrawalsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
bankWithdrawalsStreamingDF.withColumn("value",from_json("value",bankWithdrawalsMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("BankWithdrawals")

# Using spark.sql we can select any valid select statement from the spark view
bankWithdrawalsSelectStarDF=spark.sql("select * from BankWithdrawals")

customersRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-withdrawals")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
atmStreamingDF = customersRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
atmStreamingDF.withColumn("value",from_json("value",atmWithdrawalsMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("AtmWithdrawals")

# Using spark.sql we can select any valid select statement from the spark view
atmSelectStarDF=spark.sql("select transactionDate, transactionId as atmTransactionId, atmLocation from AtmWithdrawals")

# Join the bank deposit and customer dataframes on the accountNumber fields
atmWithdrawalDF = bankWithdrawalsSelectStarDF.join(atmSelectStarDF, expr("""
    transactionId = atmTransactionId
"""                                                                                 
))

# this takes the stream and "sinks" it to kafka as it is updated one message at a time in JSON format:
# {"accountNumber":"862939503","amount":"844.8","dateAndTime":"Oct 7, 2020 12:33:34 AM","transactionId":"1602030814320","transactionDate":"Oct 7, 2020 12:33:34 AM","atmTransactionId":"1602030814320","atmLocation":"Ukraine"}

atmWithdrawalDF.selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()

