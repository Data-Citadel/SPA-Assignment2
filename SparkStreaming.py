import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession.builder \
    .master("local") \
    .appName("MyMallStreamingApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

######################################
# Processing the customer profile data
######################################

#Loading the dataset as pandas dataframe
cust_details=pd.read_excel('./MyMall-DataSet.xlsx')

# Renaming columns just to avoid probles due to sapce and special charectors
cust_details.columns = ['cust_id','gender','age','annual_income','spending_score']

# Columns are explored using Histograms in jupyter based on that bins are formulated
# Spending capacity - convert numeric to categorical (High, Medium, Low)
bins = [0, 40, 60, np.inf]
names = ['Low', 'Medium', 'High']
cust_details['spending_score_cat'] = pd.cut(cust_details['spending_score'], bins, labels=names)

# Annual Income - convert numeric to categorical (High, Medium, Low)
bins = [0, 50, 80, np.inf]
names = ['Low', 'Medium', 'High']
cust_details['annual_income_cat'] = pd.cut(cust_details['annual_income'], bins, labels=names)

#convert panads dataframe to spark dataframe 
cust_dim = spark.createDataFrame(cust_details)

###############################################
# Processing the streaming data from kafka topic
##############################################

# Read data from kafka topic and 
df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mymall_cust_topic") \
        .option("startingOffsets", "earliest") \
        .load()

# Preprocessing 
# Extract the JSON values and split that json data in columns 
value_df = df.selectExpr("CAST(value AS STRING)","timestamp")
schema = StructType([
   StructField("id", IntegerType(), True),
   StructField("rad", IntegerType(), True)])
value_df = value_df.select(from_json(col("value"), schema).alias("data"),col('timestamp')).select("data.*","timestamp")

# Group the stream data based on sliding window and customer id
# and compute the min distance and average distance of the customer from mall 
cust_stream_df = value_df.groupBy(
      window(value_df.timestamp, "5 minutes", "5 minutes"),
      value_df.id
      ).agg(
            min("rad").alias("distance"),
            avg("rad").alias('average_distance'),
            max("timestamp").alias('max_time')
            )
# Based on distance and average distance, compute below columns
#  1. is he/she with in mall ?
#  2. is near to mall ?
#  3. is he/she spending more time in mall ?
cust_stream_df = cust_stream_df.selectExpr(
                  "id as cust_id",
                  "case when distance < 3 then 1 else 0 end as is_within_mall",
                  "case when distance between 3 and 6 then 1 else 0 end as is_near_to_mall",
                  "case when ((average_distance - distance) between 0.5 and 1.5) and average_distance < 4 then 1 else 0 end as spending_more_time_in_mall",
                  "max_time"
            )
# Picking the customers who are 1. with in the mall or 2. near to mall 
cust_stream_df = cust_stream_df.filter((col("is_near_to_mall") == 1) | (col("is_within_mall") ==1) )
###############################################
# Join of stream data + customer profile data
# stream + static df join
##############################################

cust_out_df = cust_stream_df.join(cust_dim, "cust_id")

###############################################
# Apply Promos / offers 
##############################################
# dict of promo_name and condition 
promo_dict = {
      "PROMO 1 - Flat 20% discount on All products*" : " is_near_to_mall = 1 and annual_income > 30 ",
      "PROMO 2 - Upto 30% discount on Electronics " : " gender='Male' and is_within_mall = 1 " ,
      "PROMO 3 - Flat 30% woman clotings" : " gender = 'Female' and age >= 15 ",
      "PROMO 4 - Upto 40% on Games" : " spending_more_time_in_mall = 1 " ,
      "PROMO 5 - Flat 10% New Arrivals" : " spending_score_cat = 'High' ",
 }
expression = " CASE "
for promo,condition in promo_dict.items():
      expression = expression + " WHEN "+condition +" THEN '"+ promo+ "' "

expression = expression + " ELSE 'NA' END as promo"

# Apply prod condition on stream dataframe
cust_out_df = cust_out_df.selectExpr(expression, "cust_id")


# To write the output on console

#cust_out_df.writeStream \
#      .format("console") \
#      .outputMode("complete") \
#      .start() \
#      .awaitTermination()

ds = cust_out_df \
  .selectExpr("CAST(cust_id AS STRING) as key", "CAST(promo AS STRING) as value") \
  .writeStream \
  .format("kafka") \
  .outputMode("update") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "mymall_promo_topic") \
  .option("checkpointLocation", "/tmp/checkpoint" ) \
  .start() \
  .awaitTermination()

