
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_key'])
print("Job Name: ", args['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import json
import boto3
import requests
from pyspark.sql import SparkSession

# Initializing the SparkSession
spark = SparkSession.builder.appName("fetchAllTickersFromPolygonApi").config("spark.some.config.option", "some-value").getOrCreate()
# AWS S3 Connection setup
s3 = boto3.client('s3')
print("AWS S3 Connection established")

s3_input_key = args['s3_input_key']
bucket_name = 'finstocktickers'
file_path = s3_input_key

# bucket_name = 'finstocktickers'
# file_path = 'bronzeLayer/allTickersData.json'

df =  spark.read.option("multiline","true").option("mode","PERMISSIVE").json(f"s3://{bucket_name}/{file_path}")

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

finalDf = df.select(df.results)
finalDf = finalDf.withColumn("results",explode(df.results))
schema = StructType([StructField('active', BooleanType(), True),\
                     StructField('cik', StringType(), True),\
                     StructField('composite_figi', StringType(), True),\
                     StructField('currency_name', StringType(), True),\
                     StructField('last_updated_utc', StringType(), True),\
                     StructField('locale', StringType(), True),\
                     StructField('market', StringType(), True),\
                     StructField('name', StringType(), True),\
                     StructField('primary_exchange', StringType(), True),\
                     StructField('share_class_figi', StringType(), True),\
                     StructField('ticker', StringType(), True),\
                     StructField('type', StringType(), True)])
finalDf = finalDf.withColumn("Active_Status", col("results").getItem("active"))\
         .withColumn("Central Index Key", col("results").getItem("cik"))\
         .withColumn("Composite FIGI", col("results").getItem("composite_figi"))\
         .withColumn("Currency Name", col("results").getItem("currency_name"))\
         .withColumn("Last Updated Time", col("results").getItem("last_updated_utc"))\
         .withColumn("Locale", col("results").getItem("locale"))\
         .withColumn("Market", col("results").getItem("market"))\
         .withColumn("Name", col("results").getItem("name"))\
         .withColumn("Primary_Exchange_Type", col("results").getItem("primary_exchange"))\
         .withColumn("Share ClassFIGI", col("results").getItem("share_class_figi"))\
         .withColumn("Ticker", col("results").getItem("ticker"))\
         .withColumn("Type", col("results").getItem("type"))\
         .drop("results")
tickersDf = spark.createDataFrame(finalDf.rdd,schema=schema)
# Defining the bucket name and file path again for another layer
file_path = 'silverLayer/allTickersTransform'
tickersDf.write.mode("overwrite").parquet(f"s3://{bucket_name}/{file_path}")
print(f"File uploaded to s3://{bucket_name}/{file_path}")
job.commit()