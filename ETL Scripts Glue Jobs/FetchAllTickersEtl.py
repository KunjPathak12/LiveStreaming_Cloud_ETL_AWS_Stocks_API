import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import boto3
import json

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_key'])
print("Job Name: ", args['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# AWS S3 Connection setup
s3 = boto3.client('s3')
print("AWS S3 Connection established")

# Get the S3 input key from job arguments
s3_input_key = args['s3_input_key']
bucket_name = 'finstockbucket12'
file_path = s3_input_key

# Read the input JSON file from S3
df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(f"s3://{bucket_name}/{file_path}")

# Transform the data
finalDf = df.select(df.results)
finalDf = finalDf.withColumn("results", explode(df.results))

# Define the schema
schema = StructType([
    StructField('active', BooleanType(), True),
    StructField('cik', StringType(), True),
    StructField('composite_figi', StringType(), True),
    StructField('currency_name', StringType(), True),
    StructField('last_updated_utc', StringType(), True),
    StructField('locale', StringType(), True),
    StructField('market', StringType(), True),
    StructField('name', StringType(), True),
    StructField('primary_exchange', StringType(), True),
    StructField('share_class_figi', StringType(), True),
    StructField('ticker', StringType(), True),
    StructField('type', StringType(), True)
])

# Map the JSON fields to the schema
finalDf = finalDf.withColumn("Active_Status", col("results").getItem("active")) \
    .withColumn("Central Index Key", col("results").getItem("cik")) \
    .withColumn("Composite FIGI", col("results").getItem("composite_figi")) \
    .withColumn("Currency Name", col("results").getItem("currency_name")) \
    .withColumn("Last Updated Time", col("results").getItem("last_updated_utc")) \
    .withColumn("Locale", col("results").getItem("locale")) \
    .withColumn("Market", col("results").getItem("market")) \
    .withColumn("Name", col("results").getItem("name")) \
    .withColumn("Primary_Exchange_Type", col("results").getItem("primary_exchange")) \
    .withColumn("Share Class FIGI", col("results").getItem("share_class_figi")) \
    .withColumn("Ticker", col("results").getItem("ticker")) \
    .withColumn("Type", col("results").getItem("type")) \
    .drop("results")

tickersDf = spark.createDataFrame(finalDf.rdd, schema=schema)

# Define the output file path for transformed data
output_file_path = 'silverLayer/allTickersTransform'
tickersDf.write.mode("overwrite").parquet(f"s3://{bucket_name}/{output_file_path}")

print(f"File uploaded to s3://{bucket_name}/{output_file_path}")
job.commit()
