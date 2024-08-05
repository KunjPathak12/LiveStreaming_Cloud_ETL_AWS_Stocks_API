import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import boto3

# Get arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_key'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# AWS S3 Connection setup
s3 = boto3.client('s3')
print("AWS S3 Connection established")
s3_input_key = args['s3_input_key']
bucket_name = 'finstockbucket12'
file_path = s3_input_key

df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(f"s3a://{bucket_name}/{file_path}")

df_ticker = df.withColumn("results", explode(df.results))

df_ticker = df_ticker.withColumn("close", col("results").getItem("c")) \
    .withColumn("high", col("results").getItem("h")) \
    .withColumn("low", col("results").getItem("l")) \
    .withColumn("open", col("results").getItem("o")) \
    .withColumn("volume", col("results").getItem("v")) \
    .withColumn("Average_Price_Traded", col("results").getItem("vw")) \
    .withColumn("numebrOfTrade", col("results").getItem("n")) \
    .withColumn("userticker", df_ticker.ticker) \
    .drop("results") \
    .drop("status") \
    .drop("resultsCount") \
    .drop("request_id") \
    .drop("resultsCount") \
    .drop("queryCount") \
    .drop("count") \
    .drop("adjusted") \
    .drop("ticker")

df_ticker.show(5)
schema = StructType([ \
    StructField('close', DoubleType(), True), \
    StructField('high', DoubleType(), True), \
    StructField('low', DoubleType(), True), \
    StructField('open', DoubleType(), True), \
    StructField('volume', DoubleType(), True), \
    StructField('Average_Price_Traded', DoubleType(), True), \
    StructField('numebrOfTrade', LongType(), True), \
    StructField('userticker', StringType(), True)])
df_ticker = spark.createDataFrame(df_ticker.rdd, schema=schema)
tickerName = df_ticker.select("userticker").distinct()
tickerRow = tickerName.collect()[0]
ticker = tickerRow["userticker"]

# Defining the bucket name and file path again for another layer
file_path = f'silverLayer/{ticker}TickerTransform'
df_ticker.write.mode("overwrite").parquet(f"s3://{bucket_name}/{file_path}")
print(f"File uploaded to s3://{bucket_name}/{file_path}")

allTickersFilePath = 'silverLayer/allTickersTransform/'

allDfTickers = spark.read.parquet(f"s3://{bucket_name}/{allTickersFilePath}")
userTicker = spark.read.parquet(f"s3://{bucket_name}/{file_path}")
FinalData = allDfTickers.join(userTicker, allDfTickers.ticker == userTicker.userticker, "inner")
FinalData = FinalData.drop("userticker").drop("composite_figi").drop("share_class_figi")

goldenLayerFilePath = f'goldenLayer/{ticker}FinStockData'
FinalData.write.mode("overwrite").parquet(f"s3://{bucket_name}/{goldenLayerFilePath}")
print(f"file writen to: s3://{bucket_name}/{goldenLayerFilePath}")

def delete_files_ending_with(bucket_name, suffix):
    continuation_token = None
    while True:
        kwargs = {'Bucket': bucket_name}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        # List objects in the bucket
        response = s3.list_objects_v2(**kwargs)

        # Check if any objects exist
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    s3.delete_object(Bucket=bucket_name, Key=key)
                    print(f'Deleted {key}')
        else:
            print('No objects found in the bucket.')

        # Check if there are more objects to retrieve
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

# Specify the suffix of the files to delete
suffix_to_delete = '.json'
delete_files_ending_with(bucket_name, suffix_to_delete)
job.commit()
