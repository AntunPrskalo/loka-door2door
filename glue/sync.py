import sys
import os
import boto3
from datetime import datetime, timedelta
from botocore import UNSIGNED
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import lit

required_params = ["JOB_NAME", "input_bucket", "input_bucket_key", "output_datacatalog_database", "output_datacatalog_table"]
if ('--{}'.format('date') in sys.argv):
    args = getResolvedOptions(sys.argv, required_params + ["date"])
else:
    args = getResolvedOptions(sys.argv, required_params)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initiate logging
logger = glueContext.get_logger()

# Create a connection to S3
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Get the date of the files to be processed (yesterday's date)
yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

# Define the bucket and prefix
bucket_name = args["input_bucket"]
process_date = args.get("date", yesterday)
prefix = f'{args["input_bucket_key"]}{process_date}'

logger.info(f"Running Glue Job sync for date: {process_date}")

# List the files starting on specified date
result = s3.list_objects(Bucket=bucket_name, Prefix=prefix)

if result.get("Contents") == None:
    logger.info(f"Done. No files found with prefix: {prefix}")
    job.commit()
    os._exit(0)

s3_paths = [f"s3://{bucket_name}/{content['Key']}" for content in result.get("Contents")]

# Fetch data for specified date
input_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={"paths": s3_paths},
    transformation_ctx="input_df",
)

# Define mapping
mapped_df = ApplyMapping.apply(
    frame=input_df,
    mappings=[
        ("event", "string", "event", "string"),
        ("on", "string", "on", "string"),
        ("at", "string", "at", "string"),
        ("data", "struct", "data", "struct"),
        ("organization_id", "string", "organization_id", "string")
    ],
    transformation_ctx="mapped_df",
)

# Add date partition field
spark_df = mapped_df.toDF()
spark_df = spark_df.withColumn("date", lit(process_date))
df_new_column = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame")

# Merge the input files
merged_df = df_new_column.coalesce(1)

# Write parquet file
glueContext.write_dynamic_frame.from_catalog(
    frame=merged_df,
    database=args["output_datacatalog_database"],
    table_name=args["output_datacatalog_table"],
    additional_options={
        "partitionKeys": ["date"], 
        "overwrite": "true"
    },
)

logger.info(f"Done.")

job.commit()
