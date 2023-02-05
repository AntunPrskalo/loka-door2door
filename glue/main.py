import sys
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import lit


args = getResolvedOptions(sys.argv, ["JOB_NAME", "input-bucket", "output-bucket", "date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Create a connection to S3
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Define the bucket and prefix
bucket_name = args["input-bucket"]
process_date = args["date"] if 'date' in  args else '2019-06-01'
prefix = f'data/{process_date}'

# List the files starting on specified date
result = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
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
    database=args["output-bucket"],
    table_name="vehicle_iot_data",
    additional_options={"partitionKeys": ["date"]},
)

job.commit()
