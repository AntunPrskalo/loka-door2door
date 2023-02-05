import boto3
from botocore import UNSIGNED
from botocore.client import Config

#create a connection to S3
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

#define the bucket and folder name
bucket_name = 'de-tech-assessment-2022'
prefix = 'data/2019-06-01-15-20-'

#list the contents of the folder
result = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
s3_paths = [f"s3://{bucket_name}/{content['Key']}" for content in result.get("Contents")]
print(s3_paths)