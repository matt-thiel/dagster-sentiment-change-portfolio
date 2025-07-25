import os
import boto3
from dotenv import load_dotenv
from arcticdb import Arctic
from dagster import resource

@resource
def s3_resource(context):
    """
    Initializes and returns a boto3 S3 client using environment variables.

    Returns:
        boto3.client: Configured S3 client.
    """
    load_dotenv()
    s3_client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
    return s3_client

@resource(required_resource_keys={"s3"})
def arctic_db_resource(context):
    """
    Initializes and returns an ArcticDB store connected to S3 using environment variables.

    Returns:
        Arctic: ArcticDB store instance.

    Raises:
        ValueError: If initialization fails or environment variables are missing.
    """
    load_dotenv()
    bucket_name = os.environ["S3_BUCKET"]
    #folder_name = os.environ["S3_FOLDER"]
    region = os.environ["AWS_REGION"]
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    try:
        store = Arctic(f's3://{endpoint_url}:{bucket_name}?region={region}&access={access_key}&secret={secret_key}')
    except Exception as e:
        raise ValueError(f"Error initializing ArcticDB in S3. Check that bucket exists and ~/.aws/credentials are configured: {e}")
    return store
