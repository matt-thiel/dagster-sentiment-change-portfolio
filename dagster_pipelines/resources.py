"""
Resources for Dagster pipelines.
"""
import os
from dotenv import load_dotenv
from arcticdb import Arctic
from dagster import resource, InitResourceContext
from typing import Optional


def _build_arctic_connection_string(
    bucket_name: str,
    access_key: str,
    secret_key: str,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None
) -> str:
    """
    Build ArcticDB connection string with optional region and endpoint.
    
    Args:
        bucket_name: S3 bucket name
        access_key: AWS access key
        secret_key: AWS secret key
        region: Optional AWS region
        endpoint_url: Optional S3 endpoint URL
        
    Returns:
        Formatted connection string for ArcticDB
    """
    # Determine the endpoint URL
    if endpoint_url:
        # Use provided endpoint URL (for local development, custom endpoints, etc.)
        final_endpoint = endpoint_url
    elif region:
        # Use region-specific AWS endpoint
        final_endpoint = f"s3.{region}.amazonaws.com"
    else:
        # Use default AWS S3 endpoint
        final_endpoint = "s3.amazonaws.com"
    
    # Build the connection string
    connection_string = f"s3://{final_endpoint}:{bucket_name}?access={access_key}&secret={secret_key}"
    
    # Add region parameter if specified
    if region:
        connection_string += f"&region={region}"
    
    return connection_string


# Disable unused argument for debugging purposes
# pylint: disable=unused-argument
@resource
def arctic_db_resource(_init_context: InitResourceContext):
    """
    Initializes and returns an ArcticDB store connected to S3 using environment variables.

    Returns:
        Arctic: ArcticDB store instance.

    Raises:
        ValueError: If initialization fails or environment variables are missing.
    """
    load_dotenv()
    
    # Required environment variables
    bucket_name = os.environ.get("S3_BUCKET")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    
    # Validate required variables
    if not all([bucket_name, access_key, secret_key]):
        missing_vars = []
        if not bucket_name:
            missing_vars.append("S3_BUCKET")
        if not access_key:
            missing_vars.append("AWS_ACCESS_KEY_ID")
        if not secret_key:
            missing_vars.append("AWS_SECRET_ACCESS_KEY")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # Optional environment variables
    region = os.environ.get("AWS_REGION")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    
    # Build connection string
    connection_string = _build_arctic_connection_string(
        bucket_name=bucket_name,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        endpoint_url=endpoint_url
    )
    
    try:
        store = Arctic(connection_string)
    except Exception as e:
        raise ValueError(
            f"Error initializing ArcticDB in S3. Check that bucket exists and credentials are configured: {e}"
        ) from e
    
    return store
