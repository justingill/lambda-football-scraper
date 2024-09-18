import boto3
from botocore.exceptions import ClientError
import logging
import os
import pandas as pd
from logger_config import get_logger

logger = get_logger("bucket_access")


def s3_copy():
    """
    Copy the dataset from one bucket to another.
    """
    s3_client = boto3.client("s3")
    try:
        copy_source = {
            "Bucket": "premier-league-bucket-stats",
            "Key": "Full_Premier_League.csv",
        }
        response = s3_client.copy(
            copy_source,
            "public-bucket-football-pl-data",
            "Full_Premier_League_public.csv",
        )
    except ClientError as e:
        logging.error(e)
        raise ClientError("Copy in S3 could not be completed.")

    return response


def s3_create_presigned_url_for_bucket(expiration=10):
    """
    Create a presigned URL.
    """
    s3_client = boto3.client("s3")
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": "public-bucket-football-pl-data",
                "Key": "Full_Premier_League_public.csv",
            },
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.error(e)
        raise ClientError("Presigned URL Could not be created.")

    return response


def s3_upload_file(dataframe: pd.DataFrame) -> bool:
    """
    Upload a file to an S3 bucket.
    """
    dataframe.to_csv("/tmp/temp.csv")
    s3_client = boto3.client("s3", region_name="us-east-1")
    try:
        s3_client.upload_file(
            "/tmp/temp.csv",
            "premier-league-bucket-stats",
            "Full_Premier_League.csv",
        )
    except ClientError as e:
        os.remove("/tmp/temp.csv")
        logging.error(e)
        return False
    os.remove("/tmp/temp.csv")
    return True
