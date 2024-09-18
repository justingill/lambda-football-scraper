from scraper import run
from bucket_access import (
    s3_copy,
    s3_create_presigned_url_for_bucket,
    s3_upload_file
)
from logger_config import get_logger
import json
import traceback
import yagmail
import os

logger = get_logger("handler")


def lambda_handler():
    try:
        dataframe = run()
        logger.info("Finished running scraper")
        uploaded = s3_upload_file(dataframe)
        logger.info("Finished Uploading to S3")
        if uploaded:
            s3_copy()
            logger.info("Finished copying to buckets.")
            resp = s3_create_presigned_url_for_bucket(604700)
            logger.info("Finished creating Presigned URL.")
            yag = yagmail.SMTP(
                user=os.environ["USER"], password=os.environ["PASSWORD"]
            )
            logger.info("Authenticated to gmail.")
            yag.send(
                os.environ["RECEIPIENTS"].split(","),
                "Weekly Football API Data",
                resp,
            )
            logger.info("Email sent.")
            return {
                "statusCode": 200,
                "body": json.dumps({"status ": "success"}),
            }
        else:
            raise Exception("S3 Upload failed.")
    except Exception:
        trace_exception = traceback.format_exc()
        return {
            "statusCode": 400,
            "body": json.dumps(
                {"status ": f"error: {trace_exception}"}
            ),
        }


if __name__ == "__main__":
    resp = lambda_handler()
    print(resp)
