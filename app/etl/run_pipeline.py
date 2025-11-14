import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3
import traceback
from clean import clean_file
from log import logger

RAW_BUCKET = "openai-data-hub-raw"

s3 = boto3.client("s3")

def run_pipeline():
    logger.info("=== Starting ETL Pipeline ===")

    try:
        response = s3.list_objects_v2(Bucket=RAW_BUCKET)
    except Exception as e:
        logger.error(f"Failed listing bucket {RAW_BUCKET}: {e}")
        return

    if "Contents" not in response:
        logger.info("No raw files found.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        if not key.endswith(".csv"):
            logger.info(f"Skipping non-CSV file: {key}")
            continue

        logger.info(f"Processing: {key}")

        try:
            clean_file(key)
            logger.info(f"SUCCESS: Finished {key}")

        except Exception as e:
            logger.error(f"ERROR cleaning {key}: {e}")
            logger.error(traceback.format_exc())

    logger.info("=== ETL Pipeline Complete ===")

if __name__ == "__main__":
    run_pipeline()
