import boto3
import traceback
from clean import clean_file
from log import logger

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3")


def run_pipeline():
    logger.info("=== Starting ETL Pipeline ===")

    # List all files in raw bucket
    try:
        response = s3.list_objects_v2(Bucket=RAW_BUCKET)
    except Exception as e:
        logger.error(f"Failed to list objects in bucket {RAW_BUCKET}: {e}")
        return

    if "Contents" not in response:
        logger.info("No files found in RAW bucket. Nothing to process.")
        return

    # Process each raw CSV
    for obj in response["Contents"]:
        key = obj["Key"]

        # Only process .csv files
        if not key.endswith(".csv"):
            logger.info(f"Skipping non-CSV file: {key}")
            continue

        logger.info(f"Cleaning file: {key}")

        try:
            clean_file(key)
            logger.info(f"SUCCESS: Cleaned and uploaded parquet for {key}")

        except Exception as e:
            logger.error(f"ERROR cleaning file {key}: {e}")
            logger.error(traceback.format_exc())

    logger.info("=== ETL Pipeline Completed ===")


if __name__ == "__main__":
    run_pipeline()
