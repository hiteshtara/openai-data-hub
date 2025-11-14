import boto3
from log import logger
from etl.clean import clean_file
from vectors.embed_data import embed_parquet

RAW_BUCKET = "openai-data-hub-raw"

s3 = boto3.client("s3", region_name="us-east-1")

def run_pipeline():
    logger.info("=== ETL Pipeline Start ===")

    # List raw files
    response = s3.list_objects_v2(Bucket=RAW_BUCKET)
    if "Contents" not in response:
        logger.info("No raw files found.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if not (key.endswith(".csv") or key.endswith(".xlsx")):
            logger.info(f"Skipping non-data file: {key}")
            continue

        logger.info(f"Processing: {key}")

        # Step 1 — Clean file
        clean_key = clean_file(key)
        if not clean_key:
            logger.error(f"Cleaning failed: {key}")
            continue

        # Step 2 — Embed cleaned parquet using FAISS
        success = embed_parquet(clean_key)
        if not success:
            logger.error(f"Embedding failed: {clean_key}")
            continue

        logger.info(f"SUCCESS for {key}")

    logger.info("=== ETL Pipeline Complete ===")


if __name__ == "__main__":
    run_pipeline()
