import pandas as pd
import boto3
from io import BytesIO

from validate import validate_dataframe
from summary import summarize_dataframe
from log import logger
from app.vectors.embed_data import embed_parquet

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"
SUMMARY_BUCKET = "openai-data-hub-ai"

s3 = boto3.client("s3")


def clean_file(key):
    """
    Cleans a raw CSV file, validates it, converts to Parquet,
    generates AI summary, and generates embeddings.
    """

    logger.info(f"Starting cleaning for: {key}")

    # ---------------------------------------------------------
    # 1. LOAD RAW FILE FROM S3
    # ---------------------------------------------------------
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    df = pd.read_csv(obj["Body"])

    # ---------------------------------------------------------
    # 2. VALIDATION
    # ---------------------------------------------------------
    df, issues = validate_dataframe(df)
    logger.info(f"Validation issues for {key}: {issues}")

    if df.empty:
        logger.warning(f"{key}: File is empty. Skipping cleaning.")
        return False

    # ---------------------------------------------------------
    # 3. CLEANING
    # ---------------------------------------------------------
    df = df.dropna(how="all")    # drop fully empty rows
    df = df.fillna("")           # fill remaining missing
    df = df.drop_duplicates()    # remove duplicate rows

    # ---------------------------------------------------------
    # 4. WRITE PARQUET (fastparquet)
    # ---------------------------------------------------------
    out_buffer = BytesIO()
    parquet_key = key.replace(".csv", ".parquet")

    df.to_parquet(out_buffer, index=False, engine="fastparquet")

    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=parquet_key,
        Body=out_buffer.getvalue()
    )
    logger.info(f"Clean parquet uploaded to: {parquet_key}")

    # ---------------------------------------------------------
    # 5. AI SUMMARY
    # ---------------------------------------------------------
    try:
        summary_text = summarize_dataframe(df)
        summary_key = key.replace(".csv", ".txt")

        s3.put_object(
            Bucket=SUMMARY_BUCKET,
            Key=summary_key,
            Body=summary_text.encode("utf-8")
        )

        logger.info(f"AI summary uploaded to: {summary_key}")

    except Exception as e:
        logger.error(f"Error generating AI summary for {key}: {e}")

    # ---------------------------------------------------------
    # 6. GENERATE VECTOR EMBEDDINGS
    # ---------------------------------------------------------
    try:
        embed_parquet(parquet_key)
        logger.info(f"Embeddings generated for {parquet_key}")

    except Exception as e:
        logger.error(f"Embedding failed for {parquet_key}: {e}")

    logger.info(f"Cleaning completed for: {key}")

    return True
