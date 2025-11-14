import pandas as pd
import boto3
from io import BytesIO

from etl.validate import validate_dataframe
from etl.summary import summarize_dataframe
from vectors.embed_data import embed_parquet
from log import logger

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"
SUMMARY_BUCKET = "openai-data-hub-ai"

s3 = boto3.client("s3")


def clean_file(key):
    """
    Cleans raw CSV → Parquet
    Generates AI summary
    Generates vector embeddings
    """

    logger.info(f"Starting cleaning for: {key}")

    # Load raw CSV
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    df = pd.read_csv(obj["Body"])

    # Validate
    df, issues = validate_dataframe(df)
    logger.info(f"Validation issues: {issues}")

    if df.empty:
        logger.warning(f"{key}: Empty file. Skipping.")
        return False

    # Clean
    df = df.dropna(how="all")
    df = df.fillna("")
    df = df.drop_duplicates()

    # Convert to Parquet
    out_buffer = BytesIO()
    parquet_key = key.replace(".csv", ".parquet")
    df.to_parquet(out_buffer, index=False, engine="fastparquet")

    # Upload
    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=parquet_key,
        Body=out_buffer.getvalue()
    )
    logger.info(f"Uploaded parquet: {parquet_key}")

    # AI Summary
    try:
        summary_text = summarize_dataframe(df)
        summary_key = key.replace(".csv", ".txt")

        s3.put_object(
            Bucket=SUMMARY_BUCKET,
            Key=summary_key,
            Body=summary_text.encode("utf-8")
        )
        logger.info(f"Uploaded summary: {summary_key}")

    except Exception as e:
        logger.error(f"Summary error for {key}: {e}")

    # Vector Embeddings
    try:
        embed_parquet(parquet_key)
        logger.info(f"Embeddings OK for {parquet_key}")
    except Exception as e:
        logger.error(f"Embedding failed for {parquet_key}: {e}")

    logger.info(f"Finished cleaning for {key}")
    return True
