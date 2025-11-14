import pandas as pd
import boto3
from io import BytesIO
import chardet  # encoding detection

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

    # ---------------------------------------------------------
    # 1. LOAD RAW FILE FROM S3 WITH AUTO-ENCODING DETECTION
    # ---------------------------------------------------------
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)

    raw_bytes = obj["Body"].read()

    # Detect encoding (avoids UnicodeDecodeError)
    detected = chardet.detect(raw_bytes)
    encoding = detected.get("encoding", "utf-8")

    logger.info(f"Detected encoding for {key}: {encoding}")

    try:
        df = pd.read_csv(BytesIO(raw_bytes), encoding=encoding)
    except Exception:
        # Fallback for Windows/Excel CSV files
        logger.warning(f"{key}: Falling back to ISO-8859-1 encoding.")
        df = pd.read_csv(BytesIO(raw_bytes), encoding="ISO-8859-1", engine="python")

    # ---------------------------------------------------------
    # 2. VALIDATION
    # ---------------------------------------------------------
    df, issues = validate_dataframe(df)
    logger.info(f"Validation issues: {issues}")

    if df.empty:
        logger.warning(f"{key}: Empty file. Skipping.")
        return False

    # ---------------------------------------------------------
    # 3. CLEANING
    # ---------------------------------------------------------
    # Drop empty rows
    df = df.dropna(how="all")

    # Replace NaN with blank
    df = df.fillna("")

    # Remove duplicate rows
    df = df.drop_duplicates()

    # ---------------------------------------------------------
    # 4. CONVERT TO PARQUET (fastparquet)
    # ---------------------------------------------------------
    out_buffer = BytesIO()
    parquet_key = key.replace(".csv", ".parquet")

    df.to_parquet(out_buffer, index=False, engine="fastparquet")

    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=parquet_key,
        Body=out_buffer.getvalue()
    )
    logger.info(f"Uploaded cleaned parquet: {parquet_key}")

    # ---------------------------------------------------------
    # 5. AI SUMMARY (OpenAI GPT)
    # ---------------------------------------------------------
    try:
        summary_text = summarize_dataframe(df)
        summary_key = key.replace(".csv", ".txt")

        s3.put_object(
            Bucket=SUMMARY_BUCKET,
            Key=summary_key,
            Body=summary_text.encode("utf-8")
        )
        logger.info(f"Uploaded AI summary: {summary_key}")

    except Exception as e:
        logger.error(f"Summary error for {key}: {e}")

    # ---------------------------------------------------------
    # 6. VECTOR EMBEDDINGS (RAG)
    # ---------------------------------------------------------
    try:
        embed_parquet(parquet_key)
        logger.info(f"Embeddings generated for {parquet_key}")
    except Exception as e:
        logger.error(f"Embedding failed for {parquet_key}: {e}")

    logger.info(f"Finished cleaning for: {key}")
    return True
