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
    Handles ANY CSV: UTF-8, Windows-1252, ISO-8859-1
    Skips malformed rows safely
    """

    logger.info(f"Starting cleaning for: {key}")

    # ---------------------------------------------------------
    # 1. LOAD RAW CSV WITH AUTO-ENCODING DETECTION
    # ---------------------------------------------------------
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    raw_bytes = obj["Body"].read()

    # Detect encoding (fixes UnicodeDecodeError)
    detected = chardet.detect(raw_bytes)
    encoding = detected.get("encoding", "utf-8")

    logger.info(f"Detected encoding for {key}: {encoding}")

    # Try loading with detected encoding
    try:
        df = pd.read_csv(
            BytesIO(raw_bytes),
            encoding=encoding,
            on_bad_lines="skip"      # skip malformed rows
        )
    except Exception:
        logger.warning(f"{key}: fallback ISO-8859-1 with bad line skipping.")
        df = pd.read_csv(
            BytesIO(raw_bytes),
            encoding="ISO-8859-1",
            engine="python",
            on_bad_lines="skip"
        )

    # ---------------------------------------------------------
    # 2. VALIDATION
    # ---------------------------------------------------------
    df, issues = validate_dataframe(df)
    logger.info(f"Validation issues: {issues}")

    if df.empty:
        logger.warning(f"{key}: File is empty after parsing. Skipping.")
        return False

    # ---------------------------------------------------------
    # 3. CLEANING PIPELINE
    # ---------------------------------------------------------
    df = df.dropna(how="all")  # drop fully empty rows
    df = df.fillna("")         # fill missing values
    df = df.drop_duplicates()  # remove duplicate rows

    # ---------------------------------------------------------
    # 4. WRITE CLEAN PARQUET (fastparquet)
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

        logger.info(f"Uploaded AI summary: {summary_key}")

    except Exception as e:
        logger.error(f"AI summary failed for {key}: {e}")

    # ---------------------------------------------------------
    # 6. VECTOR EMBEDDINGS (RAG)
    # ---------------------------------------------------------
    try:
        embed_parquet(parquet_key)
        logger.info(f"Embeddings generated for: {parquet_key}")

    except Exception as e:
        logger.error(f"Embedding failed for {parquet_key}: {e}")

    logger.info(f"Finished cleaning for: {key}")
    return True
