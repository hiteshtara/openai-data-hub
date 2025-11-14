# /opt/openai-data-hub/app/analytics/loader.py
import boto3
import pandas as pd
import io
from functools import lru_cache
from app.log import logger

BUCKET = "openai-data-hub-clean"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)


@lru_cache(maxsize=1)
def load_cleaned_df():
    """Loads all parquet files from S3 into one cached DataFrame."""
    logger.info("Loading cleaned parquet files from S3 (cached)...")
    objs = s3.list_objects_v2(Bucket=BUCKET)

    if "Contents" not in objs:
        logger.error("No files found in bucket.")
        return pd.DataFrame()

    frames = []
    for item in objs["Contents"]:
        key = item["Key"]
        if key.endswith(".parquet"):
            logger.info(f"Loading parquet: {key}")
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = obj["Body"].read()
            df = pd.read_parquet(io.BytesIO(data))
            frames.append(df)

    if not frames:
        logger.error("No parquet files found in bucket.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure date handling
    date_cols = [c for c in df.columns if "date" in c]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except:
            pass

    logger.info(f"Loaded DataFrame shape: {df.shape}")
    logger.info(f"Columns detected: {list(df.columns)}")

    return df
