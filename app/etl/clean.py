import pandas as pd
import boto3
import chardet
from io import BytesIO
import os
from log import logger
from etl.validate import validate_dataframe

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3", region_name="us-east-1")

def is_printable(text):
    """Return True if a string contains readable characters."""
    if not isinstance(text, str):
        return False
    return all(32 <= ord(c) <= 126 or c in "\n\r\t" for c in text)

def load_xlsx(raw_bytes):
    """Load XLSX into a clean DataFrame (all sheets combined)."""
    try:
        excel = pd.ExcelFile(BytesIO(raw_bytes))
        frames = [excel.parse(sheet_name=sheet) for sheet in excel.sheet_names]
        df = pd.concat(frames, ignore_index=True)
        return df
    except Exception as e:
        logger.error(f"Failed to parse XLSX: {e}")
        return None

def load_csv(raw_bytes):
    """Load CSV with encoding detection."""
    try:
        enc = chardet.detect(raw_bytes)["encoding"] or "utf-8"
        return pd.read_csv(BytesIO(raw_bytes), encoding=enc, on_bad_lines="skip", engine="python")
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        return None

def clean_file(key: str):
    logger.info(f"Starting cleaning for: {key}")

    try:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
    except Exception as e:
        logger.error(f"Failed to load {key}: {e}")
        return None

    # Load based on extension
    if key.lower().endswith(".xlsx"):
        df = load_xlsx(raw_bytes)
    elif key.lower().endswith(".csv"):
        df = load_csv(raw_bytes)
    else:
        logger.error(f"Unsupported file type for {key}")
        return None

    if df is None or df.empty:
        logger.warning(f"{key} produced no dataframe")
        return None

    df.dropna(how="all", inplace=True)
    df = df.astype(str)

    # Filter printable rows
    cleaned_rows = []
    for _, row in df.iterrows():
        row_str = " ".join(row.values)
        if is_printable(row_str):
            cleaned_rows.append(row.to_dict())

    if len(cleaned_rows) == 0:
        logger.warning(f"All rows removed due to binary content in {key}")
        return None

    clean_df = pd.DataFrame(cleaned_rows)

    issues = validate_dataframe(clean_df)
    logger.info(f"Validation issues: {issues}")

    try:
        buffer = BytesIO()
        clean_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        out_key = key.replace(".xlsx", ".parquet").replace(".csv", ".parquet")

        s3.put_object(
            Bucket=CLEAN_BUCKET,
            Key=out_key,
            Body=buffer.getvalue()
        )

        logger.info(f"Uploaded cleaned parquet: {out_key}")
        return out_key
    except Exception as e:
        logger.error(f"Failed to upload cleaned file {key}: {e}")
        return None
