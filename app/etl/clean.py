import pandas as pd
import boto3
import chardet
from io import BytesIO
import os
from app.log import logger
from app.etl.validate import validate_dataframe

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3", region_name="us-east-1")

def is_printable(text):
    if not isinstance(text, str):
        return False
    return all(32 <= ord(c) <= 126 or c in "\n\r\t" for c in text)

def load_xlsx(raw_bytes):
    """Load ALL sheets from an XLSX into one DataFrame."""
    try:
        excel = pd.ExcelFile(BytesIO(raw_bytes))
        frames = []
        for sheet in excel.sheet_names:
            df = excel.parse(sheet)
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    except Exception as e:
        logger.error(f"XLSX parse error: {e}")
        return None

def load_csv(raw_bytes):
    """Load CSV safely."""
    try:
        enc = chardet.detect(raw_bytes)["encoding"] or "utf-8"
        return pd.read_csv(BytesIO(raw_bytes), encoding=enc, on_bad_lines="skip", engine="python")
    except Exception as e:
        logger.error(f"CSV parse error: {e}")
        return None

def clean_file(key: str):
    logger.info(f"Cleaning: {key}")

    try:
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
    except Exception as e:
        logger.error(f"Cannot read raw file {key}: {e}")
        return None

    if key.lower().endswith(".xlsx"):
        df = load_xlsx(raw_bytes)
    elif key.lower().endswith(".csv"):
        df = load_csv(raw_bytes)
    else:
        logger.error(f"Unsupported file type: {key}")
        return None

    if df is None or df.empty:
        logger.warning(f"No useful data in {key}")
        return None

    # Normalize & filter
    df = df.astype(str)
    df = df.dropna(how="all")

    cleaned_rows = []
    for _, row in df.iterrows():
        row_str = " ".join(row.values)
        if is_printable(row_str):
            cleaned_rows.append(row.to_dict())

    if not cleaned_rows:
        logger.warning(f"All rows removed: {key}")
        return None

    clean_df = pd.DataFrame(cleaned_rows)
    validate_dataframe(clean_df)

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

        logger.info(f"Clean parquet saved: {out_key}")
        return out_key

    except Exception as e:
        logger.error(f"Failed to upload cleaned file: {e}")
        return None
