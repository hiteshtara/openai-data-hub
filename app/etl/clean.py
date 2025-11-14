import pandas as pd
import boto3
from io import BytesIO

from validate import validate_dataframe

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3")

def clean_file(key):
    # Read raw CSV
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    df = pd.read_csv(obj["Body"])

    # Validate
    df, issues = validate_dataframe(df)

    print("Validation issues:", issues)

    # 1. Drop rows completely empty
    df = df.dropna(how="all")

    # 2. Fill missing values
    df = df.fillna("")

    # 3. Remove duplicates
    df = df.drop_duplicates()

    # Convert to parquet
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False, engine="fastparquet")

    # Upload clean file
    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=key.replace(".csv", ".parquet"),
        Body=out_buffer.getvalue(),
    )

    return True
