import pandas as pd
import boto3
from io import BytesIO

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3")

def clean_file(key):
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    df = pd.read_csv(obj["Body"])

    df = df.dropna(how="all")
    df = df.fillna("")

    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)

    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=key.replace(".csv", ".parquet"),
        Body=out_buffer.getvalue()
    )

    return True
