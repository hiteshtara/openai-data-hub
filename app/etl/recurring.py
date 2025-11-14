import boto3
import pandas as pd
from io import BytesIO

CLEAN_BUCKET = "openai-data-hub-clean"
s3 = boto3.client("s3", region_name="us-east-1")

def detect_recurring(clean_key: str):
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=clean_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    recurring = (
        df.groupby("vendor")
        .size()
        .reset_index(name="count")
        .query("count > 2")
    )

    return recurring.to_dict(orient="records")
