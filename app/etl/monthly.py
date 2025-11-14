import pandas as pd
import boto3
from io import BytesIO

CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3", region_name="us-east-1")

def compute_monthly(clean_key: str):
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=clean_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.to_period("M").astype(str)

    monthly = (
        df.groupby("month")["amount"]
        .sum()
        .reset_index()
        .sort_values("month")
    )

    return {row["month"]: float(row["amount"]) for _, row in monthly.iterrows()}, df
