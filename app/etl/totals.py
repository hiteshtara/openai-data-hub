import boto3
import pandas as pd
from io import BytesIO
import json

CLEAN_BUCKET = "openai-data-hub-clean"
TOTALS_KEY = "totals.json"

s3 = boto3.client("s3", region_name="us-east-1")

def compute_totals(clean_key: str):
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=clean_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    totals = (
        df.groupby("vendor")["amount"]
        .sum()
        .reset_index()
        .sort_values("amount")
    )

    totals_dict = {
        row["vendor"]: {
            "total": float(row["amount"]),
            "count": int((df["vendor"] == row["vendor"]).sum())
        }
        for _, row in totals.iterrows()
    }

    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=TOTALS_KEY,
        Body=json.dumps(totals_dict, indent=2)
    )

    return totals_dict
