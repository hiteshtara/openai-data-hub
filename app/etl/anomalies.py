import pandas as pd
import boto3
from io import BytesIO

CLEAN_BUCKET = "openai-data-hub-clean"
s3 = boto3.client("s3", region_name="us-east-1")

def detect_anomalies(clean_key: str):
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=clean_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    anomalies = []

    # Rule 1: Large transactions
    large = df[df["amount"].abs() > 1000]
    for _, row in large.iterrows():
        anomalies.append({"reason": "LARGE_TRANSACTION", **row.to_dict()})

    # Rule 2: Vendor never seen before
    vendor_counts = df["vendor"].value_counts()
    new_vendors = vendor_counts[vendor_counts == 1].index

    for vendor in new_vendors:
        rows = df[df["vendor"] == vendor]
        for _, row in rows.iterrows():
            anomalies.append({"reason": "NEW_VENDOR", **row.to_dict()})

    # Rule 3: Sudden balance drop
    df_sorted = df.sort_values("date")
    df_sorted["prev_balance"] = df_sorted["balance"].shift(1)
    df_sorted["drop"] = df_sorted["prev_balance"] - df_sorted["balance"]
    sudden = df_sorted[df_sorted["drop"] > 2000]

    for _, row in sudden.iterrows():
        anomalies.append({"reason": "BALANCE_DROP", **row.to_dict()})

    return anomalies
