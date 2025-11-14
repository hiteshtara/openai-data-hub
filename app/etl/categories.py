import pandas as pd
import boto3
from io import BytesIO

CLEAN_BUCKET = "openai-data-hub-clean"

s3 = boto3.client("s3", region_name="us-east-1")

CATEGORY_MAP = {
    "VERIZON": "TELECOM",
    "ATT": "TELECOM",
    "AT&T": "TELECOM",
    "T-MOBILE": "TELECOM",
    "COMCAST": "TELECOM",

    "EVERSOURCE": "UTILITIES",
    "NATIONAL GRID": "UTILITIES",
    "WATER": "UTILITIES",
    "GAS": "UTILITIES",

    "UBER EATS": "RESTAURANTS",
    "DOORDASH": "RESTAURANTS",
    "MCDONALD": "RESTAURANTS",

    "ADP": "PAYROLL",
    "SQUARE PAYROLL": "PAYROLL",

    "CHASE LOAN": "LOAN",
    "WELLS FARGO": "LOAN",

    "FEE": "FEES",
    "SERVICE CHARGE": "FEES",

    "TRANSFER": "TRANSFER",
    "ZELLE": "TRANSFER",
}

def categorize(vendor: str) -> str:
    if vendor is None:
        return "OTHER"
    vendor = vendor.upper().strip()

    for key, cat in CATEGORY_MAP.items():
        if key in vendor:
            return cat

    return "OTHER"

def compute_category_totals(clean_key: str):
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=clean_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    df["category"] = df["vendor"].apply(categorize)

    results = (
        df.groupby("category")["amount"]
        .sum()
        .reset_index()
        .sort_values("amount")
    )

    out = {}
    for _, row in results.iterrows():
        out[row["category"]] = float(row["amount"])

    return out, df
