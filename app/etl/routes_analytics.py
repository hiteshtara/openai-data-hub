from fastapi import APIRouter
import boto3
from etl.monthly import compute_monthly
from etl.categories import compute_category_totals
from etl.anomalies import detect_anomalies
from etl.recurring import detect_recurring

router = APIRouter()
CLEAN_BUCKET = "openai-data-hub-clean"
s3 = boto3.client("s3", region_name="us-east-1")

def latest_parquet():
    resp = s3.list_objects_v2(Bucket=CLEAN_BUCKET)
    files = [x["Key"] for x in resp["Contents"] if x["Key"].endswith(".parquet")]
    return sorted(files)[-1]

@router.get("/analytics/all")
def full_analytics():
    key = latest_parquet()

    monthly, df1 = compute_monthly(key)
    categories, df2 = compute_category_totals(key)
    anomalies = detect_anomalies(key)
    recurring = detect_recurring(key)

    return {
        "monthly": monthly,
        "categories": categories,
        "anomalies": anomalies,
        "recurring": recurring,
    }

@router.get("/analytics/monthly")
def monthly():
    key = latest_parquet()
    result, _ = compute_monthly(key)
    return result

@router.get("/analytics/categories")
def categories():
    key = latest_parquet()
    result, _ = compute_category_totals(key)
    return result

@router.get("/analytics/anomalies")
def anomalies():
    key = latest_parquet()
    return detect_anomalies(key)

@router.get("/analytics/recurring")
def recurring():
    key = latest_parquet()
    return detect_recurring(key)
