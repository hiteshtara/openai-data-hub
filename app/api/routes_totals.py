from fastapi import APIRouter, Query
import boto3
from app.etl.totals import compute_totals
from app.log import logger

router = APIRouter()

CLEAN_BUCKET = "openai-data-hub-clean"
s3 = boto3.client("s3", region_name="us-east-1")

@router.get("/totals")
def totals(vendor: str = Query(None)):
    try:
        # Find latest cleaned parquet file
        response = s3.list_objects_v2(Bucket=CLEAN_BUCKET)
        if "Contents" not in response:
            return {"error": "No cleaned data available"}

        parquet_files = [x["Key"] for x in response["Contents"] if x["Key"].endswith(".parquet")]
        if not parquet_files:
            return {"error": "No cleaned parquet files"}

        latest = sorted(parquet_files)[-1]   # latest processed file

        totals_dict = compute_totals(latest)

        if vendor:
            vendor = vendor.upper()
            if vendor in totals_dict:
                return {vendor: totals_dict[vendor]}
            else:
                return {"error": f"No totals found for vendor '{vendor}'"}

        return totals_dict

    except Exception as e:
        logger.error(f"Totals error: {e}")
        return {"error": str(e)}
