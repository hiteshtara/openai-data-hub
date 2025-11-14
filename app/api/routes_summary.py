from fastapi import APIRouter
import boto3

router = APIRouter()
s3 = boto3.client("s3")

SUMMARY_BUCKET = "openai-data-hub-ai"

@router.get("/summary/{file_id}")
def get_summary(file_id: str):
    """
    Returns the AI-generated summary for the cleaned dataset.
    """

    key = file_id.replace(".csv", ".txt")

    try:
        obj = s3.get_object(Bucket=SUMMARY_BUCKET, Key=key)
        text = obj["Body"].read().decode("utf-8")
        return {"file_id": file_id, "summary": text}
    except Exception as e:
        return {"error": f"Summary not found: {str(e)}"}
