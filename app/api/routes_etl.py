from fastapi import APIRouter, UploadFile, File
import boto3
import uuid

router = APIRouter()
s3 = boto3.client("s3")

RAW_BUCKET = "openai-data-hub-raw"

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_id = f"{uuid.uuid4()}.csv"
    s3.upload_fileobj(file.file, RAW_BUCKET, file_id)
    return {"status": "uploaded", "file_id": file_id}
