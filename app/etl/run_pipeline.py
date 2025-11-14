import boto3
from clean import clean_file

RAW_BUCKET = "openai-data-hub-raw"
CLEAN_BUCKET = "openai-data-hub-clean"


s3 = boto3.client("s3")

def run_pipeline():
    response = s3.list_objects_v2(Bucket=RAW_BUCKET)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            print(f"Cleaning {key}...")
            clean_file(key)

if __name__ == "__main__":
    run_pipeline()
