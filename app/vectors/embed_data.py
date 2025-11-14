import os
import json
import boto3
import pandas as pd
from io import BytesIO
import openai

from vectors.vector_store import get_collection
from log import logger

# Load API key inside script (sudo-safe)
openai.api_key = os.getenv("OPENAI_API_KEY")

s3 = boto3.client("s3")
CLEAN_BUCKET = "openai-data-hub-clean"


def embed_parquet(key: str):
    """
    Generate embeddings for each row in the cleaned parquet file.
    Uses OpenAI Embeddings (v0.28.0 API)
    """

    if not openai.api_key:
        logger.error("OpenAI key missing inside embed_data.py")
        return False

    logger.info(f"Embedding data for: {key}")

    # ---------------------------------------------------------
    # 1. Load Parquet safely from S3 (fix seek errors)
    # ---------------------------------------------------------
    try:
        obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
        df = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as e:
        logger.error(f"Could not load parquet {key}: {e}")
        return False

    rows = df.to_dict(orient="records")
    collection = get_collection()

    # ---------------------------------------------------------
    # 2. Embed each row
    # ---------------------------------------------------------
    for i, row in enumerate(rows):
        text = json.dumps(row)

        try:
            emb = openai.Embedding.create(
                model="text-embedding-3-small",
                input=text
            )["data"][0]["embedding"]

            collection.add(
                ids=[f"{key}_{i}"],
                embeddings=[emb],
                documents=[text]
            )

        except Exception as e:
            logger.error(f"Embedding failed for row {i} in {key}: {e}")
            continue

    logger.info(f"Embedding complete for: {key}")
    return True
