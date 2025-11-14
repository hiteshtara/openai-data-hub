import pandas as pd
import openai
import json
import boto3
from io import BytesIO

from vectors.vector_store import get_collection
from log import logger

openai.api_key = None  # systemd provides OPENAI_API_KEY

s3 = boto3.client("s3")

CLEAN_BUCKET = "openai-data-hub-clean"


def embed_parquet(key):
    """
    Generates vector embeddings for each row of the cleaned parquet file.
    Stores them in persistent ChromaDB.
    """

    logger.info(f"Embedding data for: {key}")

    # ---------------------------------------------------------
    # 1. Load parquet file SAFELY from S3 (fix for seek error)
    # ---------------------------------------------------------
    try:
        obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=key)
        raw_bytes = obj["Body"].read()
        df = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as e:
        logger.error(f"Failed to load parquet file {key}: {e}")
        return False

    # Convert each row into a document
    rows = df.to_dict(orient="records")

    # ---------------------------------------------------------
    # 2. Connect to Chroma vector store
    # ---------------------------------------------------------
    collection = get_collection()

    # ---------------------------------------------------------
    # 3. Embed each row using OpenAI Embeddings API (v0.28)
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

    logger.info(f"Embedding complete for {key}")
    return True
