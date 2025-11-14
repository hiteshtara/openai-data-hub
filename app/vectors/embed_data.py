import pandas as pd
import openai
import json
import boto3
from vector_store import get_collection
from log import logger

openai.api_key = None  # systemd injects API key

s3 = boto3.client("s3")
CLEAN_BUCKET = "openai-data-hub-clean"


def embed_parquet(key):
    """
    Create embeddings for each row of a cleaned parquet dataset.
    Stores them in ChromaDB.
    """

    logger.info(f"Embedding data for: {key}")

    # Load parquet from S3
    obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=key)
    df = pd.read_parquet(obj["Body"])

    rows = df.to_dict(orient="records")

    collection = get_collection()

    for i, row in enumerate(rows):
        text = json.dumps(row)

        # Generate embedding using OpenAI
        emb = openai.Embedding.create(
            model="text-embedding-3-small",
            input=text
        )["data"][0]["embedding"]

        # Store in vector DB
        collection.add(
            ids=[f"{key}_{i}"],
            embeddings=[emb],
            documents=[text]
        )

    logger.info(f"Embedding complete for {key}")
