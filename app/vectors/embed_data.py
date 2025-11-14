import os
import json
import boto3
import pandas as pd
from io import BytesIO
import openai
import numpy as np

from vectors.vector_store import add_vectors
from log import logger

openai.api_key = os.getenv("OPENAI_API_KEY")

s3 = boto3.client("s3", region_name="us-east-1")
CLEAN_BUCKET = "openai-data-hub-clean"

def embed_parquet(key: str):
    try:
        obj = s3.get_object(Bucket=CLEAN_BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
    except Exception as e:
        logger.error(f"Failed to load parquet {key}: {e}")
        return False

    rows = df.to_dict(orient="records")
    texts = [json.dumps(r) for r in rows]

    try:
        resp = openai.Embedding.create(
            model="text-embedding-3-small",
            input=texts
        )
        embeddings = [d["embedding"] for d in resp["data"]]
    except Exception as e:
        logger.error(f"Embedding failed for {key}: {e}")
        return False

    add_vectors(embeddings, texts)

    logger.info(f"FAISS embeddings added for {key}")
    return True
