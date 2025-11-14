import chromadb
from chromadb.config import Settings
import os

VECTOR_DIR = "/opt/openai-data-hub/vectors"
os.makedirs(VECTOR_DIR, exist_ok=True)

def get_collection():
    client = chromadb.Client(
        Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory=VECTOR_DIR,
        )
    )

    return client.get_or_create_collection(
        name="etl_vectors",
        metadata={"hnsw:space": "cosine"}  # legacy API requirement
    )
