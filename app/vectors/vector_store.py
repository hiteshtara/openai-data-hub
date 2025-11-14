import chromadb
from chromadb.config import Settings

# Works with Chroma 0.4.22
def get_collection():
    client = chromadb.Client(
        Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory="/opt/openai-data-hub/vectors"
        )
    )
    return client.get_or_create_collection(
        name="etl_vectors",
        metadata={"hnsw:space": "cosine"}
    )
