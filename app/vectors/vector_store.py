import chromadb
from chromadb.config import Settings

# Persistent vector database stored on EC2
def get_vector_db():
    return chromadb.Client(
        Settings(
            chroma_db_impl="duckdb+parquet",
            persist_directory="/opt/openai-data-hub/vectors"
        )
    )

def get_collection():
    db = get_vector_db()
    return db.get_or_create_collection(
        name="etl_vectors",
        metadata={"hnsw:space": "cosine"}  # cosine similarity
    )
