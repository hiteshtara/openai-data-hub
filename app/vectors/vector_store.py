import faiss
import numpy as np
import os
import pickle

VECTOR_DIR = "/opt/openai-data-hub/vectors"
os.makedirs(VECTOR_DIR, exist_ok=True)

INDEX_FILE = f"{VECTOR_DIR}/faiss.index"
META_FILE = f"{VECTOR_DIR}/metadata.pkl"

def load_index(dims):
    """Load FAISS index or create new one."""
    if os.path.exists(INDEX_FILE):
        return faiss.read_index(INDEX_FILE)
    return faiss.IndexFlatL2(dims)

def save_index(index):
    faiss.write_index(index, INDEX_FILE)

def load_metadata():
    if os.path.exists(META_FILE):
        return pickle.load(open(META_FILE, "rb"))
    return []

def save_metadata(meta):
    pickle.dump(meta, open(META_FILE, "wb"))

def add_vectors(embeddings, documents):
    """Add vectors + docs into FAISS index."""
    embeddings = np.array(embeddings).astype("float32")

    meta = load_metadata()
    start_id = len(meta)
    new_meta = meta + documents
    save_metadata(new_meta)

    index = load_index(embeddings.shape[1])
    index.add(embeddings)
    save_index(index)

def query_vectors(query_emb, k=5):
    """Search vectors."""
    index = load_index(query_emb.shape[0])
    query = np.array([query_emb]).astype("float32")

    D, I = index.search(query, k)

    meta = load_metadata()
    results = []
    for idx in I[0]:
        if idx < len(meta):
            results.append(meta[idx])
    return results
