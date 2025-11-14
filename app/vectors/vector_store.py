import faiss
import numpy as np
import os
import pickle

VECTOR_DIR = "/opt/openai-data-hub/vectors"
os.makedirs(VECTOR_DIR, exist_ok=True)

INDEX_FILE = f"{VECTOR_DIR}/faiss.index"
META_FILE = f"{VECTOR_DIR}/metadata.pkl"

def load_index(d):
    if os.path.exists(INDEX_FILE):
        index = faiss.read_index(INDEX_FILE)
    else:
        index = faiss.IndexFlatL2(d)
    return index

def save_index(index):
    faiss.write_index(index, INDEX_FILE)

def load_metadata():
    if os.path.exists(META_FILE):
        return pickle.load(open(META_FILE, "rb"))
    return []

def save_metadata(meta):
    pickle.dump(meta, open(META_FILE, "wb"))
