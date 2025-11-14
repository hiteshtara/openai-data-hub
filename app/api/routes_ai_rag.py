from fastapi import APIRouter, Query
import openai, os
import numpy as np

from vectors.vector_store import query_vectors, load_index, load_metadata

router = APIRouter()
openai.api_key = os.getenv("OPENAI_API_KEY")

@router.get("/query")
def rag_query(question: str = Query(...)):
    try:
        # Embed query
        emb = openai.Embedding.create(
            model="text-embedding-3-small",
            input=question
        )["data"][0]["embedding"]
        emb = np.array(emb).astype("float32")

        # Check if FAISS is empty
        meta = load_metadata()
        if len(meta) == 0:
            # Return direct GPT answer if no context exists
            answer = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[{
                    "role": "user",
                    "content": f"No stored context yet. Answer directly:\n\n{question}"
                }]
            )["choices"][0]["message"]["content"]

            return {"answer": answer, "context": []}

        # Query FAISS safely
        results = query_vectors(emb, k=5)

        # If nothing returned from FAISS
        if not results:
            answer = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": question}]
            )["choices"][0]["message"]["content"]
            return {"answer": answer, "context": []}

        # Use RAG-style answer
        prompt = f"Context: {results}\n\nQuestion: {question}\nAnswer:"
        answer = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )["choices"][0]["message"]["content"]

        return {"answer": answer, "context": results}

    except Exception as e:
        return {"error": str(e)}
