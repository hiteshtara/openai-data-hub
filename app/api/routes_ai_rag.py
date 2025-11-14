from fastapi import APIRouter, Query
import openai, os
import numpy as np

from vectors.vector_store import query_vectors

router = APIRouter()
openai.api_key = os.getenv("OPENAI_API_KEY")

@router.get("/query")
def rag_query(question: str = Query(...)):
    try:
        emb = openai.Embedding.create(
            model="text-embedding-3-small",
            input=question
        )["data"][0]["embedding"]

        results = query_vectors(np.array(emb, dtype="float32"))

        prompt = f"Context: {results}\n\nQuestion: {question}\nAnswer:"
        answer = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )["choices"][0]["message"]["content"]

        return {
            "answer": answer,
            "context": results
        }

    except Exception as e:
        return {"error": str(e)}
