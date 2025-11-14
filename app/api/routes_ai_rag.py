from fastapi import APIRouter, Query
import openai, os
import numpy as np

from vectors.vector_store import query_vectors

router = APIRouter()
openai.api_key = os.getenv("OPENAI_API_KEY")

@router.get("/query")
def rag_query(question: str = Query(...)):
    try:
        # Embed the query
        emb = openai.Embedding.create(
            model="text-embedding-3-small",
            input=question
        )["data"][0]["embedding"]

        emb = np.array(emb, dtype="float32")

        # Query FAISS
        results = query_vectors(emb, k=5)

        # If no vectors exist, return direct GPT answer
        if len(results) == 0:
            answer = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": f"No context available. Answer directly:\n\n{question}"
                    }
                ]
            )["choices"][0]["message"]["content"]

            return {"answer": answer, "context": []}

        # If vectors exist, use them
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
