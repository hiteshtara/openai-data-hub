from fastapi import APIRouter, Query
from vectors.vector_store import get_collection
import openai, os

router = APIRouter()

openai.api_key = os.getenv("OPENAI_API_KEY")

@router.get("/query")
def rag_query(question: str = Query(...)):
    try:
        collection = get_collection()

        # Fetch vector embeddings for your question
        emb = openai.Embedding.create(
            model="text-embedding-3-small",
            input=question
        )["data"][0]["embedding"]

        # Query vector database
        results = collection.query(
            query_embeddings=[emb],
            n_results=5
        )

        docs = results.get("documents", [[]])[0]

        # Generate answer using GPT
        prompt = f"Use these documents as context:\n{docs}\n\nQuestion: {question}"
        answer = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )["choices"][0]["message"]["content"]

        return {
            "answer": answer,
            "context_docs": docs
        }

    except Exception as e:
        return {"error": str(e)}
