from fastapi import APIRouter, Query
import openai
from vectors.vector_store import get_collection


router = APIRouter()
openai.api_key = None  # systemd injects API key


@router.get("/query")
def rag_query(question: str = Query(...)):
    """
    Ask a question and retrieve relevant rows using vector search.
    """

    collection = get_collection()

    # Embed the question
    q_emb = openai.Embedding.create(
        model="text-embedding-3-small",
        input=question
    )["data"][0]["embedding"]

    # Query top 5 similar embeddings
    results = collection.query(
        query_embeddings=[q_emb],
        n_results=5
    )

    documents = results["documents"][0]
    context = "\n".join(documents)

    # Final answer using OpenAI
    answer = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data assistant."},
            {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"}
        ]
    )["choices"][0]["message"]["content"]

    return {
        "question": question,
        "context_docs": documents,
        "answer": answer
    }
