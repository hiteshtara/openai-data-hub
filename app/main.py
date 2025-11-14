from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# Import your routers
from app.api.routes_etl import router as etl_router
from app.api.routes_ai_rag import router as rag_router
from app.api.routes_totals import router as totals_router
from app.api.routes_analytics import router as analytics_router

from app.log import logger


# ---------------------------------------------------------
#   FASTAPI APP INITIALIZATION
# ---------------------------------------------------------

app = FastAPI()


# ---------------------------------------------------------
#   BASIC HOME ROUTE
# ---------------------------------------------------------

@app.get("/")
def home():
    return {"status": "prod-ready"}


# ---------------------------------------------------------
#   API ROUTERS
# ---------------------------------------------------------

# ETL endpoints
app.include_router(etl_router, prefix="/etl")

# AI / RAG (vector search + LLM)
app.include_router(rag_router, prefix="/ai")

# Totals (vendor, category, etc.)
app.include_router(totals_router, prefix="/etl")

# Analytics (monthly, categories, anomalies)
app.include_router(analytics_router, prefix="/")


# ---------------------------------------------------------
#   DASHBOARD STATIC FILE MOUNT
# ---------------------------------------------------------

# Serve Next.js static build from /dashboard
app.mount(
    "/dashboard",
    StaticFiles(directory="/opt/openai-data-hub/dashboard/out", html=True),
    name="dashboard",
)
