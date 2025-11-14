from fastapi import FastAPI
from app.api.routes_etl import router as etl_router
from app.api.routes_ai_rag import router as rag_router
from app.api.routes_totals import router as totals_router

from app.log import logger




app=FastAPI()
@app.get('/')
def home(): return {'status':'prod-ready'}
app.include_router(etl_router, prefix="/etl")
app.include_router(summary_router, prefix="/summary")
app.include_router(rag_router, prefix="/ai")
app.include_router(totals_router, prefix="/etl")