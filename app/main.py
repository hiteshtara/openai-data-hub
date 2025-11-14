from fastapi import FastAPI
from api.routes_etl import router as etl_router
from api.routes_summary import router as summary_router


app=FastAPI()
@app.get('/')
def home(): return {'status':'prod-ready'}
app.include_router(etl_router, prefix="/etl")
app.include_router(summary_router, prefix="/etl")