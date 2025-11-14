from fastapi import FastAPI
from api.routes_etl import router as etl_router

app=FastAPI()
@app.get('/')
def home(): return {'status':'prod-ready'}
app.include_router(etl_router, prefix="/etl")
