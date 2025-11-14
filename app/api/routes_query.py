from fastapi import APIRouter
router=APIRouter()
@router.get('/data')
def query(): return {'data':[]} 