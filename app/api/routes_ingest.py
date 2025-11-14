from fastapi import APIRouter
router=APIRouter()
@router.post('/csv')
def ingest(): return {'msg':'ingest-prod'}