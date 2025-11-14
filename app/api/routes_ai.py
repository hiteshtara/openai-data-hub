from fastapi import APIRouter
router=APIRouter()
@router.post('/rag')
def rag(): return {'answer':'prod'}