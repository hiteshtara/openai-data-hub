from fastapi import APIRouter

router = APIRouter()

@router.get("/analytics/test")
def analytics_test():
    return {"message": "analytics router working"}
