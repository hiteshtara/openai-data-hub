# /opt/openai-data-hub/app/api/routes_analytics.py
from fastapi import APIRouter
from app.analytics.compute import (
    compute_summary,
    compute_vendors,
    compute_categories,
    compute_monthly,
    compute_anomalies,
)

router = APIRouter()


@router.get("/summary")
def summary():
    return compute_summary()


@router.get("/vendors")
def vendors():
    return compute_vendors()


@router.get("/categories")
def categories():
    return compute_categories()


@router.get("/monthly")
def monthly():
    return compute_monthly()


@router.get("/anomalies")
def anomalies():
    return compute_anomalies()
