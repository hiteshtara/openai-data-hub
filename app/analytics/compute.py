# /opt/openai-data-hub/app/analytics/compute.py
import pandas as pd
import numpy as np
from app.analytics.loader import load_cleaned_df
from app.log import logger


def detect_vendor_column(df):
    candidates = ["vendor", "merchant", "payee", "description"]
    for c in df.columns:
        if any(x in c for x in candidates):
            return c
    return None


def detect_amount_column(df):
    for c in df.columns:
        if "amount" in c:
            return c
    return None


def detect_category_column(df):
    for c in df.columns:
        if "category" in c:
            return c
    return None


def detect_date_column(df):
    for c in df.columns:
        if "date" in c:
            return c
    return None


def compute_summary():
    df = load_cleaned_df()
    if df.empty:
        return {}

    amt = detect_amount_column(df)
    if not amt:
        return {"error": "No amount column detected"}

    total_exp = df[df[amt] < 0][amt].sum()
    total_inc = df[df[amt] > 0][amt].sum()
    net = df[amt].sum()

    vendor_col = detect_vendor_column(df)
    category_col = detect_category_column(df)

    summary = {
        "total_income": float(round(total_inc, 2)),
        "total_expense": float(round(total_exp, 2)),
        "net": float(round(net, 2)),
        "vendor_count": int(df[vendor_col].nunique()) if vendor_col else None,
        "category_count": int(df[category_col].nunique()) if category_col else None,
        "rows": len(df),
    }

    return summary


def compute_vendors():
    df = load_cleaned_df()
    vendor_col = detect_vendor_column(df)
    amt = detect_amount_column(df)

    if not vendor_col or not amt:
        return []

    table = (
        df.groupby(vendor_col)[amt]
        .sum()
        .sort_values(ascending=True)
        .reset_index()
    )
    return table.to_dict(orient="records")


def compute_categories():
    df = load_cleaned_df()
    cat_col = detect_category_column(df)
    amt = detect_amount_column(df)

    if not cat_col or not amt:
        return []

    table = (
        df.groupby(cat_col)[amt]
        .sum()
        .sort_values()
        .reset_index()
    )
    return table.to_dict(orient="records")


def compute_monthly():
    df = load_cleaned_df()
    amt = detect_amount_column(df)
    date_col = detect_date_column(df)

    if not date_col or not amt:
        return []

    df["month"] = df[date_col].dt.to_period("M").astype(str)

    table = (
        df.groupby("month")[amt]
        .sum()
        .reset_index()
        .sort_values("month")
    )

    return table.to_dict(orient="records")


def compute_anomalies():
    df = load_cleaned_df()
    amt = detect_amount_column(df)

    if not amt:
        return []

    df = df.copy()
    df["zscore"] = (df[amt] - df[amt].mean()) / df[amt].std()

    anomalies = df[abs(df["zscore"]) > 2.5]

    return anomalies.to_dict(orient="records")
