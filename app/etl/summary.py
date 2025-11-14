import pandas as pd
import json
import openai
import os
from log import logger

# load key INSIDE code so sudo ETL works
openai.api_key = os.getenv("OPENAI_API_KEY")


def summarize_dataframe(df: pd.DataFrame):
    """Generate AI summary with OpenAI 0.28."""

    # Failsafe: ensure key exists
    if not openai.api_key:
        logger.error("SUMMARY: OPENAI_API_KEY is missing inside Python.")
        return "ERROR: No API key found inside summary.py"

    rows = len(df)
    cols = list(df.columns)
    sample = df.head(5).to_dict(orient="records")

    prompt = f"""
    Summarize this dataset.

    Rows: {rows}
    Columns: {cols}
    Sample: {json.dumps(sample)}

    Provide:
    - Overview
    - Trends
    - Data issues
    - Insights
    """

    try:
        r = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        return r["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"SUMMARY ERROR: {e}")
        return f"ERROR: {e}"
