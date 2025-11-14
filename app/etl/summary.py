import pandas as pd
import json
import os
import openai
from log import logger

# Load API key INSIDE the script (important for sudo)
openai.api_key = os.getenv("OPENAI_API_KEY")


def summarize_dataframe(df: pd.DataFrame) -> str:
    """
    Generate an AI summary of the dataset.
    Works with OpenAI v0.28.0
    """

    # Failsafe: ensure key is available
    if not openai.api_key:
        logger.error("OpenAI key missing inside summary.py")
        return "ERROR: Missing OPENAI_API_KEY"

    rows = len(df)
    cols = list(df.columns)
    sample = df.head(5).to_dict(orient="records")

    prompt = f"""
    You are a data analyst.

    The dataset has:
    - {rows} rows
    - {len(cols)} columns
    Columns: {cols}

    Sample data:
    {json.dumps(sample)}

    Provide:
    1. High-level summary
    2. Patterns or trends
    3. Possible data issues
    4. Interesting observations
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300
        )
        return response["choices"][0]["message"]["content"]

    except Exception as e:
        logger.error(f"OpenAI summary error: {e}")
        return f"ERROR: {e}"
