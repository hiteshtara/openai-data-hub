import pandas as pd
import openai
import json

from log import logger

openai.api_key = None  # systemd injects ENV variable


def summarize_dataframe(df: pd.DataFrame):
    """
    Uses OpenAI to generate a smart summary of the dataset.
    """

    # Basic metadata
    rows = len(df)
    cols = list(df.columns)
    col_count = len(cols)

    # Quick stats
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    sample = df.head(5).to_dict(orient="records")

    prompt = f"""
    You are a data analyst. Summarize this dataset clearly.

    - Number of rows: {rows}
    - Number of columns: {col_count}
    - Column names: {cols}
    - Numeric columns: {numeric_cols}
    - First rows sample: {json.dumps(sample)}

    Provide:
    1. High-level summary
    2. Interesting patterns
    3. Missing data or issues
    4. Outlier or anomaly suggestions
    5. Any insights worth highlighting
    """

    logger.info("Sending summary request to OpenAI...")

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300
    )

    summary = response["choices"][0]["message"]["content"]
    logger.info("Summary generated.")

    return summary
