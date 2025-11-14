import pandas as pd
import openai
import json
from log import logger

# Systemd injects OPENAI_API_KEY as environment variable
openai.api_key = None

def summarize_dataframe(df: pd.DataFrame):
    """
    Generates an OpenAI summary of the cleaned dataset.
    """

    # Basic metadata
    rows = len(df)
    columns = list(df.columns)
    col_count = len(columns)

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    sample = df.head(5).to_dict(orient="records")

    prompt = f"""
    You are a data analyst. Summarize this dataset.

    Metadata:
    - Rows: {rows}
    - Columns: {col_count}
    - Column names: {columns}
    - Numeric columns: {numeric_cols}

    Sample:
    {json.dumps(sample)}

    Provide:
    1. High-level summary
    2. Patterns and trends
    3. Data quality notes
    4. Any interesting insights
    """

    logger.info("Requesting summary from OpenAI...")

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=350
    )

    summary = response["choices"][0]["message"]["content"]

    logger.info("Summary received.")

    return summary
