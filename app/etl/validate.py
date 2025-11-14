import pandas as pd

def validate_dataframe(df: pd.DataFrame):
    """
    Basic validation for uploaded datasets.
    You can expand this over time.
    """

    issues = []

    # 1. Check empty file
    if df.empty:
        issues.append("File is empty.")

    # 2. Check for duplicate columns
    if df.columns.duplicated().any():
        issues.append("Duplicate column names found.")

    # 3. Check for too many nulls
    null_ratio = df.isna().mean()
    high_null_cols = null_ratio[null_ratio > 0.5].index.tolist()
    if high_null_cols:
        issues.append(f"Columns with over 50% missing values: {high_null_cols}")

    # 4. Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    return df, issues
