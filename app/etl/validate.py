import pandas as pd

def validate_dataframe(df: pd.DataFrame):
    """
    Validates the uploaded dataset and returns:
    - normalized df
    - list of issues
    """

    issues = []

    # Check empty file
    if df.empty:
        issues.append("File is empty.")

    # Check duplicate columns
    if df.columns.duplicated().any():
        issues.append("Duplicate column names found.")

    # Check missing values ratio
    null_ratio = df.isna().mean()
    high_null_cols = null_ratio[null_ratio > 0.5].index.tolist()

    if high_null_cols:
        issues.append(f"Columns with over 50% missing values: {high_null_cols}")

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    return df, issues
