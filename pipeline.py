import dlt
import duckdb
import pandas as pd
from textblob import TextBlob
from github import github_reactions, github_repo_events, github_stargazers

# Load configurations
from config import GITHUB_REPOS, DUCKDB_FILE

# DuckDB Connection
conn = duckdb.connect(DUCKDB_FILE)


def clean_text(text):
    """Cleans text by removing problematic characters."""
    if not text or pd.isna(text):
        return ""
    return text.replace("\x00", "").replace("'", "''")  # Remove NULL chars and escape single quotes

def analyze_sentiment(text):
    """Perform sentiment analysis on a text"""
    clean_text_val = clean_text(text)  # Sanitize the input first
    if not clean_text_val:
        return 0  # Neutral
    return TextBlob(clean_text_val).sentiment.polarity  # -1 (negative) to 1 (positive)


def load_duckdb_repo_reactions_issues_only() -> None:
    """Loads issues, their comments and reactions for duckdb"""
    pipeline = dlt.pipeline(
        "github_dataset",
        destination='duckdb',
        dataset_name="duckdb_issues"
    )
    # get only 100 items (for issues and pull request)
    data = github_reactions(
        "duckdb", "duckdb", items_per_page=100, max_items=100
    ).with_resources("issues")
    print(pipeline.run(data))


def transform_data():
    """Load raw data and apply transformations like sentiment analysis"""
    query = "SELECT * FROM duckdb_issues.issues"  # Ensure correct table name
    df = conn.execute(query).fetchdf()

    if not df.empty and "body" in df.columns:
        df["sentiment_score"] = df["body"].apply(analyze_sentiment)

        # Write transformed data back to DuckDB
        conn.execute("CREATE OR REPLACE TABLE duckdb_issues.issues AS SELECT * FROM df")
    
    conn.close()


def run_etl():
    # load_duckdb_repo_reactions_issues_only()
    transform_data()
    print("ETL process completed successfully!")


if __name__ == "__main__":
    run_etl()
