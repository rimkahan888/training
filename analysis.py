import duckdb
import pandas as pd
from config import DUCKDB_FILE

conn = duckdb.connect(DUCKDB_FILE)


def get_sentiment_trends():
    """Returns sentiment trends over time"""
    return conn.execute("""
        SELECT CAST(created_at AS DATE) AS date, AVG(sentiment_score) as avg_sentiment
        FROM duckdb_issues.issues
        GROUP BY date
        ORDER BY date
    """).fetchdf()


if __name__ == "__main__":
    print(get_sentiment_trends())
