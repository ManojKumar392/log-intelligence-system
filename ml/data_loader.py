import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

POSTGRES_URL = "postgresql://loguser:logpass@localhost:5432/log_intelligence"

def load_logs(limit: int | None = None) -> pd.DataFrame:
    """
    Load logs from Postgres into a pandas DataFrame
    """

    engine = create_engine(POSTGRES_URL)

    query = """
        SELECT
            timestamp,
            service,
            level,
            severity,
            message,
            metadata,
            processed_at
        FROM logs
        ORDER BY processed_at DESC
    """

    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql(query, engine)

    # ---- basic cleaning ----
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")

    df["service"] = df["service"].fillna("unknown")
    df["level"] = df["level"].fillna("UNKNOWN")
    df["message"] = df["message"].fillna("")

    return df


if __name__ == "__main__":
    df = load_logs(limit=50000)
    print(df.head())
    print(df.info())
