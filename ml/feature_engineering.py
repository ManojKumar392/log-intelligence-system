import pandas as pd
from sklearn.preprocessing import LabelEncoder


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # -------------------------
    # Time-based features
    # -------------------------

    # convert timestamp column to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    # -------------------------
    # Encode categorical fields
    # -------------------------
    service_encoder = LabelEncoder()
    level_encoder = LabelEncoder()

    df["service_encoded"] = service_encoder.fit_transform(df["service"])
    df["level_encoded"] = level_encoder.fit_transform(df["level"])

    # -------------------------
    # Message-level features
    # -------------------------
    df["message_length"] = df["message"].str.len()
    df["word_count"] = df["message"].str.split().str.len()

    # -------------------------
    # Drop raw columns ML won’t use
    # -------------------------
    features = df[
        [
            "severity",
            "hour",
            "day_of_week",
            "is_weekend",
            "service_encoded",
            "level_encoded",
            "message_length",
            "word_count",
        ]
    ]

    return features


if __name__ == "__main__":
    from data_loader import load_logs

    df = load_logs(limit=50000)
    X = engineer_features(df)

    print(X.head())
    print(X.info())
