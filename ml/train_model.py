import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest

from data_loader import load_logs
from feature_engineering import engineer_features


MODEL_PATH = "models/isolation_forest.joblib"


def train_model():
    # -------------------------
    # Load data
    # -------------------------
    print("Loading logs from database...")
    df = load_logs(limit=50_000)

    if df.empty:
        raise RuntimeError("No data found in database")

    # -------------------------
    # Feature engineering
    # -------------------------
    print("Engineering features...")
    X = engineer_features(df)

    # -------------------------
    # Train Isolation Forest
    # -------------------------
    print("Training Isolation Forest...")
    model = IsolationForest(
        n_estimators=200,
        max_samples="auto",
        contamination=0.03,   # matches your synthetic anomaly rate
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X)

    # -------------------------
    # Save model
    # -------------------------
    print(f"Saving model to {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)

    # -------------------------
    # Sanity check
    # -------------------------
    scores = model.decision_function(X)
    print("Training complete")
    print(f"Score range: min={scores.min():.3f}, max={scores.max():.3f}")


if __name__ == "__main__":
    train_model()
