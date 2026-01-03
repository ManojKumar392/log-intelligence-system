# inference.py
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from feature_engineering import engineer_features

POSTGRES_URL = "postgresql://loguser:logpass@localhost:5432/log_intelligence"
MODEL_PATH = "models/isolation_forest.joblib"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "logs.processed"
BATCH_SIZE = 500  # number of logs to process at a time

# -------------------------
# Load trained model
# -------------------------
def load_model(path=MODEL_PATH):
    print(f"Loading model from {path}...")
    return joblib.load(path)

# -------------------------
# Initialize database
# -------------------------
def get_engine():
    return create_engine(POSTGRES_URL)

def create_anomaly_table(engine):
    """
    Creates log_anomalies table if it doesn't exist
    """
    query = """
    CREATE TABLE IF NOT EXISTS log_anomalies (
        log_id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        service TEXT,
        level TEXT,
        severity FLOAT,
        message TEXT,
        category TEXT,
        metadata JSONB,
        processed_at TIMESTAMP,
        anomaly_score FLOAT,
        is_anomaly BOOLEAN
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))
    print("log_anomalies table ready")

# -------------------------
# Run inference on a batch
# -------------------------
def score_logs(df, model):
    """
    Input: dataframe of raw logs
    Output: dataframe with anomaly_score & is_anomaly
    """
    if df.empty:
        return pd.DataFrame()
    
    X = engineer_features(df)
    scores = model.decision_function(X)        # higher = normal, lower = anomaly
    preds = model.predict(X)                   # 1 = normal, -1 = anomaly

    df = df.copy()
    df["anomaly_score"] = scores
    df["is_anomaly"] = preds == -1

    return df

# -------------------------
# Save results to DB
# -------------------------
def save_anomalies(df, engine):
    if df.empty:
        return
    
    # Convert dicts to JSON strings
    if "metadata" in df.columns:
        df = df.copy()
        df["metadata"] = df["metadata"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    with engine.begin() as conn:
        df.to_sql(
            "log_anomalies",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )
    print(f"Saved {len(df)} anomaly results to DB")

# -------------------------
# Kafka streaming inference
# -------------------------
def stream_inference():
    engine = get_engine()
    create_anomaly_table(engine)
    model = load_model()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="log-intelligence-inference",
    )
    
    print(f"Listening to Kafka topic '{KAFKA_TOPIC}' for incoming logs...")
    buffer = []

    for msg in consumer:
        buffer.append(msg.value)

        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            scored_df = score_logs(df, model)
            save_anomalies(scored_df, engine)
            print(f"Processed batch of {len(buffer)} logs")
            buffer.clear()

# -------------------------
# Main entry
# -------------------------
if __name__ == "__main__":
    stream_inference()
