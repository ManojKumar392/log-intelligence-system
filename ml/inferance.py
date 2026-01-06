# inference.py
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from feature_engineering import engineer_features, get_windowed_extractor
import logging
import signal
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_URL = "postgresql://loguser:logpass@localhost:5432/log_intelligence"
MODEL_PATH = "models/isolation_forest.joblib"
SCALER_PATH = "models/scaler.joblib"  # Add if you're using a scaler
STATE_PATH = "models/windowed_state.json"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "logs.processed"
BATCH_SIZE = 500  # number of logs to process at a time

# Global windowed extractor and encoders
windowed_extractor = None
service_encoder = None
level_encoder = None

# -------------------------
# Load trained model
# -------------------------
def load_model(path=MODEL_PATH):
    logger.info(f"Loading model from {path}...")
    return joblib.load(path)

def load_scaler(path=SCALER_PATH):
    """Load scaler if you used one during training"""
    try:
        logger.info(f"Loading scaler from {path}...")
        return joblib.load(path)
    except FileNotFoundError:
        logger.warning("No scaler found. Skipping scaling step.")
        return None

# -------------------------
# Initialize windowed extractor and encoders
# -------------------------
def initialize_components(state_path=STATE_PATH):
    """Initialize windowed extractor and load encoders"""
    global windowed_extractor, service_encoder, level_encoder
    
    # Initialize windowed extractor
    windowed_extractor = get_windowed_extractor(window_sizes=[5, 10, 30])
    
    try:
        windowed_extractor.load_state(state_path)
        logger.info(f"Loaded windowed feature state from {state_path}")
    except:
        logger.info("Starting with fresh windowed feature state")
    
    # Load encoders
    from feature_engineering import load_encoders
    service_encoder, level_encoder = load_encoders()
    
    if service_encoder is None or level_encoder is None:
        raise RuntimeError(
            "Encoders not found! Please run train_model.py first to generate encoders."
        )
    
    logger.info("Encoders loaded successfully")
    
    return windowed_extractor

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
    logger.info("log_anomalies table ready")

# -------------------------
# Run inference on a batch
# -------------------------
def score_logs(df, model, scaler=None):
    """
    Input: dataframe of raw logs
    Output: dataframe with anomaly_score & is_anomaly
    """
    if df.empty:
        return pd.DataFrame()
    
    # Sort by timestamp for proper windowed feature computation
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Engineer features WITH windowed features
    # Process each row sequentially to maintain temporal order
    X_list = []
    for idx, row in df.iterrows():
        row_df = pd.DataFrame([row])
        X_row = engineer_features(
            row_df,
            windowed_extractor=windowed_extractor,
            include_windowed=True,
            service_encoder=service_encoder,
            level_encoder=level_encoder,
            is_training=False
        )
        X_list.append(X_row)
    
    X = pd.concat(X_list, axis=0, ignore_index=True)
    
    # DEBUG: Print feature info on first batch
    if not hasattr(score_logs, '_debug_printed'):
        logger.info(f"Inference features: {list(X.columns)}")
        logger.info(f"Inference shape: {X.shape}")
        if scaler is not None:
            logger.info(f"Scaler expects {scaler.n_features_in_} features")
            if hasattr(scaler, 'feature_names_in_'):
                logger.info(f"Scaler trained on: {list(scaler.feature_names_in_)}")
        score_logs._debug_printed = True
    
    # Apply scaling if scaler is available
    if scaler is not None:
        X_scaled = scaler.transform(X)
    else:
        X_scaled = X
    
    # Score with model
    scores = model.decision_function(X_scaled)  # higher = normal, lower = anomaly
    preds = model.predict(X_scaled)             # 1 = normal, -1 = anomaly

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
        df["metadata"] = df["metadata"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else x
        )

    with engine.begin() as conn:
        df.to_sql(
            "log_anomalies",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )
    
    anomaly_count = df["is_anomaly"].sum()
    logger.info(
        f"Saved {len(df)} results to DB "
        f"({anomaly_count} anomalies, {len(df) - anomaly_count} normal)"
    )

# -------------------------
# Save windowed state periodically
# -------------------------
def save_windowed_state(state_path=STATE_PATH):
    """Save windowed feature extractor state"""
    if windowed_extractor:
        windowed_extractor.save_state(state_path)
        logger.info(f"Saved windowed state to {state_path}")

# -------------------------
# Graceful shutdown handler
# -------------------------
def setup_signal_handlers(state_path=STATE_PATH):
    """Setup handlers to save state on shutdown"""
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received. Saving state...")
        save_windowed_state(state_path)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# -------------------------
# Kafka streaming inference
# -------------------------
def stream_inference():
    # Initialize components
    engine = get_engine()
    create_anomaly_table(engine)
    model = load_model()
    scaler = load_scaler()
    initialize_components()  # Initialize extractor and load encoders
    
    # Setup graceful shutdown
    setup_signal_handlers()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="log-intelligence-inference",
    )
    
    logger.info(f"Listening to Kafka topic '{KAFKA_TOPIC}' for incoming logs...")
    logger.info(f"Windowed features enabled with windows: {windowed_extractor.window_sizes}")
    
    buffer = []
    batch_count = 0

    try:
        for msg in consumer:
            buffer.append(msg.value)

            if len(buffer) >= BATCH_SIZE:
                batch_count += 1
                df = pd.DataFrame(buffer)
                
                # Score logs with windowed features
                scored_df = score_logs(df, model, scaler)
                
                # Save to database
                save_anomalies(scored_df, engine)
                
                logger.info(f"Processed batch #{batch_count} of {len(buffer)} logs")
                
                # Save windowed state periodically (every 10 batches)
                if batch_count % 10 == 0:
                    save_windowed_state()
                
                buffer.clear()
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Process remaining buffer
        if buffer:
            logger.info(f"Processing final batch of {len(buffer)} logs...")
            df = pd.DataFrame(buffer)
            scored_df = score_logs(df, model, scaler)
            save_anomalies(scored_df, engine)
        
        # Save final state
        save_windowed_state()
        consumer.close()
        logger.info("Inference service stopped gracefully")

# -------------------------
# Main entry
# -------------------------
if __name__ == "__main__":
    stream_inference()