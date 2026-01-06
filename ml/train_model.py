import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from data_loader import load_logs
from feature_engineering import engineer_features

import os

MODEL_PATH = "models/isolation_forest.joblib"
SCALER_PATH = "models/scaler.joblib"


def train_model():
    # -------------------------
    # Ensure models directory exists
    # -------------------------
    os.makedirs("models", exist_ok=True)
    
    # -------------------------
    # Load data
    # -------------------------
    print("Loading logs from database...")
    df = load_logs(limit=50_000)

    if df.empty:
        raise RuntimeError("No data found in database")
    
    print(f"Loaded {len(df)} log records")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    # -------------------------
    # Feature engineering WITH windowed features
    # -------------------------
    print("Engineering features (including windowed features)...")
    print("Note: This uses batch mode - pandas rolling windows, not streaming buffers")
    
    # Sort by service and timestamp for proper windowed feature computation
    df = df.sort_values(['service', 'timestamp']).reset_index(drop=True)
    
    # Engineer features - batch mode will use pandas rolling functions
    X = engineer_features(df, windowed_extractor=None, include_windowed=True, is_training=True)
    
    # Save encoders for inference to use
    from feature_engineering import save_encoders
    save_encoders(X._service_encoder, X._level_encoder)
    
    print(f"Feature matrix shape: {X.shape}")
    print(f"Features: {list(X.columns)}")
    
    # Check for any NaN or inf values
    if X.isnull().any().any():
        print("WARNING: NaN values detected. Filling with 0...")
        X = X.fillna(0)
    
    if np.isinf(X.values).any():
        print("WARNING: Infinite values detected. Clipping...")
        X = X.replace([np.inf, -np.inf], 0)

    # -------------------------
    # Scale features
    # -------------------------
    print("Scaling features...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    print(f"Saving scaler to {SCALER_PATH}")
    joblib.dump(scaler, SCALER_PATH)

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
        verbose=1
    )

    model.fit(X_scaled)

    # -------------------------
    # Save model
    # -------------------------
    print(f"Saving model to {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)

    # -------------------------
    # Model evaluation
    # -------------------------
    print("\n" + "="*60)
    print("MODEL EVALUATION")
    print("="*60)
    
    scores = model.decision_function(X_scaled)
    predictions = model.predict(X_scaled)
    
    anomaly_count = (predictions == -1).sum()
    normal_count = (predictions == 1).sum()
    anomaly_rate = anomaly_count / len(predictions) * 100
    
    print(f"Total samples: {len(predictions)}")
    print(f"Predicted anomalies: {anomaly_count} ({anomaly_rate:.2f}%)")
    print(f"Predicted normal: {normal_count} ({100-anomaly_rate:.2f}%)")
    print(f"\nAnomaly score range: min={scores.min():.3f}, max={scores.max():.3f}")
    print(f"Anomaly score mean: {scores.mean():.3f}")
    print(f"Anomaly score std: {scores.std():.3f}")
    
    # Show score distribution
    print("\nScore percentiles:")
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    for p in percentiles:
        print(f"  {p}th percentile: {np.percentile(scores, p):.3f}")
    
    # -------------------------
    # Feature importance analysis
    # -------------------------
    print("\n" + "="*60)
    print("FEATURE STATISTICS")
    print("="*60)
    
    feature_stats = pd.DataFrame({
        'feature': X.columns,
        'mean': X.mean(),
        'std': X.std(),
        'min': X.min(),
        'max': X.max()
    })
    
    print(feature_stats.to_string(index=False))
    
    # Show which features have the most variation
    print("\nTop 10 features by standard deviation:")
    top_features = feature_stats.nlargest(10, 'std')
    print(top_features[['feature', 'std']].to_string(index=False))
    
    # -------------------------
    # Sample predictions
    # -------------------------
    print("\n" + "="*60)
    print("SAMPLE PREDICTIONS")
    print("="*60)
    
    # Show some detected anomalies
    anomaly_indices = np.where(predictions == -1)[0]
    if len(anomaly_indices) > 0:
        print("\nSample detected anomalies:")
        sample_indices = np.random.choice(anomaly_indices, min(5, len(anomaly_indices)), replace=False)
        for idx in sample_indices:
            print(f"\nAnomaly #{idx}:")
            print(f"  Service: {df.iloc[idx]['service']}")
            print(f"  Level: {df.iloc[idx]['level']}")
            print(f"  Severity: {df.iloc[idx]['severity']}")
            print(f"  Score: {scores[idx]:.3f}")
            print(f"  Message: {df.iloc[idx]['message'][:80]}...")
    
    print("\n" + "="*60)
    print("Training complete! ✓")
    print("="*60)
    print(f"\nModel saved to: {MODEL_PATH}")
    print(f"Scaler saved to: {SCALER_PATH}")
    print("\nYou can now run inference.py to detect anomalies in real-time!")


if __name__ == "__main__":
    train_model()