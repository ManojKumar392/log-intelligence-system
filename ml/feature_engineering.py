import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from collections import deque
from typing import Dict, Optional
import json
import joblib
import os


class WindowedFeatureExtractor:
    """Maintains state for windowed feature computation across streaming data"""
    
    def __init__(self, window_sizes=[5, 10, 30]):
        self.window_sizes = window_sizes
        # Store buffers per service (or entity_id)
        self.buffers = {}
        
    def update_and_extract(self, service: str, severity: int, message_length: int) -> Dict:
        """Update windows and extract windowed features for a single record"""
        
        if service not in self.buffers:
            self.buffers[service] = {
                'severity': {size: deque(maxlen=size) for size in self.window_sizes},
                'message_length': {size: deque(maxlen=size) for size in self.window_sizes}
            }
        
        features = {}
        
        # Update buffers and compute features
        for metric_name, metric_value in [('severity', severity), ('message_length', message_length)]:
            for window_size in self.window_sizes:
                buffer = self.buffers[service][metric_name][window_size]
                buffer.append(metric_value)
                
                window_data = list(buffer)
                n = len(window_data)
                
                if n >= 2:
                    # Rolling statistics
                    features[f'{metric_name}_rolling_mean_{window_size}'] = np.mean(window_data)
                    features[f'{metric_name}_rolling_std_{window_size}'] = np.std(window_data)
                    features[f'{metric_name}_rolling_max_{window_size}'] = np.max(window_data)
                    features[f'{metric_name}_rolling_min_{window_size}'] = np.min(window_data)
                    
                    # Trend features
                    features[f'{metric_name}_trend_{window_size}'] = window_data[-1] - window_data[0]
                    
                    # Rate of change
                    if n > 1:
                        features[f'{metric_name}_rate_of_change_{window_size}'] = (
                            window_data[-1] - window_data[-2]
                        )
                else:
                    # Not enough data yet - use neutral values
                    features[f'{metric_name}_rolling_mean_{window_size}'] = metric_value
                    features[f'{metric_name}_rolling_std_{window_size}'] = 0.0
                    features[f'{metric_name}_rolling_max_{window_size}'] = metric_value
                    features[f'{metric_name}_rolling_min_{window_size}'] = metric_value
                    features[f'{metric_name}_trend_{window_size}'] = 0.0
                    features[f'{metric_name}_rate_of_change_{window_size}'] = 0.0
        
        return features
    
    def save_state(self, filepath: str):
        """Save buffer state to disk for persistence"""
        state = {}
        for service, metrics in self.buffers.items():
            state[service] = {}
            for metric_name, windows in metrics.items():
                state[service][metric_name] = {
                    size: list(deque_obj) for size, deque_obj in windows.items()
                }
        
        with open(filepath, 'w') as f:
            json.dump(state, f)
    
    def load_state(self, filepath: str):
        """Load buffer state from disk"""
        try:
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            for service, metrics in state.items():
                self.buffers[service] = {}
                for metric_name, windows in metrics.items():
                    self.buffers[service][metric_name] = {
                        int(size): deque(data, maxlen=int(size)) 
                        for size, data in windows.items()
                    }
        except FileNotFoundError:
            print(f"State file {filepath} not found. Starting fresh.")


# Global instance for streaming inference (singleton pattern)
_global_extractor = None

def get_windowed_extractor(window_sizes=[5, 10, 30]) -> WindowedFeatureExtractor:
    """Get or create global windowed feature extractor"""
    global _global_extractor
    if _global_extractor is None:
        _global_extractor = WindowedFeatureExtractor(window_sizes)
    return _global_extractor


def save_encoders(service_encoder: LabelEncoder, level_encoder: LabelEncoder, filepath: str = "models/encoders.joblib"):
    """Save label encoders for reuse during inference"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    joblib.dump({'service': service_encoder, 'level': level_encoder}, filepath)
    print(f"Saved encoders to {filepath}")


def load_encoders(filepath: str = "models/encoders.joblib"):
    """Load label encoders"""
    try:
        encoders = joblib.load(filepath)
        print(f"Loaded encoders from {filepath}")
        return encoders['service'], encoders['level']
    except FileNotFoundError:
        print(f"Encoders not found at {filepath}. Creating new ones.")
        return None, None


def engineer_features(
    df: pd.DataFrame, 
    windowed_extractor: Optional[WindowedFeatureExtractor] = None,
    include_windowed: bool = True,
    service_encoder: Optional[LabelEncoder] = None,
    level_encoder: Optional[LabelEncoder] = None,
    is_training: bool = False
) -> pd.DataFrame:
    """
    Engineer features from log data with optional windowed features.
    
    Args:
        df: Input dataframe with log data
        windowed_extractor: Optional extractor for windowed features (for streaming)
        include_windowed: Whether to compute windowed features
        service_encoder: Pre-fitted service encoder (for inference)
        level_encoder: Pre-fitted level encoder (for inference)
        is_training: Whether this is training mode (fit encoders) or inference (transform only)
    
    Returns:
        DataFrame with engineered features
    """
    df = df.copy()

    # -------------------------
    # Time-based features
    # -------------------------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    # -------------------------
    # Message-level features (compute before encoding)
    # -------------------------
    df["message_length"] = df["message"].str.len()
    df["word_count"] = df["message"].str.split().str.len()

    # -------------------------
    # Encode categorical fields
    # -------------------------
    if is_training:
        # Training: fit new encoders
        service_encoder = LabelEncoder()
        level_encoder = LabelEncoder()
        df["service_encoded"] = service_encoder.fit_transform(df["service"])
        df["level_encoded"] = level_encoder.fit_transform(df["level"])
        # Return encoders for saving
        df._service_encoder = service_encoder
        df._level_encoder = level_encoder
    else:
        # Inference: use provided encoders
        if service_encoder is None or level_encoder is None:
            raise ValueError("Encoders must be provided for inference mode")
        
        # Handle unseen categories gracefully
        def safe_transform(encoder, values):
            # Map unseen values to a default (first class)
            result = []
            for val in values:
                if val in encoder.classes_:
                    result.append(encoder.transform([val])[0])
                else:
                    # Use first class as default for unseen values
                    result.append(0)
            return result
        
        df["service_encoded"] = safe_transform(service_encoder, df["service"])
        df["level_encoded"] = safe_transform(level_encoder, df["level"])

    # -------------------------
    # Base features
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
    ].copy()

    # -------------------------
    # Windowed features
    # -------------------------
    if include_windowed and windowed_extractor is not None:
        # For streaming: process row by row
        windowed_features_list = []
        for idx, row in df.iterrows():
            windowed_feats = windowed_extractor.update_and_extract(
                service=row['service'],
                severity=row['severity'],
                message_length=row['message_length']
            )
            windowed_features_list.append(windowed_feats)
        
        windowed_df = pd.DataFrame(windowed_features_list, index=features.index)
        
        # IMPORTANT: Sort columns to match training order
        windowed_df = windowed_df[sorted(windowed_df.columns)]
        
        features = pd.concat([features, windowed_df], axis=1)
    
    elif include_windowed:
        # For batch training: compute rolling features using pandas
        df_sorted = df.sort_values(['service', 'timestamp'])
        
        window_sizes = [5, 10, 30]
        
        # Collect all windowed feature columns in a list to control order
        windowed_cols = []
        
        for window_size in window_sizes:
            for metric in ['severity', 'message_length']:
                grouped = df_sorted.groupby('service')[metric]
                
                col_prefix = f'{metric}_rolling_mean_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.rolling(window_size, min_periods=1).mean()
                )
                windowed_cols.append(col_prefix)
                
                col_prefix = f'{metric}_rolling_std_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.rolling(window_size, min_periods=1).std().fillna(0)
                )
                windowed_cols.append(col_prefix)
                
                col_prefix = f'{metric}_rolling_max_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.rolling(window_size, min_periods=1).max()
                )
                windowed_cols.append(col_prefix)
                
                col_prefix = f'{metric}_rolling_min_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.rolling(window_size, min_periods=1).min()
                )
                windowed_cols.append(col_prefix)
                
                col_prefix = f'{metric}_trend_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.diff(window_size).fillna(0)
                )
                windowed_cols.append(col_prefix)
                
                col_prefix = f'{metric}_rate_of_change_{window_size}'
                features[col_prefix] = grouped.transform(
                    lambda x: x.diff().fillna(0)
                )
                windowed_cols.append(col_prefix)
        
        # Sort all columns: base features first, then windowed features sorted
        base_cols = ['severity', 'hour', 'day_of_week', 'is_weekend', 
                     'service_encoded', 'level_encoded', 'message_length', 'word_count']
        features = features[base_cols + sorted(windowed_cols)]

    # If training, attach encoders to return them
    if is_training:
        features._service_encoder = df._service_encoder
        features._level_encoder = df._level_encoder

    return features


if __name__ == "__main__":
    from data_loader import load_logs

    # Test batch processing (training mode)
    print("=" * 60)
    print("BATCH MODE (Training)")
    print("=" * 60)
    df = load_logs(limit=1000)
    X = engineer_features(df, include_windowed=True, is_training=True)
    
    # Save encoders
    save_encoders(X._service_encoder, X._level_encoder)
    
    print(X.head())
    print(X.info())
    print(f"\nTotal features: {len(X.columns)}")
    print(f"Feature names: {list(X.columns)}")
    
    # Test streaming mode (inference mode)
    print("\n" + "=" * 60)
    print("STREAMING MODE (Inference)")
    print("=" * 60)
    
    # Load encoders
    service_encoder, level_encoder = load_encoders()
    
    df_stream = load_logs(limit=100)
    extractor = WindowedFeatureExtractor(window_sizes=[5, 10, 30])
    
    # Simulate streaming - process records one by one
    for idx, row in df_stream.iterrows():
        row_df = pd.DataFrame([row])
        X_stream = engineer_features(
            row_df, 
            windowed_extractor=extractor, 
            include_windowed=True,
            service_encoder=service_encoder,
            level_encoder=level_encoder,
            is_training=False
        )
        if idx < 3:  # Show first few
            print(f"\nRecord {idx}:")
            print(X_stream)
    
    print(f"\nProcessed {len(df_stream)} streaming records")