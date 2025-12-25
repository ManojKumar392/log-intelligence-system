from datetime import datetime

def process_log(log: dict) -> dict:
    return {
        "message": log.get("message") or log.get("raw_text"),
        "level": log.get("level", "UNKNOWN"),
        "service": log.get("service", "unknown"),
        "timestamp": log.get("timestamp", datetime.utcnow().isoformat()),
        "processed_at": datetime.utcnow().isoformat(),
    }
