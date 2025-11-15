<!-- Log model. -->
{
"timestamp": ISO8601 string,
"service": "string",
"level": "INFO|WARN|ERROR",
"message": "string",
"metadata": { ... }
}

<!-- Anamoly model. -->
{
  "id": int,
  "timestamp": ISO8601 string,
  "service": "string",
  "severity": "low|medium|high",
  "confidence": float,
  "message": "string",
  "raw_log_ref": "s3://...."
}

<!-- Alert model. -->
{
  "id": int,
  "timestamp": ISO8601 string,
  "service": "string",
  "anomaly_id": int,
  "notification_type": "email|webhook|websocket",
  "status": "sent|failed"
}

