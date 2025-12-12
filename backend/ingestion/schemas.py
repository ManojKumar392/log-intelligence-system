from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime

class LogEntry(BaseModel):
    timestamp: Optional[datetime] = None
    service: str
    level: Optional[str] = "INFO"
    message: str
    metadata: Optional[Dict[str, Any]] = None

class LogBatch(BaseModel):
    logs: List[LogEntry]
