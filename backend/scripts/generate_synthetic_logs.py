import json
import random
from datetime import datetime, timedelta

SERVICES = ["auth", "payment", "db", "cache", "api"]
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
MESSAGES = [
    "db timeout",
    "cache miss",
    "user login failed",
    "payment declined",
    "request latency high",
]

SEVERITY_MAP = {
    "DEBUG": 1,
    "INFO": 2,
    "WARN": 3,
    "WARNING": 3,
    "ERROR": 4,
    "CRITICAL": 5,
}

NUM_LOGS = 1000  # adjust as needed

logs = []
start_time = datetime.utcnow() - timedelta(days=1)

for _ in range(NUM_LOGS):
    log = {
        "timestamp": (start_time + timedelta(seconds=random.randint(0, 86400))).isoformat(),
        "service": random.choice(SERVICES),
        "level": random.choice(LEVELS),
        "severity": 0,  # will fill next
        "message": random.choice(MESSAGES),
        "metadata": {"user_id": random.randint(1, 1000)},
        "processed_at": datetime.utcnow().isoformat()
    }
    log["severity"] = SEVERITY_MAP[log["level"]]
    logs.append(log)

# Save to JSON file
with open("synthetic_logs.json", "w") as f:
    json.dump(logs, f, indent=2)

print(f"Generated {NUM_LOGS} synthetic logs")
