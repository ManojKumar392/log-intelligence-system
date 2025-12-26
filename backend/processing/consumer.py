import asyncio
import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import re

KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "logs.raw"
PROCESSED_TOPIC = "logs.processed"

logging.basicConfig(level=logging.INFO)

def process_log(log: dict) -> dict | None:
    """
    Advanced preprocessing: clean, enrich, categorize, and normalize logs.
    Return None if log is invalid or filtered out.
    """

    # 1️⃣ Required fields check
    if "message" not in log or "level" not in log:
        return None

    # 2️⃣ Clean strings
    message = log["message"].strip()
    service = log.get("service", "unknown").strip().lower()

    # 3️⃣ Normalize log level
    level = log["level"].upper()
    severity_map = {
        "DEBUG": 1,
        "INFO": 2,
        "WARN": 3,
        "WARNING": 3,
        "ERROR": 4,
        "CRITICAL": 5,
    }
    severity = severity_map.get(level, 0)

    # 4️⃣ Basic filtering: drop debug logs if needed
    if severity < 2:
        return None

    # 5️⃣ Categorize log by keywords
    category = "general"
    if re.search(r"timeout|failed|error", message, re.IGNORECASE):
        category = "error"
    elif re.search(r"cache|slow|delay", message, re.IGNORECASE):
        category = "performance"
    elif re.search(r"login|auth|access", message, re.IGNORECASE):
        category = "security"

    # 6️⃣ Enrich metadata
    metadata = log.get("metadata")

    if not isinstance(metadata, dict):
        metadata = {}

    metadata.setdefault("source_host", log.get("host", "unknown"))

    processed_log = {
        "timestamp": log.get("timestamp") or datetime.utcnow().isoformat(),
        "service": service,
        "level": level,
        "severity": severity,
        "message": message,
        "category": category,
        "metadata": metadata,
        "processed_at": datetime.utcnow().isoformat(),
    }

    return processed_log

async def consume_and_forward():
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="log-processing-group",
        auto_offset_reset="earliest",
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()

    logging.info("Advanced processing service started")

    try:
        async for msg in consumer:
            raw_log = msg.value
            logging.info(f"Raw log: {raw_log}")

            processed_log = process_log(raw_log)

            if processed_log is None:
                logging.warning("Dropped invalid or filtered log")
                continue

            await producer.send_and_wait(PROCESSED_TOPIC, processed_log)
            logging.info(f"Processed → logs.processed: {processed_log}")

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(consume_and_forward())
