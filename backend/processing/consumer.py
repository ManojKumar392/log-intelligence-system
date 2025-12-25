import asyncio
import json
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "logs.raw"
PROCESSED_TOPIC = "logs.processed"

logging.basicConfig(level=logging.INFO)

def process_log(log: dict) -> dict | None:
    """
    Clean + enrich a raw log.
    Return None if log is invalid.
    """

    # Required fields check
    if "message" not in log or "level" not in log:
        return None

    # Normalize level
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

    processed_log = {
        "timestamp": log.get("timestamp") or datetime.utcnow().isoformat(),
        "service": log.get("service", "unknown"),
        "level": level,
        "severity": severity,
        "message": log["message"],
        "metadata": log.get("metadata", {}),
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

    logging.info("Processing service started")

    try:
        async for msg in consumer:
            raw_log = msg.value
            logging.info(f"Raw log: {raw_log}")

            processed_log = process_log(raw_log)

            if processed_log is None:
                logging.warning("Dropped invalid log")
                continue

            await producer.send_and_wait(PROCESSED_TOPIC, processed_log)
            logging.info(f"Processed → logs.processed: {processed_log}")

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(consume_and_forward())
