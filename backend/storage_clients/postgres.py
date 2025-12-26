import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env

KAFKA_BOOTSTRAP = "localhost:9092"
PROCESSED_TOPIC = "logs.processed"

POSTGRES_URL = (
    os.getenv("PG_URL")
)

logging.basicConfig(level=logging.INFO)

engine = create_async_engine(POSTGRES_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def save_log(db: AsyncSession, log: dict):
    query = text("""
        INSERT INTO public.logs (
            timestamp,
            service,
            level,
            severity,
            message,
            metadata,
            processed_at
        )
        VALUES (
            :timestamp,
            :service,
            :level,
            :severity,
            :message,
            :metadata,
            :processed_at
        )
    """)

    # Convert ISO strings to datetime objects if they are strings
    timestamp = log["timestamp"]
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)

    processed_at = log["processed_at"]
    if isinstance(processed_at, str):
        processed_at = datetime.fromisoformat(processed_at)

    await db.execute(query, {
        "timestamp": timestamp,
        "service": log["service"],
        "level": log["level"],
        "severity": log["severity"],
        "message": log["message"],
        "metadata": json.dumps(log["metadata"]),
        "processed_at": processed_at,
    })

    await db.commit()


async def consume_and_store():
    consumer = AIOKafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="postgres-writer-group",
        auto_offset_reset="latest",
    )

    await consumer.start()
    logging.info("Postgres consumer started")

    try:
        async for msg in consumer:
            log = msg.value
            logging.info(f"Storing log: {log}")

            async with AsyncSessionLocal() as db:
                await save_log(db, log)

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_and_store())
