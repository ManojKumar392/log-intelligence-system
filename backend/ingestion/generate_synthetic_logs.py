import asyncio
import json
import random
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "logs.raw"

SERVICES = ["auth", "payment", "db", "cache", "api"]

NORMAL_LEVELS = ["DEBUG", "INFO", "WARN"]
ERROR_LEVELS = ["ERROR", "CRITICAL"]

MESSAGES = {
    "normal": [
        "request completed",
        "cache hit",
        "user authenticated",
        "heartbeat ok",
    ],
    "error": [
        "db timeout",
        "payment declined",
        "cache miss",
        "request latency high",
        "service unavailable",
    ],
}

# distribution knobs
TOTAL_LOGS = 50_000          
ANOMALY_RATE = 0.03          # 3% anomalous logs
BURST_PROBABILITY = 0.01     # chance to start an anomaly burst
BURST_SIZE = (20, 100)       # burst length range


async def produce_logs():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await producer.start()
    print("Synthetic producer started")

    try:
        now = datetime.utcnow()
        anomaly_burst_remaining = 0

        for i in range(TOTAL_LOGS):
            # decide anomaly vs normal
            is_anomaly = False

            if anomaly_burst_remaining > 0:
                is_anomaly = True
                anomaly_burst_remaining -= 1
            elif random.random() < BURST_PROBABILITY:
                anomaly_burst_remaining = random.randint(*BURST_SIZE)
                is_anomaly = True
            elif random.random() < ANOMALY_RATE:
                is_anomaly = True

            if is_anomaly:
                level = random.choice(ERROR_LEVELS)
                message = random.choice(MESSAGES["error"])
                service = random.choices(
                    SERVICES,
                    weights=[1, 4, 2, 1, 1],  # payment/db fail more
                )[0]
            else:
                level = random.choices(
                    NORMAL_LEVELS,
                    weights=[2, 6, 2],  # mostly INFO
                )[0]
                message = random.choice(MESSAGES["normal"])
                service = random.choice(SERVICES)

            log = {
                "timestamp": (
                    now - timedelta(seconds=random.randint(0, 86400))
                ).isoformat(),
                "service": service,
                "level": level,
                "message": message,
                "metadata": {
                    "user_id": random.randint(1, 5000),
                    "synthetic": True,
                },
            }

            await producer.send_and_wait(RAW_TOPIC, log)

            if i % 5000 == 0 and i > 0:
                print(f"Produced {i} logs")

        print(f"Finished producing {TOTAL_LOGS} logs")

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(produce_logs())
