from aiokafka import AIOKafkaProducer
import json

KAFKA_BOOTSTRAP = "localhost:9092"
PROCESSED_TOPIC = "logs.processed"

producer = None

async def start_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

async def send_processed_log(log: dict):
    await producer.send_and_wait(PROCESSED_TOPIC, log)
