import json
from aiokafka import AIOKafkaProducer

producer = None

async def start_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092"
    )
    await producer.start()

async def stop_producer():
    global producer
    if producer:
        await producer.stop()

async def send_log_to_kafka(log: dict):
    global producer
    await producer.send_and_wait("logs.raw", json.dumps(log).encode("utf-8"))
