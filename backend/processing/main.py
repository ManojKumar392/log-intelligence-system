import asyncio
from consumer import get_consumer
from producer import start_producer, send_processed_log
from processor import process_log

async def main():
    consumer = await get_consumer()
    await start_producer()

    try:
        async for msg in consumer:
            processed = process_log(msg.value)
            await send_processed_log(processed)
            print("Processed log:", processed)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
