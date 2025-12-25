from fastapi import FastAPI, Request
from .schemas import LogEntry, LogBatch
from .producer import start_producer, stop_producer, send_log_to_kafka
from slowapi import Limiter
from slowapi.middleware import SlowAPIMiddleware
import logging
from slowapi.util import get_remote_address

app = FastAPI(title="Log Ingestion Service")

# Rate Limiting: 5 requests per second per IP
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# Logging
logging.basicConfig(level=logging.INFO)


# Start/stop Kafka producer
@app.on_event("startup")
async def startup():
    await start_producer()

@app.on_event("shutdown")
async def shutdown():
    await stop_producer()

@app.get("/ping")
def ping():
    return {"message": "pong"}

@app.post("/logs")
@limiter.limit("5/second")
async def ingest_log(request: Request, log: LogEntry):
    log_data = log.model_dump()
    await send_log_to_kafka(log_data)
    logging.info(f"Received log: {log_data}")
    return {"status": "ok", "message": "Log received"}


@app.post("/logs/batch")
@limiter.limit("3/second")
async def ingest_batch(request: Request, batch: LogBatch):
    for log in batch.logs:
        await send_log_to_kafka(log.dict())
    logging.info(f"Received batch of {len(batch.logs)} logs")
    return {"status": "ok", "message": f"{len(batch.logs)} logs received"}


@app.post("/logs/raw")
@limiter.limit("5/second")
async def ingest_raw(request: Request):
    text_data = await request.body()
    log_data = {"raw_text": text_data.decode("utf-8")}
    await send_log_to_kafka(log_data)
    logging.info(f"Received raw log")
    return {"status": "ok", "message": "Raw log received"}
