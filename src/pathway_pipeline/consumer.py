import time
import json
import pathway as pw

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway-consumer-group",
    "auto.offset.reset": "latest"
}

class TxnSchema(pw.Schema):
    user_id: int
    amount: float
    location: str
    timestamp: float

synthetic_stream = pw.io.kafka.read(
    KAFKA_CONFIG,
    topic="synthetic_streams",
    format="json",
    schema=TxnSchema,
)

coinbase_raw = pw.io.kafka.read(
    KAFKA_CONFIG,
    topic="coinbase_trades",
    format="raw",
)

@pw.udf
def get_current_time(dummy) -> float:
    return time.time()

@pw.udf
def calculate_latency(event_ts: float, ingest_ts: float) -> float:
    return round((ingest_ts - event_ts) * 1000, 2)

@pw.udf
def parse_trade_id(data: bytes) -> int | None:
    try:
        msg = json.loads(data.decode('utf-8'))
        if msg.get("type") == "match":
            return msg.get("trade_id", 0)
    except:
        pass
    return None

@pw.udf
def parse_price(data: bytes) -> float:
    try:
        msg = json.loads(data.decode('utf-8'))
        if msg.get("type") == "match":
            return float(msg.get("price", 0))
    except:
        pass
    return 0.0

@pw.udf
def parse_size(data: bytes) -> float:
    try:
        msg = json.loads(data.decode('utf-8'))
        if msg.get("type") == "match":
            return float(msg.get("size", 0))
    except:
        pass
    return 0.0

@pw.udf
def parse_time(data: bytes) -> str:
    try:
        msg = json.loads(data.decode('utf-8'))
        if msg.get("type") == "match":
            return msg.get("time", "")
    except:
        pass
    return ""

@pw.udf
def is_valid_trade(data: bytes) -> bool:
    try:
        msg = json.loads(data.decode('utf-8'))
        return msg.get("type") == "match"
    except:
        pass
    return False

synthetic_with_metrics = synthetic_stream.select(
    user_id=pw.this.user_id,
    amount=pw.this.amount,
    location=pw.this.location,
    event_timestamp=pw.this.timestamp,
    ingestion_timestamp=get_current_time(pw.this.timestamp),
)

synthetic_with_latency = synthetic_with_metrics.select(
    user_id=pw.this.user_id,
    amount=pw.this.amount,
    location=pw.this.location,
    event_timestamp=pw.this.event_timestamp,
    ingestion_timestamp=pw.this.ingestion_timestamp,
    latency_ms=calculate_latency(pw.this.event_timestamp, pw.this.ingestion_timestamp),
    stream_type="synthetic"
)

coinbase_with_metrics = coinbase_raw.select(
    trade_id=parse_trade_id(pw.this.data),
    price=parse_price(pw.this.data),
    size=parse_size(pw.this.data),
    trade_time=parse_time(pw.this.data),
    valid=is_valid_trade(pw.this.data),
    ingestion_timestamp=get_current_time(pw.this.data),
    stream_type="coinbase"
)

coinbase_valid = coinbase_with_metrics.filter(pw.this.valid)


pw.io.kafka.write(
    synthetic_with_latency,
    KAFKA_CONFIG,
    topic_name="synthetic_metrics",
    format="json",
)

def on_synthetic_change(key, row, time, is_addition):
    if is_addition:
        print(f"[SYNTHETIC] User: {row['user_id']}, Amount: ${row['amount']:.2f}, "
              f"Location: {row['location']}, Latency: {row['latency_ms']}ms")

pw.io.subscribe(synthetic_with_latency, on_synthetic_change)

pw.io.kafka.write(
    coinbase_valid,
    KAFKA_CONFIG,
    topic_name="coinbase_metrics",
    format="json",
)

def on_coinbase_change(key, row, time, is_addition):
    if is_addition:
        print(f"[COINBASE] Trade ID: {row['trade_id']}, "
              f"Price: ${row['price']:.2f}, Size: {row['size']:.6f}")

pw.io.subscribe(coinbase_valid, on_coinbase_change)

pw.run()