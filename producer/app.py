#!/usr/bin/env python3
"""
Binance → Kafka resilient producer
- Idempotent, durable writes (acks=all, enable.idempotence)
- Preserves ordering under retries (max.in.flight <= 5)
- Backpressure-aware produce (handles local queue saturation)
- Exponential WS reconnect with endpoint rotation
- Minimal deps: websockets, confluent_kafka, python-dotenv
"""

import asyncio
import json
import os
import signal
import sys
import time
from typing import List, Optional

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092") 
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_ticks")
SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt").lower().replace(" ", "").split(",")

# Public Binance endpoints (no auth). We’ll rotate on errors like HTTP 451.
BINANCE_ENDPOINTS = [
    #"wss://stream.binance.com:9443/stream",        # global
    "wss://data-stream.binance.vision/stream",     # mirror
    "wss://stream.binance.us:9443/stream",         # US
]

RECONNECT_INITIAL = float(os.getenv("RECONNECT_INITIAL", "1.0"))
RECONNECT_MAX = float(os.getenv("RECONNECT_MAX", "30.0"))
PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "180"))
PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "20"))

# Producer tuning
DELIVERY_TIMEOUT_MS = int(os.getenv("MESSAGE_TIMEOUT_MS", "120000"))
PRODUCER_DEBUG = os.getenv("PRODUCER_DEBUG", "").strip()  # e.g., "broker,protocol"

_shutdown = asyncio.Event()


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def _delivery_report(err, msg) -> None:
    """Delivery callback to log failures; successes are silent to avoid spam."""
    if err is not None:
        sys.stderr.write(f"[Kafka] Delivery failed for key={msg.key()!r}: {err}\n")


def build_kafka_producer() -> Producer:
    """
    Idempotent, durable producer:
    - enable.idempotence: broker-side dedupe for retries
    - acks=all: wait for ISR replication
    - max.in.flight <= 5: preserve per-partition ordering under retries
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        # Reliability / Idempotence
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10000000,  # effectively unlimited (fail fast controlled by message.timeout.ms)
        "max.in.flight.requests.per.connection": 5,
        "message.timeout.ms": DELIVERY_TIMEOUT_MS,
        # Throughput / batching
        "linger.ms": 20,
        "compression.type": "lz4",
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 200000,
        # Stability
        "socket.keepalive.enable": True,
    }
    if PRODUCER_DEBUG:
        conf["debug"] = PRODUCER_DEBUG
    return Producer(conf)


def safe_produce(
    producer: Producer,
    topic: str,
    key: bytes,
    value: bytes,
    on_delivery=_delivery_report,
    max_retries: int = 3,
) -> None:
    """
    Produce with local backpressure handling. If the client-side queue is full,
    poll briefly to make space and retry a few times before letting the exception bubble.
    """
    attempt = 0
    while True:
        try:
            producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
            return
        except BufferError:
            attempt += 1
            # The local queue is full. Poll to drain delivery callbacks and try again.
            producer.poll(0.1)
            if attempt > max_retries:
                # As a last resort, block a bit longer before final attempt
                producer.poll(0.5)
                producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
                return


# ── Transform ─────────────────────────────────────────────────────────────────
def normalize_aggtrade(payload: dict) -> dict:
    # https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
    return {
        "source": "binance",
        "event_type": payload.get("e", "aggTrade"),
        "event_time": payload.get("E"),        # ms
        "symbol": payload.get("s"),            # e.g., BTCUSDT
        "agg_id": payload.get("a"),
        "price": float(payload.get("p", "0")),
        "qty": float(payload.get("q", "0")),
        "trade_time": payload.get("T"),        # ms
        "is_buyer_maker": payload.get("m", False),
        "ingest_ts": int(time.time() * 1000),  # ms now
    }


# ── Streaming loop ────────────────────────────────────────────────────────────
async def stream_symbols(symbols: List[str], producer: Producer) -> None:
    streams = "/".join(f"{sym}@aggTrade" for sym in symbols)
    ep_idx = 0
    backoff = RECONNECT_INITIAL
    last_stats_log: float = time.time()

    while not _shutdown.is_set():
        url = f"{BINANCE_ENDPOINTS[ep_idx]}?streams={streams}"
        try:
            async with websockets.connect(
                url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                max_size=None,  # allow large frames
            ) as ws:
                sys.stdout.write(f"[WS] Connected: {url}\n")
                backoff = RECONNECT_INITIAL  # reset after successful connect

                async for raw in ws:
                    if _shutdown.is_set():
                        break

                    try:
                        msg = json.loads(raw)
                        data = msg.get("data", {})
                        if not data or data.get("e") != "aggTrade":
                            continue

                        normalized = normalize_aggtrade(data)
                        # Use symbol as key to keep per-symbol partition affinity
                        key = normalized["symbol"].encode("utf-8")
                        val = json.dumps(normalized, separators=(",", ":")).encode("utf-8")

                        safe_produce(producer, KAFKA_TOPIC, key, val)
                    except Exception as e:
                        sys.stderr.write(f"[WS] Parse/produce error: {e}\n")

                    # Drive delivery callbacks; very cheap call
                    producer.poll(0)

                    # Lightweight heartbeat logging every ~30s
                    now = time.time()
                    if now - last_stats_log > 30:
                        last_stats_log = now
                        sys.stdout.write("[Kafka] Producer alive; polling OK.\n")

        except websockets.InvalidStatusCode as e:
            sys.stderr.write(f"[WS] InvalidStatusCode {e.status_code} on {url}\n")
            # If blocked (451) or other hard HTTP error, try next endpoint.
            if getattr(e, "status_code", None) in (451, 403, 400, 404, 500):
                ep_idx = (ep_idx + 1) % len(BINANCE_ENDPOINTS)
        except websockets.ConnectionClosedError as e:
            sys.stderr.write(f"[WS] Disconnected ({e}); will retry...\n")
        except Exception as e:
            sys.stderr.write(f"[WS] Error ({e}); will retry...\n")

        # Exponential backoff before reconnect
        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(RECONNECT_MAX, max(RECONNECT_INITIAL, backoff * 2))

    sys.stdout.write("[Kafka] Flushing...\n")
    # Give a bit more time with larger batches
    producer.flush(15)


# ── Signals & entrypoint ──────────────────────────────────────────────────────
def _handle_signal(signame: str) -> None:
    sys.stdout.write(f"[Signal] {signame} received, shutting down...\n")
    _shutdown.set()


async def main() -> None:
    sys.stdout.write(
        f"""Starting Binance → Kafka producer
  Kafka bootstrap: {KAFKA_BOOTSTRAP}
  Kafka topic:     {KAFKA_TOPIC}
  Symbols:         {", ".join(SYMBOLS)}
"""
    )
    producer = build_kafka_producer()

    # Graceful shutdown hooks
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig.name)
        except NotImplementedError:
            # Windows / limited environments
            pass

    await stream_symbols(SYMBOLS, producer)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
