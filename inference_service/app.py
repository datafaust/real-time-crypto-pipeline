# /inference_service/app.py
import os, json, time, threading, pickle
from datetime import datetime, timezone
from collections import defaultdict, deque

import numpy as np
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

MODEL_PATH = os.getenv("MODEL_PATH", "/models/model.pkl")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
GROUP_ID = os.getenv("GROUP_ID", "inference_service_v2")  # bump this on rollout
IN_TOPIC = os.getenv("IN_TOPIC", "ohlcv_1m_out_json")
OUT_TOPIC = os.getenv("OUT_TOPIC", "predictions_1m")
EMA_WINDOW = int(os.getenv("EMA_WINDOW", "5"))

# Avro serializer (value)
sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
with open("/schemas/ohlcv_1m_predictions.avsc", "r") as f:
    pred_schema_str = f.read()
def pred_to_dict(rec, ctx): return rec
pred_serializer = AvroSerializer(sr, pred_schema_str, pred_to_dict)

# Kafka clients
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
producer = Producer({"bootstrap.servers": BOOTSTRAP})

# State
MAX_LAGS = 10
recent_volumes = defaultdict(lambda: deque(maxlen=MAX_LAGS))
MODEL = {"version": "ema-fallback", "per_symbol": {}, "n_features": {}}

def load_model():
    global MODEL
    try:
        with open(MODEL_PATH, "rb") as f:
            MODEL = pickle.load(f)
        # Sanity: ensure n_features is present and int
        nmap = {str(k): int(v) for k, v in MODEL.get("n_features", {}).items()}
        MODEL["n_features"] = nmap
        print(f"[inference] Loaded model {MODEL.get('version')} "
              f"symbols={list(MODEL.get('per_symbol', {}).keys())} "
              f"n_features={nmap}")
    except Exception as e:
        print(f"[inference] model load failed, using EMA fallback: {e}")

def hot_reload_watch():
    last_mtime = None
    while True:
        try:
            mtime = os.path.getmtime(MODEL_PATH)
            if last_mtime is None:
                last_mtime = mtime
            elif mtime != last_mtime:
                last_mtime = mtime
                load_model()
        except FileNotFoundError:
            pass
        time.sleep(5)

def _deliver(err, msg):
    if err:
        print(f"[inference] delivery error: {err}")

def predict(symbol: str, ts_ms: int):
    vols = np.array(recent_volumes[symbol], dtype=float)
    if vols.size == 0:
        return None

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    model = MODEL.get("per_symbol", {}).get(symbol)
    nmap = MODEL.get("n_features", {})
    expected = nmap.get(symbol)

    if model is not None and expected is not None and expected > 0:
        use = min(len(vols), expected)
        X = np.flip(vols[-use:])
        if use < expected:
            X = np.pad(X, (0, expected - use), constant_values=0.0)

        # Hard guard: never send wrong sized features
        if X.shape[0] != expected:
            # Should not happen, but just in case
            print(f"[inference] guard mismatch {symbol}: Xlen={len(X)} expected={expected} -> skip")
            return None

        try:
            yhat = float(model.predict([X])[0])
        except Exception as e:
            print(f"[inference] predict error for {symbol}: exp={expected} recent={len(vols)} -> {e}")
            return None

        return {
            "SYMBOL_VALUE": symbol,
            "WINDOW_START": ts_ms,
            "PREDICTED_VOLUME": max(yhat, 0.0),
            "MODEL_VERSION": MODEL.get("version", "unknown"),
            "FEATURES_LAG_MINUTES": list(range(1, expected + 1)),
            "CREATED_AT": now_ms
        }

    # EMA fallback
    alpha = 2 / (EMA_WINDOW + 1)
    ema = vols[0]
    for v in vols[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return {
        "SYMBOL_VALUE": symbol,
        "WINDOW_START": ts_ms,
        "PREDICTED_VOLUME": float(ema),
        "MODEL_VERSION": f"ema_{EMA_WINDOW}",
        "FEATURES_LAG_MINUTES": list(range(1, min(len(vols), 5) + 1)),
        "CREATED_AT": now_ms
    }

def main():
    load_model()
    threading.Thread(target=hot_reload_watch, daemon=True).start()
    consumer.subscribe([IN_TOPIC])
    print(f"[inference] Consuming {IN_TOPIC} → Producing {OUT_TOPIC}")
    seen = produced = skipped = 0

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[inference] consumer error: {msg.error()}")
            continue
        try:
            raw = msg.value()
            if raw is None:
                skipped += 1
                continue

            payload = json.loads(raw.decode("utf-8")) if isinstance(raw, (bytes, bytearray)) else raw
            symbol = payload.get("SYMBOL_VALUE") or payload.get("SYMBOL")
            ts_ms  = payload.get("WINDOW_START")
            vol    = payload.get("VOLUME")
            if symbol is None or ts_ms is None or vol is None:
                skipped += 1
                continue

            recent_volumes[symbol].append(float(vol))
            out = predict(symbol, int(ts_ms))
            seen += 1

            if out:
                producer.produce(
                    OUT_TOPIC,
                    key=str(symbol).encode("utf-8"),
                    value=pred_serializer(out, SerializationContext(OUT_TOPIC, MessageField.VALUE)),
                    on_delivery=_deliver
                )
                producer.poll(0)
                produced += 1

            if (seen + skipped) % 50 == 0:
                print(f"[inference] seen={seen}, produced={produced}, skipped={skipped}")

        except Exception as e:
            print(f"[inference] failed to process: {e}")

    # not reached
    # consumer.close()

if __name__ == "__main__":
    main()
