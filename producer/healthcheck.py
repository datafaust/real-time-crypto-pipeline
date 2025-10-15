import os, sys
from confluent_kafka import Producer
bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
try:
    p = Producer({"bootstrap.servers": bootstrap})
    md = p.list_topics(timeout=2.0)
    sys.exit(0 if md.brokers else 1)
except Exception:
    sys.exit(1)
