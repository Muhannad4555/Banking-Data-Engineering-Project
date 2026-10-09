"""Reads Debezium events from Kafka and writes them as JSON-lines files to S3 (the raw layer)."""
import os
import time
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ["S3_BUCKET"]
TOPICS = ["banking.public.customers", "banking.public.accounts", "banking.public.transactions"]
MAX_MESSAGES, MAX_SECONDS = 100, 30

# boto3 picks AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY up from the environment by itself
s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "s3-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # commit only after the file is safely in S3
})
consumer.subscribe(TOPICS)

batch = {}  # topic -> list of raw JSON strings
count, started = 0, time.time()


def flush():
    global batch, count
    now = datetime.now(timezone.utc)
    for topic, lines in batch.items():
        key = f"raw/{topic.split('.')[-1]}/dt={now:%Y-%m-%d}/{now:%H%M%S%f}.jsonl"
        s3.put_object(Bucket=BUCKET, Key=key, Body="\n".join(lines).encode())
        print(f"wrote {len(lines)} events to s3://{BUCKET}/{key}")
    consumer.commit()  # a crash between put and commit re-sends events: at-least-once
    batch, count = {}, 0


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:  # topic appears with its first event
                    raise RuntimeError(msg.error())
            elif msg.value() is not None:  # skip tombstones (null value after a delete)
                if not count:
                    started = time.time()
                batch.setdefault(msg.topic(), []).append(msg.value().decode())
                count += 1
        if count and (count >= MAX_MESSAGES or time.time() - started >= MAX_SECONDS):
            flush()
except KeyboardInterrupt:
    if count:
        flush()
finally:
    consumer.close()
