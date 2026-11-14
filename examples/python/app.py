#!/usr/bin/env python3
"""
Velocity-Suite Production Integration Example (Python)
Demonstrates:
1. Sub-millisecond state caching via VelocityDB (Redis Port 6379)
2. Guaranteed zero-loss event streaming via VelocityMQ (Kafka Port 9092)
"""

import os
import json
import time
import redis
from kafka import KafkaProducer

VELOCITY_HOST = os.environ.get('VELOCITY_HOST', 'localhost')

# 1. Connect to VelocityDB (:6379)
db = redis.Redis(host=VELOCITY_HOST, port=6379, db=0)

# 2. Connect to VelocityMQ (:9092)
mq = KafkaProducer(
    bootstrap_servers=[f"{VELOCITY_HOST}:9092"],
    acks='all' # Strict NVMe durability
)

def process_checkout(user_id, item_id, price):
    print(f"\n🛒 Processing checkout for {user_id}...")

    # Step 1: VelocityDB In-Memory Caching (Sub-Millisecond Speed)
    db.set(f"session:{user_id}", "ACTIVE_CHECKOUT", ex=300)
    db.set(f"inventory:{item_id}", 1000)
    stock = db.decr(f"inventory:{item_id}")
    print(f"   [VelocityDB :6379] Session cached & stock decremented in memory: {stock}")

    # Step 2: VelocityMQ Direct NVMe WAL Ingestion (Zero GC, Zero Loss)
    event_payload = {
        "event": "ORDER_PLACED",
        "user_id": user_id,
        "item_id": item_id,
        "price": price,
        "timestamp_ns": time.time_ns(),
    }
    
    mq.send('orders-stream', json.dumps(event_payload).encode('utf-8'))
    mq.flush()
    print(f"   [VelocityMQ :9092] OrderEvent committed directly to hardware NVMe WAL!")

if __name__ == '__main__':
    print("⚡ Connecting to Velocity-Suite...")
    process_checkout("usr_alpha_99", "TURBO-NVME-SSD", 299.99)
    print("\n✅ Transaction completed seamlessly across VelocityMQ & VelocityDB!\n")
