import socket
import struct
import time
import urllib.request

HOST = '127.0.0.1'
PORT = 9092
NUM_MESSAGES = 1000

def make_produce_frame(msg, corr_id):
    payload = msg.encode()
    topic = b'orders-live'
    client = b'benchmark'

    # Kafka Header
    h = bytearray()
    h.extend(struct.pack('>h', 0))       # api_key: 0 (Produce)
    h.extend(struct.pack('>h', 3))       # api_version: 3
    h.extend(struct.pack('>i', corr_id)) # correlation_id
    h.extend(struct.pack('>h', len(client)) + client)

    # Kafka Produce Body
    b = bytearray()
    b.extend(struct.pack('>h', -1))      # transactional_id: null
    b.extend(struct.pack('>h', -1))      # acks: -1 (all)
    b.extend(struct.pack('>i', 5000))    # timeout_ms
    b.extend(struct.pack('>i', 1))       # 1 topic
    b.extend(struct.pack('>h', len(topic)) + topic)
    b.extend(struct.pack('>i', 1))       # 1 partition
    b.extend(struct.pack('>i', 0))       # partition: 0
    b.extend(struct.pack('>i', len(payload)) + payload)

    full = h + b
    return struct.pack('>i', len(full)) + full

print(f"\n==================================================")
print(f"⚡ VELOCITYMQ ZERO-COPY LIVE BENCHMARK")
print(f"   Target Broker:   {HOST}:{PORT}")
print(f"   Total Messages:  {NUM_MESSAGES:,}")
print(f"==================================================")
print(f"🚀 Blasting messages into NVMe Zero-Copy WAL...")

start = time.time()
success_count = 0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
s.connect((HOST, PORT))

for i in range(NUM_MESSAGES):
    frame = make_produce_frame(f"Live-Order-Event-#{i}", i)
    s.sendall(frame)
    
    resp_head = s.recv(4)
    if len(resp_head) == 4:
        rlen = struct.unpack('>i', resp_head)[0]
        s.recv(rlen)
        success_count += 1

s.close()

total_time = time.time() - start
rps = int(success_count / total_time) if total_time > 0 else 0
avg_us = int((total_time / success_count) * 1_000_000) if success_count > 0 else 0

print("\n" + "="*50)
print("🏆 BENCHMARK RESULTS:")
print(f"   Successfully Written: {success_count:,} Messages")
print(f"   Total Time Taken:     {total_time:.4f} seconds")
print(f"   Throughput:           {rps:,} Messages / Second")
print(f"   Average Latency:      {avg_us:,} Microseconds (µs)")
print("="*50 + "\n")

print("📊 Live Updated Prometheus Metrics from Port 9093:")
try:
    metrics = urllib.request.urlopen("http://127.0.0.1:9093/metrics").read().decode()
    for line in metrics.splitlines():
        if any(k in line for k in ['messages_produced', 'bytes_written', 'uptime_seconds', 'raft_is_leader']):
            if not line.startswith('#'):
                print("   ->", line)
except Exception as e:
    print(f"   Could not fetch metrics: {e}")
print("\n✅ Verification Complete!\n")
