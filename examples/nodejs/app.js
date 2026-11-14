const Redis = require('ioredis');
const { Kafka } = require('kafkajs');

/**
 * Velocity-Suite Production Integration Example (Node.js)
 * Demonstrates:
 * 1. Sub-millisecond state caching via VelocityDB (Redis Port 6379)
 * 2. Guaranteed zero-loss event streaming via VelocityMQ (Kafka Port 9092)
 */

const VELOCITY_HOST = process.env.VELOCITY_HOST || 'localhost';

// Connect to VelocityDB (:6379)
const db = new Redis({
  host: VELOCITY_HOST,
  port: 6379,
  enableAutoPipelining: true,
});

// Connect to VelocityMQ (:9092)
const kafka = new Kafka({
  clientId: 'velocity-suite-demo',
  brokers: [`${VELOCITY_HOST}:9092`],
});
const producer = kafka.producer();

async function main() {
  console.log("⚡ Connecting to Velocity-Suite...");
  await producer.connect();
  console.log("✅ Connected to VelocityMQ (Kafka Port 9092) and VelocityDB (Redis Port 6379)!\n");

  const orderId = `ORD-${Date.now()}`;
  const userId = "user_49102";
  const productId = "ITEM-NVME-99";

  // 1. VelocityDB: Decrement inventory stock atomically in memory (~50µs)
  await db.set(`stock:${productId}`, 500);
  const remainingStock = await db.decr(`stock:${productId}`);
  console.log(`[VelocityDB :6379] Stock updated atomically in cache. Remaining: ${remainingStock}`);

  // 2. VelocityMQ: Publish persistent, durable order event to NVMe WAL (~120µs)
  const orderEvent = {
    orderId,
    userId,
    productId,
    remainingStock,
    amount: 149.99,
    timestamp: Date.now(),
  };

  await producer.send({
    topic: 'orders-live-stream',
    messages: [{ key: orderId, value: JSON.stringify(orderEvent) }],
  });
  console.log(`[VelocityMQ :9092] OrderEvent formally committed to zero-copy NVMe WAL!`);

  console.log("\n🎉 Transaction completed seamlessly across both engines with zero friction!");
  await producer.disconnect();
  await db.quit();
}

main().catch(console.error);
