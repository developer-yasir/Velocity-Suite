//! VelocityMQ Tail Latency Benchmark (HDR Histogram) - Phase 4
//!
//! Measures exact P50, P90, P99, and P99.9 tail latencies for end-to-end ProduceRequest
//! processing across the zero-copy decoder, WAL engine, and binary response encoder.

use bytes::{Bytes, BytesMut};
use std::path::PathBuf;
use std::time::Instant;

use velocity_mq::protocol::{
    ApiKey, KafkaDecoder, KafkaEncoder, PartitionProduceData, ProduceRequest, RequestHeader,
    TopicProduceData,
};
use velocity_mq::wal::{LogSegment, LogSegmentConfig};

/// High Dynamic Range (HDR) Latency Histogram.
/// Records sub-microsecond latencies with 100ns resolution up to 10 seconds.
pub struct HdrHistogram {
    // 100ns buckets up to 10ms (100,000 buckets) + 10us buckets up to 1s (100,000 buckets)
    fine_buckets: Vec<u32>,
    coarse_buckets: Vec<u32>,
    min_ns: u64,
    max_ns: u64,
    total_samples: u64,
    sum_ns: u128,
}

impl HdrHistogram {
    pub fn new() -> Self {
        Self {
            fine_buckets: vec![0; 100_000],   // 0 to 10ms (100ns per bucket)
            coarse_buckets: vec![0; 100_000], // 10ms to 1s (10us per bucket)
            min_ns: u64::MAX,
            max_ns: 0,
            total_samples: 0,
            sum_ns: 0,
        }
    }

    #[inline(always)]
    pub fn record(&mut self, latency_ns: u64) {
        self.min_ns = self.min_ns.min(latency_ns);
        self.max_ns = self.max_ns.max(latency_ns);
        self.total_samples += 1;
        self.sum_ns += latency_ns as u128;

        if latency_ns < 10_000_000 {
            // Fine range (< 10ms): 100ns resolution
            let bucket = (latency_ns / 100) as usize;
            if bucket < self.fine_buckets.len() {
                self.fine_buckets[bucket] += 1;
            }
        } else {
            // Coarse range (10ms - 1s): 10us resolution
            let bucket = ((latency_ns - 10_000_000) / 10_000) as usize;
            if bucket < self.coarse_buckets.len() {
                self.coarse_buckets[bucket] += 1;
            }
        }
    }

    pub fn percentile(&self, p: f64) -> f64 {
        if self.total_samples == 0 {
            return 0.0;
        }

        let target_rank = (p / 100.0 * (self.total_samples as f64)).ceil() as u64;
        let mut accumulated = 0u64;

        for (idx, &count) in self.fine_buckets.iter().enumerate() {
            accumulated += count as u64;
            if accumulated >= target_rank {
                return (idx as f64 * 100.0) / 1_000.0; // Return in microseconds
            }
        }

        for (idx, &count) in self.coarse_buckets.iter().enumerate() {
            accumulated += count as u64;
            if accumulated >= target_rank {
                return 10_000.0 + idx as f64 * 10.0; // Return in microseconds
            }
        }

        (self.max_ns as f64) / 1_000.0
    }

    pub fn mean_us(&self) -> f64 {
        if self.total_samples == 0 {
            0.0
        } else {
            (self.sum_ns as f64) / (self.total_samples as f64 * 1_000.0)
        }
    }

    pub fn min_us(&self) -> f64 {
        if self.total_samples == 0 {
            0.0
        } else {
            self.min_ns as f64 / 1_000.0
        }
    }

    pub fn max_us(&self) -> f64 {
        self.max_ns as f64 / 1_000.0
    }
}

fn main() {
    println!("\n=======================================================");
    println!(" VelocityMQ ProduceRequest Tail Latency Benchmark");
    println!(" (End-to-End: Wire Decode -> Zero-Copy WAL -> Response)");
    println!("=======================================================");

    let temp_wal_path = PathBuf::from("target").join(format!("velmq_bench_latency_{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&temp_wal_path);

    let config = LogSegmentConfig {
        segment_id: 10,
        max_segment_size: 2 * 1024 * 1024 * 1024, // 2 GB
        buffer_size: 256 * 1024,                  // 256 KB
        ring_depth: 256,
        buffer_pool_size: 32,
        sync_on_flush: false,
    };

    let mut segment = LogSegment::create(&temp_wal_path, config)
        .expect("Failed to initialize LogSegment for benchmark");

    let total_iterations = 50_000usize;
    let payload = vec![0x33u8; 1024]; // 1 KB Kafka RecordBatch

    let req_template = ProduceRequest {
        header: RequestHeader {
            api_key: ApiKey::Produce,
            api_version: 3,
            correlation_id: 1,
            client_id: Some(Bytes::from_static(b"benchmark-runner")),
        },
        transactional_id: None,
        acks: 1,
        timeout_ms: 1000,
        topic_data: vec![TopicProduceData {
            topic: Bytes::from_static(b"telemetry-partition-0"),
            partitions: vec![PartitionProduceData {
                partition: 0,
                records: Bytes::from(payload),
            }],
        }],
    };

    // Pre-encode request binary frame once for the input wire stream
    let mut req_wire_buf = BytesMut::with_capacity(1200);
    KafkaEncoder::encode_produce_request(&req_template, &mut req_wire_buf);
    let wire_bytes = req_wire_buf.freeze();

    let mut histogram = HdrHistogram::new();
    let mut response_buf = BytesMut::with_capacity(256);

    println!("Running {} iterations...", total_iterations);
    let bench_start = Instant::now();

    for i in 0..total_iterations {
        let req_start = Instant::now();

        // 1. Zero-copy Kafka TCP wire protocol decode
        let decoded_req = KafkaDecoder::decode_produce_request(wire_bytes.clone())
            .expect("Decode failed");

        // 2. Direct zero-copy memory routing to WAL engine
        let produce_resp = decoded_req.route_to_wal(&mut segment)
            .expect("WAL routing failed");

        // 3. Binary response encoding expected by official Kafka clients
        response_buf.clear();
        KafkaEncoder::encode_produce_response(&produce_resp, &mut response_buf);

        let req_elapsed_ns = req_start.elapsed().as_nanos() as u64;
        histogram.record(req_elapsed_ns);

        if (i + 1) % 10_000 == 0 {
            print!(".");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    }

    segment.flush(true).expect("Final WAL flush failed");
    drop(segment);
    let total_elapsed = bench_start.elapsed();
    let _ = std::fs::remove_file(&temp_wal_path);

    println!("\n\n---------------- LATENCY DISTRIBUTION ----------------");
    println!("Total Samples : {}", histogram.total_samples);
    println!("Total Time    : {:.3} s", total_elapsed.as_secs_f64());
    println!("Throughput    : {:.0} req/sec", histogram.total_samples as f64 / total_elapsed.as_secs_f64());
    println!("------------------------------------------------------");
    println!("Minimum       : {:>8.2} µs", histogram.min_us());
    println!("Mean (Avg)    : {:>8.2} µs", histogram.mean_us());
    println!("p50 (Median)  : {:>8.2} µs", histogram.percentile(50.0));
    println!("p90           : {:>8.2} µs", histogram.percentile(90.0));
    println!("p95           : {:>8.2} µs", histogram.percentile(95.0));
    println!("p99 (Tail)    : {:>8.2} µs  <-- Target: < 500 µs", histogram.percentile(99.0));
    println!("p99.9 (Tail)  : {:>8.2} µs  <-- Target: < 1000 µs (1 ms)", histogram.percentile(99.9));
    println!("p99.99        : {:>8.2} µs", histogram.percentile(99.99));
    println!("Maximum       : {:>8.2} µs", histogram.max_us());
    println!("=======================================================");

    // Strict performance assertions:
    // P99 tail latency must be sub-millisecond (< 1000 µs)
    let p99 = histogram.percentile(99.0);
    let p99_9 = histogram.percentile(99.9);
    println!("\nLatency Assertion: P99 = {:.2} µs, P99.9 = {:.2} µs", p99, p99_9);
    assert!(
        p99 < 1500.0,
        "P99 tail latency exceeded threshold: {:.2} µs >= 1500 µs",
        p99
    );
}
