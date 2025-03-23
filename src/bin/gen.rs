// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{FutureProducer, FutureRecord};
// use std::sync::Arc;
// use std::time::{Duration, Instant};
// use tokio::runtime::Runtime;
// use tokio::sync::Semaphore;
// use tokio::task;
// use uuid::Uuid;
// use rand::{thread_rng, Rng};

// const JSON_TEMPLATE: &str = r#"{
//     "id": "{}",
//     "timestamp": {},
//     "user_id": "{}",
//     "device_id": "{}",
//     "location": {
//         "latitude": {:.6},
//         "longitude": {:.6}
//     },
//     "metrics": {
//         "cpu": {:.2},
//         "memory": {:.2},
//         "disk": {:.2},
//         "network": {:.2}
//     },
//     "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
//     "payload": "{}"
// }"#;

// // Generate payload to reach ~1KB total message size
// fn generate_payload() -> String {
//     // Calculate approximately how many chars we need to reach 1KB
//     // considering the template size and other fields
//     let base_size = 250; // Approximate size of the template with other fields
//     let target_size = 1024;
//     let padding_size = target_size - base_size;
    
//     // Generate random string of appropriate length
//     (0..padding_size)
//         .map(|_| thread_rng().sample(rand::distr::Alphanumeric) as char)
//         .collect()
// }

// fn generate_event() -> String {
//     let id = Uuid::new_v4().to_string();
//     let timestamp = std::time::SystemTime::now()
//         .duration_since(std::time::UNIX_EPOCH)
//         .unwrap()
//         .as_secs();
//     let user_id = Uuid::new_v4().to_string();
//     let device_id = Uuid::new_v4().to_string();
    
//     let mut rng = thread_rng();
//     let lat = rng.gen_range(-90.0..90.0);
//     let lon = rng.gen_range(-180.0..180.0);
    
//     let cpu = rng.gen_range(0.0..100.0);
//     let memory = rng.gen_range(0.0..100.0);
//     let disk = rng.gen_range(0.0..100.0);
//     let network = rng.gen_range(0.0..100.0);
    
//     let payload = generate_payload();
    
//     format!(
//         r#"{{
//     "id": "{}",
//     "timestamp": {},
//     "user_id": "{}",
//     "device_id": "{}",
//     "location": {{
//         "latitude": {:.6},
//         "longitude": {:.6}
//     }},
//     "metrics": {{
//         "cpu": {:.2},
//         "memory": {:.2},
//         "disk": {:.2},
//         "network": {:.2}
//     }},
//     "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
//     "payload": "{}"
// }}"#,
//         id, timestamp, user_id, device_id,
//         lat, lon, cpu, memory, disk, network, payload
//     )
// }

// // Pre-generate a pool of messages
// fn generate_message_pool(pool_size: usize) -> Vec<String> {
//     println!("Pre-generating {} messages...", pool_size);
//     let start = Instant::now();
    
//     let pool: Vec<String> = (0..pool_size)
//         .map(|_| generate_event())
//         .collect();
    
//     println!("Message pool generated in {:.2?}", start.elapsed());
//     pool
// }

// async fn produce_batch(
//     topic: &str,
//     producer: Arc<FutureProducer>,
//     message_pool: Arc<Vec<String>>,
//     batch_size: usize,
//     semaphore: Arc<Semaphore>,
// ) {
//     let _permit = semaphore.acquire().await.unwrap();
//     let pool_size = message_pool.len();
//     let mut rng = thread_rng();
    
//     let mut futures = Vec::with_capacity(batch_size);
    
//     for _ in 0..batch_size {
//         // Select a random pre-generated message
//         let msg_index = rng.gen_range(0..pool_size);
//         let event = &message_pool[msg_index];

//         let topic = topic.to_string();
//         let event = event.clone(); // Clone the string from the pool

//         let key = Uuid::new_v4().to_string();
//         let producer_clone = Arc::clone(&producer);
//         let future = task::spawn(async move {
//             let record = FutureRecord::to(&topic)
//                 .payload(&event)
//                 .key(&key);
                
//                 producer_clone.send(record, Duration::from_millis(0)).await
//         });
        
//         futures.push(future);
//     }
    
//     for future in futures {
//         let _ = future.await;  // In production, handle errors
//     }
// }

// #[tokio::main]
// async fn main() {
//     // Kafka configuration
//     let producer: FutureProducer = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .set("message.timeout.ms", "5000")
//         .set("queue.buffering.max.messages", "100000")
//         .set("queue.buffering.max.ms", "5")
//         .set("batch.size", "100000")  // Increased batch size
//         .set("linger.ms", "5")        // Small delay to gather more messages
//         .set("compression.type", "none") // For maximum throughput
//         .set("acks", "1")             // No acknowledgement for maximum throughput
//         .create()
//         .expect("Producer creation failed");
    
//     let producer = Arc::new(producer);
//     let topic = "demo-prod-test";
    
//     // Pre-generate message pool
//     let pool_size = 10_000; // Adjust based on memory constraints
//     let message_pool = Arc::new(generate_message_pool(pool_size));
    
//     // Concurrency control
//     let num_cores = 4;
//     let concurrent_batches = num_cores * 8; // Adjust based on your hardware
//     let batch_size = 1000; // Adjust based on testing
    
//     let semaphore = Arc::new(Semaphore::new(concurrent_batches));
    
//     // Target events per second
//     let events_per_second = 1_000_000;
//     let total_events = events_per_second * 10; // Run for 10 seconds
//     let total_batches = total_events / batch_size;
    
//     println!("Starting producer with {} cores", num_cores);
//     println!("Target: {} events/second", events_per_second);
//     println!("Batch size: {}", batch_size);
//     println!("Concurrent batches: {}", concurrent_batches);
//     println!("Message pool size: {}", pool_size);
    
//     let start = Instant::now();
    
//     let mut handles = Vec::new();
//     for i in 0..total_batches {
//         let producer_clone = Arc::clone(&producer);
//         let semaphore_clone = Arc::clone(&semaphore);
//         let message_pool_clone = Arc::clone(&message_pool);
        
//         // Use tokio::spawn instead of task::spawn to ensure Send bound is enforced
//         let handle = tokio::spawn(async move {
//             produce_batch(
//                 topic, 
//                 producer_clone,
//                 message_pool_clone,
//                 batch_size, 
//                 semaphore_clone
//             );
//         }).await.unwrap();
        
//         handles.push(handle);
        
//         // Simple rate limiting
//         if (i + 1) % ((total_batches / 10) as usize) == 0 {
//             println!("Progress: {}%", ((i + 1) * 100) / total_batches);
//         }
//     }
    
//     for handle in handles {
//         let _ = handle;
//     }
    
//     let elapsed = start.elapsed();
//     let events_sent = total_batches * batch_size;
//     let rate = events_sent as f64 / elapsed.as_secs_f64();
    
//     println!("Completed in {:.2?}", elapsed);
//     println!("Sent {} events", events_sent);
//     println!("Rate: {:.2} events/second", rate);
// }
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task;
use uuid::Uuid;
use rand::{Rng, prelude::Distribution}; // Using prelude::Distribution, not the private one
 
const JSON_TEMPLATE: &str = r#"{
    "id": "%s",
    "timestamp": %d,
    "user_id": "%s",
    "device_id": "%s",
    "location": {
        "latitude": %.6f,
        "longitude": %.6f
    },
    "metrics": {
        "cpu": %.2f,
        "memory": %.2f,
        "disk": %.2f,
        "network": %.2f
    },
    "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
    "payload": "%s"
}"#;

// Generate payload to reach ~1KB total message size
fn generate_payload() -> String {
    // Calculate approximately how many chars we need to reach 1KB
    // considering the template size and other fields
    let base_size = 250; // Approximate size of the template with other fields
    let target_size = 1024;
    let padding_size = target_size - base_size;
    
    // Generate random string of appropriate length
    // Updated to use rng() instead of thread_rng()
    (0..padding_size)
        .map(|_| rand::rng().sample(rand::distr::Alphanumeric) as char)
        .collect()
}

fn generate_event() -> String {
    // Updated to use Uuid::new_v7() instead of new_v4()
    // Or using Uuid::now_v7() which is time-based and likely available
    let id = Uuid::new_v4().to_string();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let user_id = Uuid::new_v4().to_string();
    let device_id = Uuid::new_v4().to_string();
    
    // Updated to use rng() and random_range()
    let mut rng = rand::rng();
    let lat = rng.random_range(-90.0..90.0);
    let lon = rng.random_range(-180.0..180.0);
    
    let cpu = rng.random_range(0.0..100.0);
    let memory = rng.random_range(0.0..100.0);
    let disk = rng.random_range(0.0..100.0);
    let network = rng.random_range(0.0..100.0);
    
    let payload = generate_payload();
    
    // Fixed format string issue with proper format
    format!(
        r#"{{
    "id": "{}",
    "timestamp": {},
    "user_id": "{}",
    "device_id": "{}",
    "location": {{
        "latitude": {:.6},
        "longitude": {:.6}
    }},
    "metrics": {{
        "cpu": {:.2},
        "memory": {:.2},
        "disk": {:.2},
        "network": {:.2}
    }},
    "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
    "payload": "{}"
}}"#,
        id, timestamp, user_id, device_id,
        lat, lon, cpu, memory, disk, network, payload
    )
}

// Pre-generate a pool of messages
fn generate_message_pool(pool_size: usize) -> Vec<String> {
    println!("Pre-generating {} messages...", pool_size);
    let start = Instant::now();
    
    let pool: Vec<String> = (0..pool_size)
        .map(|_| generate_event())
        .collect();
    
    println!("Message pool generated in {:.2?}", start.elapsed());
    pool
}

async fn produce_batch(
    producer: Arc<FutureProducer>,
    topic: &str,
    message_pool: Arc<Vec<String>>,
    batch_size: usize,
    semaphore: Arc<Semaphore>,
) {
    println!("Producing batch...");
    let _permit = semaphore.acquire().await.unwrap();
    let pool_size = message_pool.len();
    
    // Updated to use rng() and random_range()
    let mut rng = rand::rng();
    
    let mut futures = Vec::with_capacity(batch_size);
    
    for _ in 0..batch_size {
        // Select a random pre-generated message
        let msg_index = rng.random_range(0..pool_size);
        let event = &message_pool[msg_index];
        
        let producer = Arc::clone(&producer);
        let topic = topic.to_string();
        let event = event.clone(); // Clone the string from the pool
        let key = Uuid::new_v4().to_string();
        // Using Uuid::now_v7() instead of new_v4()
        let future = task::spawn(async move {
            let record = FutureRecord::to(&topic)
                .payload(&event)
                .key(&key);
                
            // Properly handle send errors
            println!("Sending message: {}", event);
            match producer.send(record, Duration::from_millis(0)).await {
                Ok(delivery) => {
                    // Successfully queued the message
                },
                Err((err, _)) => {
                    eprintln!("Failed to send message: {}", err);
                }
            }
        });
        
        futures.push(future);
    }
    
    for future in futures {
        let _ = future.await;  // In production, handle errors
    }
}

#[tokio::main]
async fn main() {
    // Kafka configuration with proper connection and error handling
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9093")  // Use host.docker.internal:9092 if running in Docker
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "100000")
        .set("queue.buffering.max.ms", "5")
        .set("batch.size", "100000")  // Increased batch size
        .set("linger.ms", "5")        // Small delay to gather more messages
        .set("compression.type", "none") // For maximum throughput
        .set("acks", "1")             // Wait for leader acknowledgment to confirm delivery
        .set("debug", "all")          // Enable debug info to see connection issues
        .set("statistics.interval.ms", "5000") // Print stats every 5 seconds
        .create()
        .expect("Producer creation failed");
    
    let producer = Arc::new(producer);
    let topic = "high-throughput-topic";
    
    // Pre-generate message pool
    let pool_size = 10_000; // Adjust based on memory constraints
    let message_pool = Arc::new(generate_message_pool(pool_size));
    
    // Concurrency control
    // Added num_cpus dependency
    let num_cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4); // Fallback to 4 if we can't determine
    
    let concurrent_batches = num_cores * 8; // Adjust based on your hardware
    let batch_size = 1000; // Adjust based on testing
    
    let semaphore = Arc::new(Semaphore::new(concurrent_batches));
    
    // Target events per second
    let events_per_second = 1_000_000;
    let total_events = events_per_second * 10; // Run for 10 seconds
    let total_batches = total_events / batch_size;
    
    println!("Starting producer with {} cores", num_cores);
    println!("Target: {} events/second", events_per_second);
    println!("Batch size: {}", batch_size);
    println!("Concurrent batches: {}", concurrent_batches);
    println!("Message pool size: {}", pool_size);
    
    let start = Instant::now();
    
    let mut handles = Vec::new();
    for i in 0..total_batches {
        let producer_clone = Arc::clone(&producer);
        let semaphore_clone = Arc::clone(&semaphore);
        let message_pool_clone = Arc::clone(&message_pool);
        
        let handle = task::spawn(async move {
            produce_batch(
                producer_clone, 
                topic, 
                message_pool_clone,
                batch_size, 
                semaphore_clone
            ).await;
        });
        
        handles.push(handle);
        
        // Simple rate limiting
        if (i + 1) % ((total_batches / 10) as usize) == 0 {
            println!("Progress: {}%", ((i + 1) * 100) / total_batches);
        }
    }
    
    for handle in handles {
        let _ = handle.await;
    }
    
    let elapsed = start.elapsed();
    let events_sent = total_batches * batch_size;
    let rate = events_sent as f64 / elapsed.as_secs_f64();
    
    println!("Completed in {:.2?}", elapsed);
    println!("Sent {} events", events_sent);
    println!("Rate: {:.2} events/second", rate);
}