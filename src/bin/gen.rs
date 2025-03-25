// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{Producer, ThreadedProducer};
// use std::time::{Duration, Instant};
// use tokio::task;
// use serde_json::json;
// use std::sync::Arc;
// use std::mem::size_of;
// use serde_json::Value;

// #[tokio::main]
// async fn main() {
//     let producer: ThreadedProducer<_> = ClientConfig::new()
//     .set("bootstrap.servers", "localhost:9092")
//     .set("queue.buffering.max.ms", "10")
//     // .set("socket.send.buffer.bytes", "1048576") // 1 MB
//     // .set("socket.receive.buffer.bytes", "1048576") // 1 MB
//     .set("queue.buffering.max.messages", "1000000")
//     .set("max.in.flight.requests.per.connection", "100")
//     .create()
//     .expect("Producer creation error");

//     let arc_producer  = Arc::new(producer);
//     let num_producers = 1;
//     let mut producers = Vec::new();
//     let duration_seconds = 10;
//     let start_time = Instant::now();    
//     let messages_per_second = 1000000;

//     for i in 0..num_producers {
//         producers.push(task::spawn(send_messages(i, arc_producer.clone(),start_time,duration_seconds,messages_per_second)));
//     }

//     for producer in producers {
//         let result = producer.await;
//         println!("Producer result: {:?}", result);
//     }
// }

// pub fn sizeof_val(v: &serde_json::Value) -> usize {
//     size_of::<serde_json::Value>()
//         + match v {
//             Value::Null => 0,
//             Value::Bool(_) => 0,
//             Value::Number(_) => 0, // incorrect if arbitrary_precision is enabled
//             Value::String(s) => s.capacity(),
//             Value::Array(a) => a.iter().map(sizeof_val).sum(),
//             Value::Object(o) => o
//                 .iter()
//                 .map(|(k, v)| {
//                     size_of::<String>() + k.capacity() + sizeof_val(v) + size_of::<usize>() * 3
//                     //crude approximation, each map entry has 3 words of overhead
//                 })
//                 .sum(),
//         }
//    }
   
// use tokio::time::sleep;

// async fn send_messages(partition: i32, producer: Arc<ThreadedProducer<rdkafka::producer::DefaultProducerContext>>,start_time: Instant,duration_seconds: u64,messages_per_second: u64) {
    
//     const TARGET_JSON_SIZE: usize = 1024; // Change this value to 512, 2048, etc.

//     let start = Instant::now();
//     let flush_interval = 500_000;

//     let mut next_send_time = Instant::now();
//     // Calculate the interval between messages to achieve the target rate
//     let message_interval = if messages_per_second > 0 {
//         Duration::from_micros(1_000_000 / messages_per_second)
//     } else {
//         Duration::from_micros(0)
//     };
    
//     while start_time.elapsed().as_secs() < duration_seconds {

//         let now = Instant::now();
//         if now < next_send_time {
//             sleep(next_send_time.duration_since(now)).await;
//         }
//         next_send_time = next_send_time.checked_add(message_interval).unwrap_or_else(Instant::now);

//         for i in 0..10_000 {
//             let base_message = format!("Message {}", i);
//                 // Build a temporary JSON object with an empty "data" field to calculate overhead.
//                 let base_obj = json!({
//                     "id": i,
//                     "message": base_message,
//                     "data": ""
//                 });
//                 let overhead = base_obj.to_string().len();
//                 if overhead > TARGET_JSON_SIZE {
//                     panic!("TARGET_JSON_SIZE ({}) is too small; overhead is {} bytes", TARGET_JSON_SIZE, overhead);
//                 }
        
//                 // Calculate filler length so that total JSON size equals TARGET_JSON_SIZE.
//                 let filler_len = TARGET_JSON_SIZE - overhead;
//                 let filler = "A".repeat(filler_len);
        
//                 // Construct the final JSON payload.
//                 let json_payload = json!({
//                     "id": i,
//                     "message": base_message,
//                     "data": filler
//                 });

//                 // let payload_size = sizeof_val(&json_payload);
//                 // println!("Payload size: {} bytes", payload_size);
//                 let payload = json_payload.to_string();
//                 // println!("Payload ({} bytes): {}", payload.len(), payload);
            
//             // Send the message using rdkafka.
//             producer.send(
//                 rdkafka::producer::base_producer::BaseRecord::to("demo-new-10")
//                     .payload(&payload)
//                     .key(&format!(""))
//                     .partition(partition),
//             ).expect("Failed to enqueue");        
//             }
//             let duration = start.elapsed();
//             println!("Time taken: {:?}", duration);
        
//             let timeout = Duration::from_secs(100);
//             let start = Instant::now();
//             let result = producer.flush(timeout);
//             let duration = start.elapsed();
//             println!("Flush result: {:?}", result);
//             println!("Flush time taken: {:?}", duration);
//     }
// }
use rdkafka::config::ClientConfig;
use rdkafka::producer::{Producer, ThreadedProducer};
use std::time::{Duration, Instant};
use tokio::task;
use serde_json::json;
use std::sync::Arc;
use std::mem::size_of;
use serde_json::Value;

#[tokio::main]
async fn main() {
    let producer: ThreadedProducer<_> = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("queue.buffering.max.ms", "10")
    // .set("socket.send.buffer.bytes", "1048576") // 1 MB
    // .set("socket.receive.buffer.bytes", "1048576") // 1 MB
    .set("queue.buffering.max.messages", "1000000")
    .set("max.in.flight.requests.per.connection", "100")
    .create()
    .expect("Producer creation error");

    let arc_producer = Arc::new(producer);
    let num_producers = 10;
    let mut producers = Vec::new();
    let duration_seconds = 10;
    let start_time = Instant::now();    
    let messages_per_second = 1_000_000;

    println!("Starting Kafka producer test with {} workers for {} seconds...", 
             num_producers, duration_seconds);

    for i in 0..num_producers {
        producers.push(task::spawn(send_messages(
            i, arc_producer.clone(), start_time, duration_seconds, messages_per_second
        )));
    }

    let mut total_messages = 0;
    let mut successful_workers = 0;
    let mut worker_stats = Vec::new();

    for (i, producer) in producers.into_iter().enumerate() {
        match producer.await {
            Ok(count) => {
                total_messages += count;
                successful_workers += 1;
                let throughput = count as f64 / duration_seconds as f64;
                worker_stats.push((i, count, throughput));
                println!("Producer {} sent {} messages ({:?} msgs/sec)", 
                         i, count, throughput);
            },
            Err(e) => println!("Producer {} error: {:?}", i, e),
        }
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    let global_throughput = total_messages as f64 / elapsed;
    
    println!("\n--- Performance Summary ---");
    println!("Test duration: {:?} seconds", elapsed);
    println!("Total messages sent: {}", total_messages);
    println!("Global throughput: {:?} msgs/sec", global_throughput);
    println!("Successful workers: {}/{}", successful_workers, num_producers);
    println!("Average throughput per worker: {:?} msgs/sec", 
             if successful_workers > 0 { global_throughput / successful_workers as f64 } else { 0.0 });
}
   
use tokio::time::sleep;

async fn send_messages(
    partition: i32, 
    producer: Arc<ThreadedProducer<rdkafka::producer::DefaultProducerContext>>,
    start_time: Instant,
    duration_seconds: u64,
    messages_per_second: u64
) -> u64 {
    
    const TARGET_JSON_SIZE: usize = 1024; // Change this value to 512, 2048, etc.

    let start = Instant::now();
    let mut message_count: u64 = 0;

    let mut next_send_time = Instant::now();
    // Calculate the interval between messages to achieve the target rate
    let message_interval = if messages_per_second > 0 {
        Duration::from_micros(1_000_000 / messages_per_second)
    } else {
        Duration::from_micros(0)
    };
    
    while start_time.elapsed().as_secs() < duration_seconds {
        let now = Instant::now();
        if now < next_send_time {
            sleep(next_send_time.duration_since(now)).await;
        }
        next_send_time = next_send_time.checked_add(message_interval).unwrap_or_else(Instant::now);

        let batch_size = 100_000;
        let mut batch_sent = 0;

        for i in 0..batch_size {
            let base_message: String = format!("Message {}", i);
            // Build a JSON payload of TARGET_JSON_SIZE bytes
            let base_obj = json!({
                "id": i,
                "message": base_message,
                "data": ""
            });
            let overhead = base_obj.to_string().len();
            
            // Calculate filler length so that total JSON size equals TARGET_JSON_SIZE
            let filler_len = TARGET_JSON_SIZE - overhead;
            let filler = "A".repeat(filler_len);
        
            // Construct the final JSON payload
            let json_payload = json!({
                "id": i,
                "message": base_message,
                "data": filler
            });
            
            let payload = json_payload.to_string();
            
            // Send the message using rdkafka
            match producer.send(
                rdkafka::producer::base_producer::BaseRecord::to("demo")
                    .payload(&payload)
                    .key(&format!(""))
                    .partition(partition),
            ) {
                Ok(_) => {
                    batch_sent += 1;
                },
                Err(e) => {
                    println!("Send error (worker {}): {:?}", partition, e);
                    return message_count; // Return early if we encounter errors
                }
            }
        }
        
        message_count += batch_sent as u64;
        
        let duration = start.elapsed();
        println!("Worker {}: Batch time: {:?}, Messages: {}", 
                 partition, duration, batch_sent);
        
        let timeout = Duration::from_secs(1);
        let flush_start = Instant::now();
        let result = producer.flush(timeout);
        let flush_duration = flush_start.elapsed();
        
        println!("Worker {}: Flush result: {:?}, Flush time: {:?}", 
                 partition, result, flush_duration);
        
        // Calculate current throughput for this worker
        let elapsed_sec = start_time.elapsed().as_secs_f64();
        if elapsed_sec > 0.0 {
            let current_throughput = message_count as f64 / elapsed_sec;
            println!("Worker {}: Current throughput: {:?} msgs/sec", 
                     partition, current_throughput);
        }
    }
    
    message_count
}