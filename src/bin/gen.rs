// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{FutureProducer, FutureRecord};
// use futures::future::join_all;
// use std::time::Duration;

// #[tokio::main]
// async fn main() {
//     // Create a FutureProducer instance configured to connect to your Kafka brokers.
//     let producer: FutureProducer = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9092")
//         .create()
//         .expect("Producer creation error");

//     let topic = "demo-prod-test";

//     // Define a batch of messages as tuples (key, payload).
//     let messages = vec![
//         ("key1", "Hello, Kafka!"),
//         ("key2", "Batch message 2"),
//         ("key3", "Batch message 3"),
//     ];

//     // Create a collection of futures for sending messages.
//     let send_futures = messages.into_iter().map(|(key, payload)| {
//         let record = FutureRecord::to(topic)
//             .payload(payload)
//             .key(key);
//         // The second parameter is the timeout for delivery confirmation.
//         producer.send(record, Duration::from_secs(1))
//     });

//     // Wait for all messages to be delivered.
//     let results = join_all(send_futures).await;

//     // Iterate over results and handle delivery outcomes.
//     for result in results {
//         match result {
//             Ok((partition, offset)) => {
//                 println!("Message delivered to partition {} at offset {}", partition, offset);
//             }
//             Err((e, _msg)) => {
//                 println!("Failed to deliver message: {:?}", e);
//             }
//         }
//     }
// }
use rdkafka::config::ClientConfig;
use rdkafka::producer::{Producer, ThreadedProducer};
use std::time::{Duration, Instant};
use tokio::task;
use std::sync::Arc;
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

    let arc_producer: Arc<ThreadedProducer<rdkafka::producer::DefaultProducerContext>> = Arc::new(producer);
    let num_producers = 10;
    let mut producers = Vec::new();
    for i in 0..num_producers {
        producers.push(task::spawn(send_messages(i, arc_producer.clone())));
    }
    for producer in producers {
        producer.await.unwrap();
    }
}

async fn send_messages(partition: i32, producer: Arc<ThreadedProducer<rdkafka::producer::DefaultProducerContext>>) {
    
    // producer.poll(Duration::from_millis(2000));
    //         
    // let base_message = format!("Message {}", 1);
    // // Calculate how many filler characters are needed.
    // let filler_length = if base_message.len() < 1024 {
    //     1024 - base_message.len()
    // } else {
    //     0
    // };
    // // Create a filler string of 'A's.
    // let filler = "A".repeat(filler_length);
    // // Combine base message and filler to form a 1KB payload.
    // let payload = format!("{}{}", base_message, filler);

    // // Calculate payload size in bytes and kilobytes.
    // let payload_size_bytes = payload.len();
    // let payload_size_kb = payload_size_bytes as f64 / 1024.0;
    // println!(
    //     "Payload size: {} bytes ({} KB)",
    //     payload_size_bytes, payload_size_kb
    // );


    let start = Instant::now();
    let flush_interval = 500_000;

    for i in 0..100_000 {
        // Create a base message that includes the index.
        let base_message = format!("Message {}", i);
        // Calculate how many filler characters are needed.
        let filler_length = if base_message.len() < 1024 {
            1024 - base_message.len()
        } else {
            0
        };
        // Create a filler string of 'A's.
        let filler = "A".repeat(filler_length);
        // Combine base message and filler to form a 1KB payload.
        let payload = format!("{}{}", base_message, filler);

        // Send the message using rdkafka.
        producer.send(
            rdkafka::producer::base_producer::BaseRecord::to("demo-new-10")
                .payload(&payload)
                .key(&format!("Key {}", i))
                .partition(partition),
        ).expect("Failed to enqueue");        
        // // Periodically flush to prevent too many messages from piling up.
        // if i % flush_interval == 0 {
        //     let result = producer.flush(Duration::from_millis(1000));
        //     println!("Flush result: {:?}", result);
        // }
    }

    let duration = start.elapsed();
    println!("Time taken: {:?}", duration);

    let timeout = Duration::from_secs(100);
    let start = Instant::now();
    let result = producer.flush(timeout);
    let duration = start.elapsed();
    println!("Flush result: {:?}", result);
    println!("Flush time taken: {:?}", duration);
}
