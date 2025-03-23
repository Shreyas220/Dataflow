mod window;
mod window_process;
mod partition_spliter;
mod kafka_ingestion;
mod json_arrow;


use futures::stream::StreamExt; 
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::config::ClientConfig;
use serde_json::Value;
use tokio::task;
use arrow::array::{StringArray, Int64Array, Float64Array, BooleanArray, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{record_batch, RecordBatch};
use std::collections::HashMap;
use std::time::Instant;
use std::sync::Arc;


#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),       
        Field::new("count", DataType::Int64, true),       
        ]));

    let overall_start = Instant::now();


    let init_start = Instant::now();
    println!("Hello, world!");
    // let mut errors = 0;
    println!("Initialization took: {:.3?}", init_start.elapsed());

    let consumer_start = Instant::now();
    let consumer: StreamConsumer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("group.id", format!("my-consumer-group-{}", 0))
    .set("auto.offset.reset", "earliest")
    .create()
    .expect("Consumer creation error");


    print!("Subscribing to topic");
    consumer.subscribe(&["demo"]).expect("Can't subscribe to specified topics");

    println!("Consumer creation took: {:.3?}", consumer_start.elapsed());
    
    let mut stream = consumer.stream();
    let mut json_values = Vec::new();
    let mut message_count = 0;
    let mut errors = 0;
    let target_messages = 100000;
    
    let consumer_start = Instant::now();
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(message) => {
                if let Some(Ok(payload)) = message.payload_view::<str>() {
                    match serde_json::from_str::<Value>(payload) {
                        Ok(value) => {
                            json_values.push(value);
                            message_count += 1;
                        },
                        Err(e) => {
                            eprintln!("Failed to parse JSON: {}", e);
                            errors += 1;
                        }
                    }
                } else {
                    eprintln!("Failed to parse message payload as string");
                    errors += 1;
                }
            },
            Err(e) => {
                eprintln!("Message error: {}", e);
                errors += 1;
            }
        }
        
        // Print progress every 1000 messages
        if message_count % 1000 == 0 && message_count > 0 {
            println!("Processed {} messages so far", message_count);
        }
        
        // Stop after receiving 10,000 messages
        if message_count >= target_messages {
            println!("Reached target of {} messages", target_messages);
            break;
        }
    }


    println!("Processed {} messages took {:?}\n", message_count, consumer_start.elapsed());

    let arrow_start = Instant::now();

    let rb = json_to_arrow(&json_values, schema).await.expect("Failed to convert JSON to Arrow");

    println!("Arrow conversion took: {:.3?}", arrow_start.elapsed());
    let partition_start = Instant::now();
    let vec_rb = vec![rb];
    let partitionedz̄  = partition_spliter::split_batches_by_partition(vec_rb, "count").expect("Failed to partition");
    println!("Partitioning took: {:.3?}", partition_start.elapsed());
    println!("Partitioned length: {}", partitionedz̄.len());


}


async fn json_to_arrow(json_messages: &[Value], schema: Arc<Schema>) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Create dynamic arrays based on schema fields
let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
let mut column_data: HashMap<String, Vec<Option<Value>>> = HashMap::new();

// Initialize column data vectors
for field in schema.fields() {
    column_data.insert(field.name().clone(), Vec::with_capacity(json_messages.len()));
}

// Extract field values from JSON messages
for json in json_messages {
    for field in schema.fields() {
        let field_name = field.name();
        let values = column_data.get_mut(field_name).unwrap();
        
        if let Some(value) = json.get(field_name) {
            values.push(Some(value.clone()));
        } else if !field.is_nullable() {
            return Err(format!("Missing required field: {}", field_name).into());
        } else {
            values.push(None);
        }
    }
}

// Convert JSON values to Arrow arrays
for field in schema.fields() {
    
    let field_name = field.name();
    let values = column_data.get(field_name).unwrap();
    
    // Create appropriate Arrow array based on field type
    match field.data_type() {
        DataType::Int64 => {
            let array_data: Vec<Option<i64>> = values.iter()
                .map(|opt_value| {
                    opt_value.as_ref().and_then(|v| v.as_i64())
                })
                .collect();
            arrays.push(Arc::new(Int64Array::from(array_data)));
        },
        DataType::Int32 => {
            let array_data: Vec<Option<i32>> = values.iter()
                .map(|opt_value| opt_value.as_ref().and_then(|v| v.as_i64()).map(|n| n as i32))
                .collect();
            arrays.push(Arc::new(arrow::array::Int32Array::from(array_data)));
        },    
        DataType::Utf8 => {
            let array_data: Vec<Option<String>> = values.iter()
                .map(|opt_value| {
                    opt_value.as_ref().and_then(|v| v.as_str().map(|s| s.to_string()))
                })
                .collect();
            arrays.push(Arc::new(StringArray::from(array_data)));
        },
        DataType::Float64 => {
            let array_data: Vec<Option<f64>> = values.iter()
                .map(|opt_value| {
                    opt_value.as_ref().and_then(|v| v.as_f64())
                })
                .collect();
            arrays.push(Arc::new(Float64Array::from(array_data)));
        },
        DataType::Boolean => {
            let array_data: Vec<Option<bool>> = values.iter()
                .map(|opt_value| {
                    opt_value.as_ref().and_then(|v| v.as_bool())
                })
                .collect();
            arrays.push(Arc::new(BooleanArray::from(array_data)));
        },
        dt => return Err(format!("Unsupported data type in conversion: {:?}", dt).into()),
    }
}

// Create a RecordBatch
let batch = RecordBatch::try_new(schema, arrays)?;

Ok(batch)
}
