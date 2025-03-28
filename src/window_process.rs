use arrow::error::ArrowError;
use crate::window::DataWindow;
use tokio::sync::mpsc;
use crate::partition_spliter::split_batches_by_partition;
// the goal of this window process is to take the ownership of the data window from the window manager
// and process it with the given function
// the window process will now push this data to the next stage of the pipeline weather it is nats or kafka or using arrow flight 
use crate::iceberg_writer::{init, commit_data_files, write_record_batch};
use iceberg::{
    spec::{write_data_files_to_avro, DataFile, DataFileFormat, TableMetadataRef},
    transaction::Transaction,
    table::Table,
    Catalog, TableCreation,
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            ParquetWriterBuilder,
        },
        IcebergWriter, IcebergWriterBuilder,
    },
    io::FileIOBuilder,
};
use iceberg_catalog_rest::RestCatalog;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;


pub async fn process_window(mut rx: mpsc::Receiver<DataWindow>) -> Result<(), ArrowError> {
    // process the window
    while let Some(window) = rx.recv().await {
        // process the window
        let batches = window.batches;
        let partition_batches = split_batches_by_partition(batches, "partition")?;
        for (key, batch) in partition_batches {
            // println!("\nPartition key: {}", key);
            // use arrow::util::pretty::pretty_format_batches;
            // match pretty_format_batches(&[batch]) {
            //     Ok(formatted) => println!("{}", formatted),
            //     Err(e) => eprintln!("Error formatting batch: {}", e),
            // }
        
        let (catalog, table) = init().await;
        let data_files = write_record_batch(&table, batch, key.to_string()).await;
        let (attempt, success) = commit_data_files(&table, data_files, &catalog, 0, 0).await;
        if success {
            println!("Commit successful");
        } else {
            println!("Commit failed");
        }
    }
    }
    Ok(())
}


pub async fn process_window_arrow(batches: Vec<RecordBatch>) -> Result<(), ArrowError> {
    // process the window
        // process the window
        println!("batches: {:?}", batches.len());
        let partition_batches = split_batches_by_partition(batches, "count")?;
        for (key, batch) in partition_batches {
            // println!("\nPartition key: {}", key);
            // use arrow::util::pretty::pretty_format_batches;
            // match pretty_format_batches(&[batch]) {
            //     Ok(formatted) => println!("{}", formatted),
            //     Err(e) => eprintln!("Error formatting batch: {}", e),
            // }
        
        let (catalog, table) = init().await;
        let data_files = write_record_batch(&table, batch, key.to_string()).await;
        let (attempt, success) = commit_data_files(&table, data_files, &catalog, 0, 0).await;
        if success {
            println!("Commit successful");
        } else {
            println!("Commit failed");
        }
    
    }
    Ok(())
}


