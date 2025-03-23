
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
use iceberg::{
    io::{S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,S3_DISABLE_CONFIG_LOAD},
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use iceberg_catalog_rest::RestCatalogConfig;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use arrow::array::{record_batch, RecordBatch};
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalog;
pub type DataFilesAvro = Vec<u8>;

pub async fn init() -> (RestCatalog, Table) {

    let config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_owned())
        .props(HashMap::from([
            (S3_ENDPOINT.to_owned(), "http://localhost:9000".to_owned()),
            (S3_REGION.to_owned(), "us-east-1".to_owned()),
            (S3_ACCESS_KEY_ID.to_owned(), "admin".to_owned()),
            (S3_SECRET_ACCESS_KEY.to_owned(), "password".to_owned()),
            (S3_ALLOW_ANONYMOUS.to_owned(), "true".to_owned()),
            (S3_DISABLE_CONFIG_LOAD.to_owned(), "true".to_owned()),
            // ("s3.bucket".to_owned(), "icebergdata".to_owned()),
        ]))
        .build();

    let catalog = RestCatalog::new(config);
    let ns = NamespaceIdent::new("test-ns".to_owned());
    let tb = TableIdent::new(ns, "test-table-4".to_owned());
    let table = create_table(&catalog, &tb).await;

    return (catalog, table);
}

async fn create_table(catalog: &impl Catalog, tb: &TableIdent) -> Table {
    // doess this purge table as well?

    let table_exists = catalog
        .table_exists(&tb)
        .await
        .expect("Failed to check existence of table");

    if table_exists {
        println!("Table exists, dropping table");
        catalog.drop_table(&tb).await.expect("Failed to drop table");
    } else {
        println!("Table does not exist");
    }
    println!("Creating table");

    // create table
    let fields: Vec<NestedField> = vec![
        NestedField::new(0, "name", Type::Primitive(PrimitiveType::String), false),
        NestedField::new(1, "size", Type::Primitive(PrimitiveType::String), false),
        NestedField::new(2, "count", Type::Primitive(PrimitiveType::Int), false),
    ];

    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(Arc::from))
        .build()
        .expect("Failed to build schema");

    let table_schema = TableCreation::builder()
        .name(tb.name().to_owned())
        .schema(schema)
        .build();

    catalog
        .create_table(&tb.namespace(), table_schema)
        .await
        .expect("Failed to create table")
}

pub fn next_instant(duration: usize) -> Instant {
    Instant::now() + Duration::from_secs(duration as u64)
}


pub async fn commit_data_files(table: &Table, data_files: Vec<DataFile>, catalog: &RestCatalog,committer_id: usize,iteration: usize) -> (usize,bool) {

    const MAX_RETRIES: usize = 10;
    
    // Start with a small backoff time (milliseconds)
    let mut backoff_ms = 100;
    
    for attempt in 0..MAX_RETRIES {
        // Refresh the table state from the catalog before each attempt
        let refreshed_table = catalog.load_table(table.identifier())
        .await
        .expect("Failed to refresh table");
    
        let mut action = Transaction::new(&refreshed_table)
            .fast_append(None, vec![])
            .expect("Failed to create transaction");
        
            action
            .add_data_files(data_files.clone())    
        .expect("Failed to add data files") ;
    
    let commit_result = action
        .apply()
        .await
        .expect("Failed to apply action")
        .commit(catalog)
        .await;
        // .expect("Failed to commit insert");

        if let Err(e) = commit_result {
            if attempt < MAX_RETRIES - 1 {
                // println!("Commit failed on attempt {}:. Retrying after {}ms...", 
                //     attempt + 1, backoff_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                continue;
            } else {
                println!("commit {} Failed to commit number {} after {} attempts ", committer_id, iteration,MAX_RETRIES);
                return (attempt,false);
            }
        }else{
            return (attempt,true);
        }

    }

    (MAX_RETRIES,false)
}



async fn write_record_batch(table: &Table, rb: RecordBatch, partition: i32) -> Vec<DataFile> {

    let schema = table.metadata().current_schema().to_owned();
    
    // Set up file writer components
    let file_io = table.file_io().clone();
    let loc_gen = DefaultLocationGenerator::new(table.metadata().to_owned())
        .expect("Failed to create location generator");
    //random number   
    let i = rand::random_range(0..1000);
    let file_name_gen =    DefaultFileNameGenerator::new(format!("{i}"), None, DataFileFormat::Parquet);

    // Create writer
    let file_writer_builder = ParquetWriterBuilder::new(
        Default::default(),
        schema,
        file_io,
        loc_gen,
        file_name_gen
    );
    
    let mut data_file_writer = DataFileWriterBuilder::new(file_writer_builder, None, 0)
        .build()
        .await
        .expect("Failed to build data file writer");
    
    // Write the record batch
    data_file_writer
        .write(rb)
        .await
        .expect("Failed to write to iceberg");
    
    // Close the writer and return data files
    data_file_writer
        .close()
        .await
        .expect("Failed to close the writer")
}
