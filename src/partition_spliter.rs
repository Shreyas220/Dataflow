use std::collections::HashMap;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, BooleanArray};
use arrow::compute::concat;
use arrow_array::Array;
use arrow_select::filter::filter_record_batch;


fn append_batches(existing: &RecordBatch, new_batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    if existing.schema() != new_batch.schema() {
        return Err(ArrowError::ComputeError("Schemas do not match".to_string()));
    }
    let schema = existing.schema();
    let mut columns = Vec::with_capacity(existing.num_columns());
    for i in 0..existing.num_columns() {
        let col1 = existing.column(i);
        let col2 = new_batch.column(i);
        let concatenated = concat(&[col1.as_ref(), col2.as_ref()])
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
        columns.push(concatenated);
    }
    RecordBatch::try_new(schema.clone(), columns)
}

pub fn split_batches_by_partition(
    batches: Vec<RecordBatch>,
    partition_column: &str,
) -> Result<HashMap<i64, RecordBatch>, ArrowError> {
    let mut partitioned_batches: HashMap<i64, RecordBatch> = HashMap::new();

    // Iterate over each record batch in the vector.
    for batch in batches {
        println!("batch: {:?}", batch);
        // Retrieve the partition column.
        let col = batch.column_by_name(partition_column)
            .ok_or_else(|| ArrowError::ComputeError(format!("Column {} not found", partition_column)))?;
        
        // Downcast the column to an Int64Array.
        let int_array = col.as_ref().as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| ArrowError::ComputeError("Failed to downcast partition column to Int64Array".to_string()))?;
        
        // Build a map from batch_id to vector of row indices for this batch.
        let mut indices_map: HashMap<i64, Vec<usize>> = HashMap::new();
        for i in 0..int_array.len() {
            if int_array.is_valid(i) {
                let key = int_array.value(i);
                indices_map.entry(key).or_default().push(i);
            }
        }
        
        // For each unique batch_id, create a boolean mask and filter the batch.
        for (key, indices) in indices_map {
            let mut mask = vec![false; batch.num_rows()];
            for &i in &indices {
                mask[i] = true;
            }
            let predicate = BooleanArray::from(mask);
            
            // Use Arrow's filter function to obtain the partitioned batch.
            let filtered_batch = filter_record_batch(&batch, &predicate)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
            
            // If data already exists for this partition key, append the new filtered batch.
            partitioned_batches
                .entry(key)
                .and_modify(|existing_batch| {
                    *existing_batch = append_batches(existing_batch, &filtered_batch)
                        .expect("Failed to append batches");
                })
                .or_insert(filtered_batch);
        }
    }
    
    Ok(partitioned_batches)
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::error::ArrowError;
    use crate::partition_spliter::split_batches_by_partition;

    #[test]
    fn test_split_batches_by_partition() -> Result<(), ArrowError> {
        // Define the schema with three columns.
        let schema = Arc::new(Schema::new(vec![
            Field::new("batch_id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create Batch1 with 4 rows.
        let batch1_batch_id = Int64Array::from(vec![1, 2, 1, 3]);
        let batch1_value = Int64Array::from(vec![10, 20, 30, 40]);
        let batch1_name = StringArray::from(vec!["a", "b", "c", "d"]);
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(batch1_batch_id),
                Arc::new(batch1_value),
                Arc::new(batch1_name),
            ],
        )?;

        // Create Batch2 with 3 rows.
        let batch2_batch_id = Int64Array::from(vec![2, 3, 1]);
        let batch2_value = Int64Array::from(vec![50, 60, 70]);
        let batch2_name = StringArray::from(vec!["e", "f", "g"]);
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(batch2_batch_id),
                Arc::new(batch2_value),
                Arc::new(batch2_name),
            ],
        )?;

        // Create Batch3 with 5 rows.
        let batch3_batch_id = Int64Array::from(vec![1, 2, 3, 3, 2]);
        let batch3_value = Int64Array::from(vec![80, 90, 100, 110, 120]);
        let batch3_name = StringArray::from(vec!["h", "i", "j", "k", "l"]);
        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(batch3_batch_id),
                Arc::new(batch3_value),
                Arc::new(batch3_name),
            ],
        )?;

        // Combine all batches into a vector.
        let batches = vec![batch1, batch2, batch3];

        // Call our partition splitter function using "batch_id" as the partition column.
        let partitioned = split_batches_by_partition(batches, "batch_id")?;
        
        // Print each partition with formatted output
        println!("\n=== Partitioned RecordBatches Results ===");
        for (key, batch) in partitioned.clone() {
            println!("\nPartition key: {}", key);
            use arrow::util::pretty::pretty_format_batches;
            match pretty_format_batches(&[batch]) {
                Ok(formatted) => println!("{}", formatted),
                Err(e) => eprintln!("Error formatting batch: {}", e),
            }
        }

        // We expect 3 partitions (keys 1, 2, and 3).
        assert_eq!(partitioned.len(), 3);

        // Verify partition 1.
        // Expected rows for batch_id==1:
        //   - Batch1: rows 0 and 2 → (1,10,"a") and (1,30,"c")
        //   - Batch2: row 2 → (1,70,"g")
        //   - Batch3: row 0 → (1,80,"h")
        let partition1 = partitioned.get(&1).expect("Missing partition for key 1");
        assert_eq!(partition1.num_rows(), 4);
        let partition1_batch_ids = partition1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..partition1_batch_ids.len() {
            assert_eq!(partition1_batch_ids.value(i), 1);
        }

        // Verify partition 2.
        // Expected rows for batch_id==2:
        //   - Batch1: row 1 → (2,20,"b")
        //   - Batch2: row 0 → (2,50,"e")
        //   - Batch3: rows 1 and 4 → (2,90,"i") and (2,120,"l")
        let partition2 = partitioned.get(&2).expect("Missing partition for key 2");
        assert_eq!(partition2.num_rows(), 4);
        let partition2_batch_ids = partition2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..partition2_batch_ids.len() {
            assert_eq!(partition2_batch_ids.value(i), 2);
        }

        // Verify partition 3.
        // Expected rows for batch_id==3:
        //   - Batch1: row 3 → (3,40,"d")
        //   - Batch2: row 1 → (3,60,"f")
        //   - Batch3: rows 2 and 3 → (3,100,"j") and (3,110,"k")
        let partition3 = partitioned.get(&3).expect("Missing partition for key 3");
        assert_eq!(partition3.num_rows(), 4);
        let partition3_batch_ids = partition3
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..partition3_batch_ids.len() {
            assert_eq!(partition3_batch_ids.value(i), 3);
        }

        // for (key, batch) in partitioned {
        //     println!("key: {:?}, batch: {:?}", key, batch);
        //         let print_record_batch = batch.clone();
        //         use arrow::util::pretty::pretty_format_batches;
        //         match pretty_format_batches(&[print_record_batch]) {
        //             Ok(formatted) => println!("{}", formatted),
        //             Err(e) => eprintln!("Formatting error: {}", e),
        //         }
        // }
        Ok(())
    }
}