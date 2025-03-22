use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::time;

/// Represents a window of data with Arrow batches
pub struct DataWindow {
    /// Unique identifier for this window
    pub id: String,
    
    /// Creation timestamp of this window
    pub created_at: Instant,
    
    /// Raw batches in this window
    pub batches: Vec<RecordBatch>,
    
}

impl DataWindow {
    /// Create a new data window
    pub fn new(id: String) -> Self {
        DataWindow {
            id,
            created_at: Instant::now(),
            batches: Vec::new(),
        }
    }
    
    /// Add a batch to this window
    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<(), ArrowError> {
        // Simply store the raw batch
        // add to record batch 



        self.batches.push(batch);
        Ok(())
    }
    
    /// Check if this window has exceeded its time limit
    pub fn is_expired(&self, window_duration: Duration) -> bool {
        self.created_at.elapsed() >= window_duration
    }
    
    /// Get the number of rows in all batches
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
    
    /// Get the number of batches
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }
}

/// Configuration for the window manager
pub struct WindowManagerConfig {
    /// Duration of each window in seconds
    pub window_duration_secs: u64,
    
    /// Size of the processing channel buffer
    pub channel_buffer_size: usize,
    
    /// Size of the overflow buffer (maximum windows in backpressure)
    pub max_overflow_size: usize,
    
    /// Ticker interval in milliseconds
    pub ticker_interval_ms: u64,
}

impl Default for WindowManagerConfig {
    fn default() -> Self {
        WindowManagerConfig {
            window_duration_secs: 15,
            channel_buffer_size: 100,
            max_overflow_size: 1000,
            ticker_interval_ms: 100, // Check every 100ms
        }
    }
}

/// Window manager with timer-based rotation
pub struct WindowManager {
    /// Current active window
    current_window: Arc<Mutex<DataWindow>>,
    
    /// Duration of each window
    window_duration: Duration,
    
    /// Sender for the main processing channel
    processing_tx: mpsc::Sender<DataWindow>,
    
    /// Receiver for the main processing channel
    processing_rx: Option<mpsc::Receiver<DataWindow>>,
    
    /// Overflow buffer for backpressure handling
    overflow_buffer: Vec<DataWindow>,
    
    /// Maximum overflow buffer size
    max_overflow_size: usize,
    
    /// Ticker interval
    ticker_interval: Duration,
    
    /// Flag to indicate if the window manager is running
    is_running: Arc<Mutex<bool>>,
}

impl WindowManager {
    /// Create a new window manager
    pub fn new(config: WindowManagerConfig) -> Self {
        // Create the processing channel
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);
        
        WindowManager {
            current_window: Arc::new(Mutex::new(DataWindow::new("window-1".to_string()))),
            window_duration: Duration::from_secs(config.window_duration_secs),
            processing_tx: tx,
            processing_rx: Some(rx),
            overflow_buffer: Vec::with_capacity(config.max_overflow_size),
            max_overflow_size: config.max_overflow_size,
            ticker_interval: Duration::from_millis(config.ticker_interval_ms),
            is_running: Arc::new(Mutex::new(false)),
        }
    }
    
    /// Add a batch to the current window
    pub fn add_batch(&self, batch: RecordBatch) -> Result<(), ArrowError> {
        let mut window = self.current_window.lock().unwrap();
        window.add_batch(batch)
    }
    
    /// Start the window rotation ticker
    pub fn start_rotation_ticker(&mut self) -> mpsc::Receiver<DataWindow> {
        // Take the receiver out
        let rx = self.processing_rx.take().unwrap_or_else(|| {
            // If already taken, create a new channel
            let (tx, rx) = mpsc::channel(100);
            self.processing_tx = tx;
            rx
        });
        
        // Mark as running
        let mut is_running = self.is_running.lock().unwrap();
        *is_running = true;
        drop(is_running);
        
        // Clone what we need for the ticker task
        let current_window = self.current_window.clone();
        let window_duration = self.window_duration;
        let ticker_interval = self.ticker_interval;
        let processing_tx = self.processing_tx.clone();
        let is_running = self.is_running.clone();
        let max_overflow_size = self.max_overflow_size;
        
        // Start the ticker task
        tokio::spawn(async move {
            let mut overflow_buffer: Vec<DataWindow> = Vec::with_capacity(max_overflow_size);
            let mut interval = time::interval(ticker_interval);
            let mut window_counter = 2; // Start from 2 since window-1 is already created
            
            info!("Starting window rotation ticker with interval {:?}", ticker_interval);
            
            loop {
                interval.tick().await;
                
                // Check if we should exit
                if !*is_running.lock().unwrap() {
                    info!("Window rotation ticker stopping");
                    break;
                }
                
                // Check if we need to rotate the window
                let should_rotate = {
                    let window = current_window.lock().unwrap();
                    window.is_expired(window_duration)
                };
                
                if should_rotate {
                    // Rotate the window and send it for processing
                    let rotated_window = {
                        let mut current = current_window.lock().unwrap();
                        
                        // Create a new window
                        let new_id = format!("window-{}", window_counter);
                        window_counter += 1;
                        let new_window = DataWindow::new(new_id);
                        
                        // Swap the current window with the new one
                        std::mem::replace(&mut *current, new_window)
                    };
                    
                    debug!(
                        "Rotated window: {}, contains {} batches with {} rows",
                        rotated_window.id,
                        rotated_window.num_batches(),
                        rotated_window.num_rows()
                    );
                    
                    // Try to send the window for processing
                    match processing_tx.try_send(rotated_window) {
                        Ok(_) => {
                            debug!("Sent window for processing");
                            
                            // Try to drain overflow buffer if we have space
                            while let Some(window) = overflow_buffer.pop() {
                                match processing_tx.try_send(window) {
                                    Ok(_) => {
                                        debug!("Sent buffered window for processing");
                                    },
                                    Err(mpsc::error::TrySendError::Full(window)) => {
                                        // Channel is still full, put it back and stop
                                        overflow_buffer.push(window);
                                        break;
                                    },
                                    Err(mpsc::error::TrySendError::Closed(window)) => {
                                        // Channel closed, can't do anything
                                        error!("Processing channel closed - dropping buffered window");
                                        break;
                                    }
                                }
                            }
                        },
                        Err(mpsc::error::TrySendError::Full(window)) => {
                            // Backpressure detected - buffer the window
                            warn!(
                                "Processing backpressure detected - buffering window {} (buffer size: {})",
                                window.id, overflow_buffer.len()
                            );
                            
                            if overflow_buffer.len() < max_overflow_size {
                                overflow_buffer.push(window);
                            } else {
                                error!(
                                    "Overflow buffer full ({} windows) - dropping window {}",
                                    overflow_buffer.len(), window.id
                                );
                                // In a real system, you might want to persist this window or alert
                            }
                        },
                        Err(mpsc::error::TrySendError::Closed(window)) => {
                            // Channel is closed - can't send windows anymore
                            error!("Processing channel closed - stopping rotation ticker");
                            break;
                        }
                    }
                }
            }
            
            info!("Window rotation ticker stopped");
        });
        
        rx
    }
    
    /// Stop the window rotation ticker
    pub fn stop(&self) {
        let mut is_running = self.is_running.lock().unwrap();
        *is_running = false;
    }
    
    /// Get the window duration
    pub fn get_window_duration(&self) -> Duration {
        self.window_duration
    }
    
    /// Get the current window information (for monitoring)
    pub fn get_current_window_info(&self) -> (String, usize, usize) {
        let window = self.current_window.lock().unwrap();
        (
            window.id.clone(),
            window.num_batches(),
            window.num_rows()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tokio::runtime::Runtime;
    
    // Helper to create a test batch
    fn create_test_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
        ]));
        
        let value_array = Int64Array::from(
            values.iter().map(|v| Some(*v)).collect::<Vec<_>>()
        );
        
        RecordBatch::try_new(
            schema,
            vec![Arc::new(value_array)],
        ).unwrap()
    }
    
    #[test]
    fn test_window_rotation() {
        // Create a window manager with a short window duration for testing
        let rt = Runtime::new().unwrap();
        
        rt.block_on(async {

            let config = WindowManagerConfig {
                window_duration_secs: 1, // 1 second windows for testing
                channel_buffer_size: 10,
                max_overflow_size: 100,
                ticker_interval_ms: 100,
            };
            
            let mut window_manager = WindowManager::new(config);
            let mut window_rx = window_manager.start_rotation_ticker();
            
            // Add some test batches
            let batch1 = create_test_batch(&[1, 2, 3]);
            window_manager.add_batch(batch1).unwrap();
            
            let batch2 = create_test_batch(&[4, 5, 6]);
            window_manager.add_batch(batch2).unwrap();
            
            // Wait for the window to rotate
            tokio::time::sleep(Duration::from_secs(2)).await;
            
            // Should receive a rotated window
            match tokio::time::timeout(Duration::from_secs(1), window_rx.recv()).await {
                Ok(Some(window)) => {
                    assert_eq!(window.num_batches(), 2);
                    assert_eq!(window.num_rows(), 6); // 3 from batch1, 3 from batch2
                },
                Ok(None) => panic!("Window channel closed unexpectedly"),
                Err(_) => panic!("Timeout waiting for window"),
            }
            
            // Stop the window manager
            window_manager.stop();
        });

    }

}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use arrow::array::{Int64Array, StringArray};
//     use arrow::datatypes::{DataType, Field, Schema};
//     use std::sync::{Arc, Mutex};
//     use tokio::runtime::Runtime;
//     use tokio::sync::mpsc;
//     use std::collections::HashSet;
    
//     // Helper to create a test batch
//     fn create_test_batch(batch_id: i64, values: &[i64]) -> RecordBatch {
//         let schema = Arc::new(Schema::new(vec![
//             Field::new("batch_id", DataType::Int64, false),
//             Field::new("value", DataType::Int64, false),
//         ]));
        
//         // Create batch_id column (all rows have same batch_id)
//         let batch_id_array = Int64Array::from(
//             vec![Some(batch_id); values.len()]
//         );
        
//         // Create value column
//         let value_array = Int64Array::from(
//             values.iter().map(|v| Some(*v)).collect::<Vec<_>>()
//         );
        
//         RecordBatch::try_new(
//             schema,
//             vec![Arc::new(batch_id_array), Arc::new(value_array)],
//         ).unwrap()
//     }
    
//     // Helper to extract batch_ids from a window
//     fn extract_batch_ids(window: &DataWindow) -> HashSet<i64> {
//         let mut batch_ids = HashSet::new();
        
//         for batch in &window.batches {
//             if let Some(batch_id_array) = batch.column_by_name("batch_id") {
//                 if let Some(id_array) = batch_id_array.as_any().downcast_ref::<Int64Array>() {
//                     for i in 0..id_array.len() {
//                         batch_ids.insert(id_array.value(i));
//                     }
//                 }
//             }
//         }
        
//         batch_ids
//     }
    
//     #[test]
//     fn test_window_rotation_with_processing() {
//         // Create a runtime for async test
//         let rt = Runtime::new().unwrap();
        
//         rt.block_on(async {
//             // Create a window manager with short windows for testing
//             let config = WindowManagerConfig {
//                 window_duration_secs: 1, // 1 second windows
//                 channel_buffer_size: 10,
//                 max_overflow_size: 100,
//                 ticker_interval_ms: 100, // Check every 100ms
//             };
            
//             let mut window_manager = WindowManager::new(config);
//             let mut window_rx = window_manager.start_rotation_ticker();
            
//             // Create a channel to track processed windows
//             let (processed_tx, mut processed_rx) = mpsc::channel::<(String, HashSet<i64>)>(20);
            
//             // Start the processing task
//             let processing_task = {
//                 let processed_tx = processed_tx.clone();
//                 tokio::spawn(async move {
//                     let mut processed_count = 0;
                    
//                     while let Some(window) = window_rx.recv().await {
//                         // Extract batch IDs for verification
//                         let window_id = window.id.clone();
//                         let batch_ids = extract_batch_ids(&window);
                        
//                         // Track that this window was processed
//                         processed_tx.send((window_id, batch_ids)).await.unwrap();
//                         processed_count += 1;
//                     }
                    
//                     processed_count
//                 })
//             };
            
//             // Add batches to the first window
//             let batch1 = create_test_batch(1, &[1, 2, 3]);
//             window_manager.add_batch(batch1).unwrap();
            
//             let batch2 = create_test_batch(2, &[4, 5, 6]);
//             window_manager.add_batch(batch2).unwrap();
            
//             // Wait for the first window to rotate
//             tokio::time::sleep(Duration::from_millis(1100)).await;
            
//             // Add batches to the second window
//             let batch3 = create_test_batch(3, &[7, 8, 9]);
//             window_manager.add_batch(batch3).unwrap();
            
//             let batch4 = create_test_batch(4, &[10, 11, 12]);
//             window_manager.add_batch(batch4).unwrap();
            
//             // Wait for the second window to rotate
//             tokio::time::sleep(Duration::from_millis(1100)).await;
            
//             // Add batches to the third window
//             let batch5 = create_test_batch(5, &[13, 14, 15]);
//             window_manager.add_batch(batch5).unwrap();
            
//             // Wait to ensure the third window has time to process
//             tokio::time::sleep(Duration::from_millis(1100)).await;
            
//             // Stop the window manager
//             window_manager.stop();
            
//             // Wait a bit more for processing to complete
//             tokio::time::sleep(Duration::from_millis(500)).await;
            
//             // Verify the windows were processed
//             let mut processed_windows = Vec::new();
//             while let Ok(window_info) = processed_rx.try_recv() {
//                 processed_windows.push(window_info);
//             }
            
//             // We should have at least 3 processed windows
//             assert!(processed_windows.len() >= 3, 
//                    "Expected at least 3 processed windows, got {}", processed_windows.len());
            
//             // Check the first window
//             if let Some((window_id, batch_ids)) = processed_windows.get(0) {
//                 assert_eq!(window_id, "window-1");
//                 assert!(batch_ids.contains(&1), "First window should contain batch 1");
//                 assert!(batch_ids.contains(&2), "First window should contain batch 2");
//                 assert!(!batch_ids.contains(&3), "First window should not contain batch 3");
//             }
            
//             // Check the second window
//             if let Some((window_id, batch_ids)) = processed_windows.get(1) {
//                 assert_eq!(window_id, "window-2");
//                 assert!(batch_ids.contains(&3), "Second window should contain batch 3");
//                 assert!(batch_ids.contains(&4), "Second window should contain batch 4");
//                 assert!(!batch_ids.contains(&5), "Second window should not contain batch 5");
//             }
            
//             // Check the third window
//             if let Some((window_id, batch_ids)) = processed_windows.get(2) {
//                 assert_eq!(window_id, "window-3");
//                 assert!(batch_ids.contains(&5), "Third window should contain batch 5");
//             }
            
//             // Abort the processing task
//             processing_task.abort();
//         });
//     }
    
//     #[test]
//     fn test_overlapping_window_rotation_and_batch_addition() {
//         // This test verifies that batches are correctly assigned to windows
//         // even when they're added during window rotation
//         let rt = Runtime::new().unwrap();
        
//         rt.block_on(async {
//             // Create a window manager with very short windows
//             let config = WindowManagerConfig {
//                 window_duration_secs: 1,
//                 channel_buffer_size: 10,
//                 max_overflow_size: 100,
//                 ticker_interval_ms: 100,
//             };
            
//             let mut window_manager = WindowManager::new(config);
//             let mut window_rx = window_manager.start_rotation_ticker();
            
//             // Create shared counters to track which window receives which batch
//             let window1_batches = Arc::new(Mutex::new(HashSet::new()));
//             let window2_batches = Arc::new(Mutex::new(HashSet::new()));
            
//             // Start the processing task
//             let processing_task = {
//                 let window1_batches = window1_batches.clone();
//                 let window2_batches = window2_batches.clone();
                
//                 tokio::spawn(async move {
//                     let mut window_count = 0;
                    
//                     while let Some(window) = window_rx.recv().await {
//                         window_count += 1;
//                         let batch_ids = extract_batch_ids(&window);
                        
//                         match window.id.as_str() {
//                             "window-1" => {
//                                 let mut w1 = window1_batches.lock().unwrap();
//                                 for id in batch_ids {
//                                     w1.insert(id);
//                                 }
//                             },
//                             "window-2" => {
//                                 let mut w2 = window2_batches.lock().unwrap();
//                                 for id in batch_ids {
//                                     w2.insert(id);
//                                 }
//                             },
//                             _ => {}
//                         }
//                     }
                    
//                     window_count
//                 })
//             };
            
//             // Add some initial batches
//             window_manager.add_batch(create_test_batch(1, &[1, 2, 3])).unwrap();
//             window_manager.add_batch(create_test_batch(2, &[4, 5, 6])).unwrap();
            
//             // Sleep until just before window rotation
//             tokio::time::sleep(Duration::from_millis(980)).await;
            
//             // Add a batch right around rotation time
//             window_manager.add_batch(create_test_batch(3, &[7, 8, 9])).unwrap();
            
//             // Sleep during potential rotation
//             tokio::time::sleep(Duration::from_millis(200)).await;
            
//             // Add another batch after rotation should have happened
//             window_manager.add_batch(create_test_batch(4, &[10, 11, 12])).unwrap();
            
//             // Wait for processing to complete
//             tokio::time::sleep(Duration::from_millis(1000)).await;
            
//             // Stop the window manager
//             window_manager.stop();
//             processing_task.abort();
            
//             // Check which batches ended up in which windows
//             let w1_batches = window1_batches.lock().unwrap();
//             let w2_batches = window2_batches.lock().unwrap();
            
//             // Batches 1 and 2 should definitely be in window 1
//             assert!(w1_batches.contains(&1), "Batch 1 should be in window 1");
//             assert!(w1_batches.contains(&2), "Batch 2 should be in window 1");
            
//             // Batch 4 should definitely be in window 2
//             assert!(w2_batches.contains(&4), "Batch 4 should be in window 2");
            
//             // Batch 3 could be in either window because it was added near rotation time
//             // But it must be in exactly one of them
//             assert!(
//                 (w1_batches.contains(&3) && !w2_batches.contains(&3)) || 
//                 (!w1_batches.contains(&3) && w2_batches.contains(&3)),
//                 "Batch 3 should be in exactly one window"
//             );
            
//             // Print which window batch 3 ended up in
//             if w1_batches.contains(&3) {
//                 println!("Batch 3 was added to window 1");
//             } else {
//                 println!("Batch 3 was added to window 2");
//             }
//         });
//     }
// }