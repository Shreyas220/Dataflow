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
            let mut window_counter = 2; 
            
            info!("Starting window rotation ticker with interval {:?}", ticker_interval);
            
            loop {
                interval.tick().await;
                
                // Check if we should exist | well just another food for thought question is why do we exist or why do we need to exist?
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
                    //the lock is released when the window is rotated
                    
                    debug!(
                        "Rotated window: {}, contains {} batches with {} rows",
                        rotated_window.id,
                        rotated_window.num_batches(),
                        rotated_window.num_rows()
                    );
                    
                    // Try to send the window for processing
                    // check if overflow len greater than 0 then then the overflow buffer first to maintain the order of the data

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
                                        // Channel is still full
                                        overflow_buffer.push(window);
                                        break;
                                    },
                                    Err(mpsc::error::TrySendError::Closed(window)) => {
                                        //BOOM!!!!
                                        error!("Processing channel closed - dropping buffered window");
                                        break;
                                    }
                                }
                            }
                        },
                        Err(mpsc::error::TrySendError::Full(window)) => {
                            // Backpressure detected 
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
        
        //sending back the receiver to send to prcossing the window 
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
