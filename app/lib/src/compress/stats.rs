//! Compression statistics tracking.
//!
//! This module provides thread-safe statistics tracking for compression operations
//! using atomic counters. Statistics include input/output sizes, patterns detected,
//! and per-column effectiveness metrics.
//!
//! # Thread Safety
//!
//! All types in this module are designed to be thread-safe:
//!
//! - [`CompressionStats`] uses atomic operations for all counters, allowing
//!   concurrent updates from multiple threads without locks. All operations
//!   use `Ordering::Relaxed` for maximum performance, which is appropriate
//!   since the counters are independent and don't require synchronization
//!   with other memory operations.
//!
//! - [`StatsSnapshot`], [`ColumnStats`], and [`CompressionReport`] are immutable
//!   value types that can be safely shared across threads.
//!
//! # Lock-Free Operations
//!
//! The [`CompressionStats`] struct provides lock-free reads and writes:
//!
//! - **Reads**: All getter methods (`get_*`) perform atomic loads with relaxed
//!   ordering, providing consistent values without blocking.
//!
//! - **Writes**: All update methods (`add_*`, `record_*`) use atomic fetch-and-add
//!   operations, allowing concurrent updates without data races.
//!
//! - **Snapshots**: The [`CompressionStats::snapshot`] method creates a point-in-time
//!   copy of all counters. Note that the snapshot may not be perfectly consistent
//!   if other threads are updating counters simultaneously, but each individual
//!   counter value will be valid.
//!
//! # Example
//!
//! ```
//! use als_compression::CompressionStats;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let stats = Arc::new(CompressionStats::new());
//!
//! // Spawn multiple threads that update stats concurrently
//! let handles: Vec<_> = (0..4).map(|_| {
//!     let stats = Arc::clone(&stats);
//!     thread::spawn(move || {
//!         for _ in 0..100 {
//!             stats.add_input_bytes(10);
//!             stats.record_raw_value();
//!         }
//!     })
//! }).collect();
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! // All updates are reflected
//! assert_eq!(stats.get_input_bytes(), 4000);
//! assert_eq!(stats.get_raw_values(), 400);
//! ```

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::pattern::PatternType;

/// Thread-safe compression statistics.
///
/// Uses atomic operations for all counters to ensure correctness when
/// accessed from multiple threads during parallel compression.
///
/// # Thread Safety
///
/// This struct is `Send + Sync`, meaning it can be safely shared across threads
/// and accessed concurrently. All counter operations are lock-free and use
/// atomic instructions.
///
/// # Memory Ordering
///
/// All operations use `Ordering::Relaxed` because:
/// - Counters are independent and don't need to synchronize with each other
/// - We only need eventual consistency, not strict ordering
/// - This provides the best performance for statistics collection
///
/// If you need a consistent view of all counters at a point in time, use
/// the [`snapshot`](Self::snapshot) method.
///
/// # Example
///
/// ```
/// use als_compression::CompressionStats;
///
/// let stats = CompressionStats::new();
///
/// // Thread-safe updates
/// stats.add_input_bytes(1000);
/// stats.add_output_bytes(500);
///
/// // Lock-free reads
/// let ratio = stats.compression_ratio();
/// assert_eq!(ratio, 2.0);
/// ```
#[derive(Debug, Default)]
pub struct CompressionStats {
    /// Total input bytes processed.
    pub input_bytes: AtomicU64,
    /// Total output bytes produced.
    pub output_bytes: AtomicU64,
    /// Number of patterns detected.
    pub patterns_detected: AtomicUsize,
    /// Number of range operators used.
    pub ranges_used: AtomicUsize,
    /// Number of multiplier operators used.
    pub multipliers_used: AtomicUsize,
    /// Number of toggle operators used.
    pub toggles_used: AtomicUsize,
    /// Number of dictionary references used.
    pub dict_refs_used: AtomicUsize,
    /// Number of raw values (no compression).
    pub raw_values: AtomicUsize,
    /// Number of columns processed.
    pub columns_processed: AtomicUsize,
    /// Number of columns that benefited from compression.
    pub columns_compressed: AtomicUsize,
}

impl CompressionStats {
    /// Create a new statistics tracker with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.input_bytes.store(0, Ordering::Relaxed);
        self.output_bytes.store(0, Ordering::Relaxed);
        self.patterns_detected.store(0, Ordering::Relaxed);
        self.ranges_used.store(0, Ordering::Relaxed);
        self.multipliers_used.store(0, Ordering::Relaxed);
        self.toggles_used.store(0, Ordering::Relaxed);
        self.dict_refs_used.store(0, Ordering::Relaxed);
        self.raw_values.store(0, Ordering::Relaxed);
        self.columns_processed.store(0, Ordering::Relaxed);
        self.columns_compressed.store(0, Ordering::Relaxed);
    }


    /// Calculate the compression ratio.
    ///
    /// Returns the ratio of input size to output size.
    /// A ratio > 1.0 means compression was achieved.
    /// Returns 0.0 if output is zero.
    pub fn compression_ratio(&self) -> f64 {
        let input = self.input_bytes.load(Ordering::Relaxed) as f64;
        let output = self.output_bytes.load(Ordering::Relaxed) as f64;
        if output > 0.0 {
            input / output
        } else {
            0.0
        }
    }

    /// Add to input bytes counter.
    pub fn add_input_bytes(&self, bytes: u64) {
        self.input_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add to output bytes counter.
    pub fn add_output_bytes(&self, bytes: u64) {
        self.output_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a pattern detection.
    pub fn record_pattern(&self, pattern_type: PatternType) {
        self.patterns_detected.fetch_add(1, Ordering::Relaxed);
        
        match pattern_type {
            PatternType::Sequential | PatternType::Arithmetic => {
                self.ranges_used.fetch_add(1, Ordering::Relaxed);
            }
            PatternType::Repeat => {
                self.multipliers_used.fetch_add(1, Ordering::Relaxed);
            }
            PatternType::Toggle | PatternType::RepeatedToggle => {
                self.toggles_used.fetch_add(1, Ordering::Relaxed);
            }
            PatternType::RepeatedRange => {
                self.ranges_used.fetch_add(1, Ordering::Relaxed);
                self.multipliers_used.fetch_add(1, Ordering::Relaxed);
            }
            PatternType::Raw => {
                self.raw_values.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record a dictionary reference usage.
    pub fn record_dict_ref(&self) {
        self.dict_refs_used.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple dictionary reference usages.
    pub fn record_dict_refs(&self, count: usize) {
        self.dict_refs_used.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a raw value (no compression).
    pub fn record_raw_value(&self) {
        self.raw_values.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple raw values.
    pub fn record_raw_values(&self, count: usize) {
        self.raw_values.fetch_add(count, Ordering::Relaxed);
    }

    /// Record a column being processed.
    pub fn record_column_processed(&self, was_compressed: bool) {
        self.columns_processed.fetch_add(1, Ordering::Relaxed);
        if was_compressed {
            self.columns_compressed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the input bytes count.
    pub fn get_input_bytes(&self) -> u64 {
        self.input_bytes.load(Ordering::Relaxed)
    }

    /// Get the output bytes count.
    pub fn get_output_bytes(&self) -> u64 {
        self.output_bytes.load(Ordering::Relaxed)
    }

    /// Get the number of patterns detected.
    pub fn get_patterns_detected(&self) -> usize {
        self.patterns_detected.load(Ordering::Relaxed)
    }

    /// Get the number of ranges used.
    pub fn get_ranges_used(&self) -> usize {
        self.ranges_used.load(Ordering::Relaxed)
    }

    /// Get the number of multipliers used.
    pub fn get_multipliers_used(&self) -> usize {
        self.multipliers_used.load(Ordering::Relaxed)
    }

    /// Get the number of toggles used.
    pub fn get_toggles_used(&self) -> usize {
        self.toggles_used.load(Ordering::Relaxed)
    }

    /// Get the number of dictionary references used.
    pub fn get_dict_refs_used(&self) -> usize {
        self.dict_refs_used.load(Ordering::Relaxed)
    }

    /// Get the number of raw values.
    pub fn get_raw_values(&self) -> usize {
        self.raw_values.load(Ordering::Relaxed)
    }

    /// Get the number of columns processed.
    pub fn get_columns_processed(&self) -> usize {
        self.columns_processed.load(Ordering::Relaxed)
    }

    /// Get the number of columns that benefited from compression.
    pub fn get_columns_compressed(&self) -> usize {
        self.columns_compressed.load(Ordering::Relaxed)
    }

    /// Get the column compression effectiveness as a percentage.
    ///
    /// Returns the percentage of columns that benefited from compression.
    pub fn column_effectiveness(&self) -> f64 {
        let processed = self.columns_processed.load(Ordering::Relaxed) as f64;
        let compressed = self.columns_compressed.load(Ordering::Relaxed) as f64;
        if processed > 0.0 {
            (compressed / processed) * 100.0
        } else {
            0.0
        }
    }

    /// Create a snapshot of the current statistics.
    ///
    /// This is useful for reporting statistics at a point in time
    /// without holding references to the atomic counters.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            input_bytes: self.input_bytes.load(Ordering::Relaxed),
            output_bytes: self.output_bytes.load(Ordering::Relaxed),
            patterns_detected: self.patterns_detected.load(Ordering::Relaxed),
            ranges_used: self.ranges_used.load(Ordering::Relaxed),
            multipliers_used: self.multipliers_used.load(Ordering::Relaxed),
            toggles_used: self.toggles_used.load(Ordering::Relaxed),
            dict_refs_used: self.dict_refs_used.load(Ordering::Relaxed),
            raw_values: self.raw_values.load(Ordering::Relaxed),
            columns_processed: self.columns_processed.load(Ordering::Relaxed),
            columns_compressed: self.columns_compressed.load(Ordering::Relaxed),
        }
    }
}


/// A point-in-time snapshot of compression statistics.
///
/// This struct contains non-atomic copies of all statistics counters,
/// useful for reporting and serialization.
///
/// # Thread Safety
///
/// This struct is `Send + Sync` and can be safely shared across threads.
/// Since it contains only primitive values (no interior mutability),
/// it is inherently thread-safe.
///
/// # Note
///
/// When created from a [`CompressionStats`] via [`CompressionStats::snapshot`],
/// the snapshot represents the counter values at approximately the same point
/// in time. However, if other threads are actively updating the stats, individual
/// counter values may be from slightly different moments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total input bytes processed.
    pub input_bytes: u64,
    /// Total output bytes produced.
    pub output_bytes: u64,
    /// Number of patterns detected.
    pub patterns_detected: usize,
    /// Number of range operators used.
    pub ranges_used: usize,
    /// Number of multiplier operators used.
    pub multipliers_used: usize,
    /// Number of toggle operators used.
    pub toggles_used: usize,
    /// Number of dictionary references used.
    pub dict_refs_used: usize,
    /// Number of raw values (no compression).
    pub raw_values: usize,
    /// Number of columns processed.
    pub columns_processed: usize,
    /// Number of columns that benefited from compression.
    pub columns_compressed: usize,
}

impl StatsSnapshot {
    /// Calculate the compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        if self.output_bytes > 0 {
            self.input_bytes as f64 / self.output_bytes as f64
        } else {
            0.0
        }
    }

    /// Get the column compression effectiveness as a percentage.
    pub fn column_effectiveness(&self) -> f64 {
        if self.columns_processed > 0 {
            (self.columns_compressed as f64 / self.columns_processed as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// Per-column compression statistics.
///
/// # Thread Safety
///
/// This struct is `Send + Sync` and can be safely shared across threads.
/// It is an immutable value type with no interior mutability.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// Column name.
    pub name: String,
    /// Column index.
    pub index: usize,
    /// Input size in bytes for this column.
    pub input_bytes: usize,
    /// Output size in bytes for this column.
    pub output_bytes: usize,
    /// Pattern type used for this column.
    pub pattern_type: PatternType,
    /// Number of values in the column.
    pub value_count: usize,
    /// Whether the column benefited from compression.
    pub was_compressed: bool,
}

impl ColumnStats {
    /// Create new column statistics.
    pub fn new(
        name: String,
        index: usize,
        input_bytes: usize,
        output_bytes: usize,
        pattern_type: PatternType,
        value_count: usize,
    ) -> Self {
        Self {
            name,
            index,
            input_bytes,
            output_bytes,
            pattern_type,
            value_count,
            was_compressed: output_bytes < input_bytes,
        }
    }

    /// Calculate the compression ratio for this column.
    pub fn compression_ratio(&self) -> f64 {
        if self.output_bytes > 0 {
            self.input_bytes as f64 / self.output_bytes as f64
        } else {
            0.0
        }
    }

    /// Calculate bytes saved by compression.
    pub fn bytes_saved(&self) -> i64 {
        self.input_bytes as i64 - self.output_bytes as i64
    }
}

/// Detailed compression report with per-column statistics.
///
/// # Thread Safety
///
/// This struct is `Send + Sync` and can be safely shared across threads.
/// It is an immutable value type with no interior mutability.
#[derive(Debug, Clone)]
pub struct CompressionReport {
    /// Overall statistics snapshot.
    pub overall: StatsSnapshot,
    /// Per-column statistics.
    pub columns: Vec<ColumnStats>,
    /// Whether CTX fallback was used.
    pub used_ctx_fallback: bool,
    /// Dictionary utilization (entries used / total entries).
    pub dictionary_utilization: f64,
}

impl CompressionReport {
    /// Create a new compression report.
    pub fn new(
        overall: StatsSnapshot,
        columns: Vec<ColumnStats>,
        used_ctx_fallback: bool,
        dictionary_utilization: f64,
    ) -> Self {
        Self {
            overall,
            columns,
            used_ctx_fallback,
            dictionary_utilization,
        }
    }

    /// Get the most effective column (highest compression ratio).
    pub fn most_effective_column(&self) -> Option<&ColumnStats> {
        self.columns
            .iter()
            .filter(|c| c.was_compressed)
            .max_by(|a, b| {
                a.compression_ratio()
                    .partial_cmp(&b.compression_ratio())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Get the least effective column (lowest compression ratio).
    pub fn least_effective_column(&self) -> Option<&ColumnStats> {
        self.columns
            .iter()
            .min_by(|a, b| {
                a.compression_ratio()
                    .partial_cmp(&b.compression_ratio())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    /// Get total bytes saved across all columns.
    pub fn total_bytes_saved(&self) -> i64 {
        self.columns.iter().map(|c| c.bytes_saved()).sum()
    }

    /// Get the number of columns that benefited from compression.
    pub fn compressed_column_count(&self) -> usize {
        self.columns.iter().filter(|c| c.was_compressed).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_stats_new() {
        let stats = CompressionStats::new();
        assert_eq!(stats.get_input_bytes(), 0);
        assert_eq!(stats.get_output_bytes(), 0);
        assert_eq!(stats.get_patterns_detected(), 0);
    }

    #[test]
    fn test_compression_stats_add_bytes() {
        let stats = CompressionStats::new();
        stats.add_input_bytes(100);
        stats.add_output_bytes(50);
        
        assert_eq!(stats.get_input_bytes(), 100);
        assert_eq!(stats.get_output_bytes(), 50);
    }

    #[test]
    fn test_compression_ratio() {
        let stats = CompressionStats::new();
        stats.add_input_bytes(100);
        stats.add_output_bytes(50);
        
        assert_eq!(stats.compression_ratio(), 2.0);
    }

    #[test]
    fn test_compression_ratio_zero_output() {
        let stats = CompressionStats::new();
        stats.add_input_bytes(100);
        
        assert_eq!(stats.compression_ratio(), 0.0);
    }

    #[test]
    fn test_record_pattern_range() {
        let stats = CompressionStats::new();
        stats.record_pattern(PatternType::Sequential);
        
        assert_eq!(stats.get_patterns_detected(), 1);
        assert_eq!(stats.get_ranges_used(), 1);
    }

    #[test]
    fn test_record_pattern_repeat() {
        let stats = CompressionStats::new();
        stats.record_pattern(PatternType::Repeat);
        
        assert_eq!(stats.get_patterns_detected(), 1);
        assert_eq!(stats.get_multipliers_used(), 1);
    }

    #[test]
    fn test_record_pattern_toggle() {
        let stats = CompressionStats::new();
        stats.record_pattern(PatternType::Toggle);
        
        assert_eq!(stats.get_patterns_detected(), 1);
        assert_eq!(stats.get_toggles_used(), 1);
    }

    #[test]
    fn test_record_pattern_repeated_range() {
        let stats = CompressionStats::new();
        stats.record_pattern(PatternType::RepeatedRange);
        
        assert_eq!(stats.get_patterns_detected(), 1);
        assert_eq!(stats.get_ranges_used(), 1);
        assert_eq!(stats.get_multipliers_used(), 1);
    }

    #[test]
    fn test_record_pattern_raw() {
        let stats = CompressionStats::new();
        stats.record_pattern(PatternType::Raw);
        
        assert_eq!(stats.get_patterns_detected(), 1);
        assert_eq!(stats.get_raw_values(), 1);
    }

    #[test]
    fn test_record_dict_refs() {
        let stats = CompressionStats::new();
        stats.record_dict_ref();
        stats.record_dict_refs(5);
        
        assert_eq!(stats.get_dict_refs_used(), 6);
    }

    #[test]
    fn test_record_raw_values() {
        let stats = CompressionStats::new();
        stats.record_raw_value();
        stats.record_raw_values(3);
        
        assert_eq!(stats.get_raw_values(), 4);
    }

    #[test]
    fn test_record_column_processed() {
        let stats = CompressionStats::new();
        stats.record_column_processed(true);
        stats.record_column_processed(false);
        stats.record_column_processed(true);
        
        assert_eq!(stats.get_columns_processed(), 3);
        assert_eq!(stats.get_columns_compressed(), 2);
    }

    #[test]
    fn test_column_effectiveness() {
        let stats = CompressionStats::new();
        stats.record_column_processed(true);
        stats.record_column_processed(true);
        stats.record_column_processed(false);
        stats.record_column_processed(false);
        
        assert_eq!(stats.column_effectiveness(), 50.0);
    }

    #[test]
    fn test_column_effectiveness_zero() {
        let stats = CompressionStats::new();
        assert_eq!(stats.column_effectiveness(), 0.0);
    }

    #[test]
    fn test_reset() {
        let stats = CompressionStats::new();
        stats.add_input_bytes(100);
        stats.add_output_bytes(50);
        stats.record_pattern(PatternType::Sequential);
        
        stats.reset();
        
        assert_eq!(stats.get_input_bytes(), 0);
        assert_eq!(stats.get_output_bytes(), 0);
        assert_eq!(stats.get_patterns_detected(), 0);
    }

    #[test]
    fn test_snapshot() {
        let stats = CompressionStats::new();
        stats.add_input_bytes(100);
        stats.add_output_bytes(50);
        stats.record_pattern(PatternType::Sequential);
        stats.record_column_processed(true);
        
        let snapshot = stats.snapshot();
        
        assert_eq!(snapshot.input_bytes, 100);
        assert_eq!(snapshot.output_bytes, 50);
        assert_eq!(snapshot.patterns_detected, 1);
        assert_eq!(snapshot.ranges_used, 1);
        assert_eq!(snapshot.columns_processed, 1);
        assert_eq!(snapshot.columns_compressed, 1);
    }

    #[test]
    fn test_stats_snapshot_compression_ratio() {
        let snapshot = StatsSnapshot {
            input_bytes: 100,
            output_bytes: 50,
            patterns_detected: 0,
            ranges_used: 0,
            multipliers_used: 0,
            toggles_used: 0,
            dict_refs_used: 0,
            raw_values: 0,
            columns_processed: 0,
            columns_compressed: 0,
        };
        
        assert_eq!(snapshot.compression_ratio(), 2.0);
    }

    #[test]
    fn test_column_stats_new() {
        let stats = ColumnStats::new(
            "test_col".to_string(),
            0,
            100,
            50,
            PatternType::Sequential,
            10,
        );
        
        assert_eq!(stats.name, "test_col");
        assert_eq!(stats.index, 0);
        assert_eq!(stats.input_bytes, 100);
        assert_eq!(stats.output_bytes, 50);
        assert!(stats.was_compressed);
    }

    #[test]
    fn test_column_stats_compression_ratio() {
        let stats = ColumnStats::new(
            "test".to_string(),
            0,
            100,
            25,
            PatternType::Sequential,
            10,
        );
        
        assert_eq!(stats.compression_ratio(), 4.0);
    }

    #[test]
    fn test_column_stats_bytes_saved() {
        let stats = ColumnStats::new(
            "test".to_string(),
            0,
            100,
            25,
            PatternType::Sequential,
            10,
        );
        
        assert_eq!(stats.bytes_saved(), 75);
    }

    #[test]
    fn test_column_stats_not_compressed() {
        let stats = ColumnStats::new(
            "test".to_string(),
            0,
            50,
            100,
            PatternType::Raw,
            10,
        );
        
        assert!(!stats.was_compressed);
        assert_eq!(stats.bytes_saved(), -50);
    }

    #[test]
    fn test_compression_report() {
        let overall = StatsSnapshot {
            input_bytes: 200,
            output_bytes: 100,
            patterns_detected: 2,
            ranges_used: 1,
            multipliers_used: 1,
            toggles_used: 0,
            dict_refs_used: 0,
            raw_values: 0,
            columns_processed: 2,
            columns_compressed: 2,
        };
        
        let columns = vec![
            ColumnStats::new("col1".to_string(), 0, 100, 25, PatternType::Sequential, 10),
            ColumnStats::new("col2".to_string(), 1, 100, 75, PatternType::Repeat, 10),
        ];
        
        let report = CompressionReport::new(overall, columns, false, 0.8);
        
        assert_eq!(report.total_bytes_saved(), 100);
        assert_eq!(report.compressed_column_count(), 2);
        assert!(!report.used_ctx_fallback);
        assert_eq!(report.dictionary_utilization, 0.8);
    }

    #[test]
    fn test_compression_report_most_effective() {
        let overall = StatsSnapshot {
            input_bytes: 200,
            output_bytes: 100,
            patterns_detected: 2,
            ranges_used: 1,
            multipliers_used: 1,
            toggles_used: 0,
            dict_refs_used: 0,
            raw_values: 0,
            columns_processed: 2,
            columns_compressed: 2,
        };
        
        let columns = vec![
            ColumnStats::new("col1".to_string(), 0, 100, 25, PatternType::Sequential, 10),
            ColumnStats::new("col2".to_string(), 1, 100, 75, PatternType::Repeat, 10),
        ];
        
        let report = CompressionReport::new(overall, columns, false, 0.8);
        
        let most_effective = report.most_effective_column().unwrap();
        assert_eq!(most_effective.name, "col1");
        assert_eq!(most_effective.compression_ratio(), 4.0);
    }

    #[test]
    fn test_compression_report_least_effective() {
        let overall = StatsSnapshot {
            input_bytes: 200,
            output_bytes: 100,
            patterns_detected: 2,
            ranges_used: 1,
            multipliers_used: 1,
            toggles_used: 0,
            dict_refs_used: 0,
            raw_values: 0,
            columns_processed: 2,
            columns_compressed: 2,
        };
        
        let columns = vec![
            ColumnStats::new("col1".to_string(), 0, 100, 25, PatternType::Sequential, 10),
            ColumnStats::new("col2".to_string(), 1, 100, 75, PatternType::Repeat, 10),
        ];
        
        let report = CompressionReport::new(overall, columns, false, 0.8);
        
        let least_effective = report.least_effective_column().unwrap();
        assert_eq!(least_effective.name, "col2");
    }

    #[test]
    fn test_stats_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompressionStats>();
        assert_send_sync::<StatsSnapshot>();
        assert_send_sync::<ColumnStats>();
        assert_send_sync::<CompressionReport>();
    }

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(CompressionStats::new());
        let num_threads = 4;
        let iterations_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..iterations_per_thread {
                        stats.add_input_bytes(10);
                        stats.add_output_bytes(5);
                        stats.record_raw_value();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all updates were recorded
        let expected_total = num_threads * iterations_per_thread;
        assert_eq!(stats.get_input_bytes(), (expected_total * 10) as u64);
        assert_eq!(stats.get_output_bytes(), (expected_total * 5) as u64);
        assert_eq!(stats.get_raw_values(), expected_total);
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(CompressionStats::new());
        let num_writers = 2;
        let num_readers = 2;
        let iterations = 500;

        // Spawn writer threads
        let writer_handles: Vec<_> = (0..num_writers)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..iterations {
                        stats.add_input_bytes(1);
                        stats.record_pattern(PatternType::Sequential);
                    }
                })
            })
            .collect();

        // Spawn reader threads
        let reader_handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    let mut snapshots = Vec::new();
                    for _ in 0..iterations {
                        // Take snapshots while writers are active
                        snapshots.push(stats.snapshot());
                        // Also read individual values
                        let _ = stats.get_input_bytes();
                        let _ = stats.compression_ratio();
                    }
                    snapshots
                })
            })
            .collect();

        // Wait for all threads
        for handle in writer_handles {
            handle.join().unwrap();
        }

        let all_snapshots: Vec<Vec<StatsSnapshot>> = reader_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        // Verify final state
        let expected_input = (num_writers * iterations) as u64;
        assert_eq!(stats.get_input_bytes(), expected_input);

        // Verify snapshots are valid (values should be monotonically increasing or equal)
        for snapshots in all_snapshots {
            for window in snapshots.windows(2) {
                // Input bytes should never decrease
                assert!(window[1].input_bytes >= window[0].input_bytes);
            }
        }
    }

    #[test]
    fn test_reset_is_atomic() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(CompressionStats::new());
        
        // Pre-populate with some data
        stats.add_input_bytes(1000);
        stats.add_output_bytes(500);
        stats.record_raw_values(100);

        // Spawn threads that will reset and read
        let stats_clone = Arc::clone(&stats);
        let reset_handle = thread::spawn(move || {
            stats_clone.reset();
        });

        let stats_clone = Arc::clone(&stats);
        let read_handle = thread::spawn(move || {
            // Read values - they should be either pre-reset or post-reset
            let input = stats_clone.get_input_bytes();
            let output = stats_clone.get_output_bytes();
            (input, output)
        });

        reset_handle.join().unwrap();
        let (input, output) = read_handle.join().unwrap();

        // Values should be either original or zero (after reset)
        assert!(input == 0 || input == 1000);
        assert!(output == 0 || output == 500);

        // After both threads complete, values should be zero
        assert_eq!(stats.get_input_bytes(), 0);
        assert_eq!(stats.get_output_bytes(), 0);
    }
}
