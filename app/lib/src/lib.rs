//! # ALS Compression Library
//!
//! Adaptive Logic Stream (ALS) compression library for structured data (CSV, JSON).
//!
//! This library provides high-performance compression using algorithmic pattern
//! description rather than raw enumeration, achieving superior compression ratios
//! for structured data.
//!
//! ## Features
//!
//! - **Pattern-based compression**: Detects and encodes sequential ranges, repetitions,
//!   and alternating patterns
//! - **Multiple formats**: Supports CSV and JSON input/output
//! - **Zero-copy parsing**: Minimizes memory allocations using borrowed references
//! - **SIMD acceleration**: Uses AVX2, AVX-512, or NEON instructions when available
//! - **Parallel processing**: Leverages multiple CPU cores for large datasets
//! - **Cross-platform**: Works on macOS, Windows, and Linux
//! - **Thread-safe**: All public types implement `Send + Sync`
//!
//! ## Quick Start
//!
//! ### Compression
//!
//! ```rust,ignore
//! use als_compression::AlsCompressor;
//!
//! // Create a compressor with default settings
//! let compressor = AlsCompressor::new();
//!
//! // Compress CSV data
//! let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
//! let als = compressor.compress_csv(csv)?;
//! println!("Compressed: {}", als);
//!
//! // Compress JSON data
//! let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
//! let als = compressor.compress_json(json)?;
//! ```
//!
//! ### Decompression
//!
//! ```rust,ignore
//! use als_compression::AlsParser;
//!
//! // Create a parser
//! let parser = AlsParser::new();
//!
//! // Parse ALS and convert to CSV
//! let als = "#id #name\n1>3|Alice Bob Charlie";
//! let csv = parser.to_csv(als)?;
//! println!("CSV: {}", csv);
//!
//! // Parse ALS and convert to JSON
//! let json = parser.to_json(als)?;
//! println!("JSON: {}", json);
//! ```
//!
//! ### Configuration
//!
//! ```rust,ignore
//! use als_compression::{AlsCompressor, CompressorConfig};
//!
//! // Create custom configuration
//! let config = CompressorConfig::default()
//!     .with_ctx_fallback_threshold(1.5)
//!     .with_min_pattern_length(4)
//!     .with_parallelism(4);
//!
//! let compressor = AlsCompressor::with_config(config);
//! ```
//!
//! ## Thread Safety
//!
//! All public types in this library are thread-safe (`Send + Sync`):
//!
//! - **Compression types**: [`AlsCompressor`], [`CompressionStats`], [`StatsSnapshot`]
//! - **Document types**: [`AlsDocument`], [`AlsOperator`], [`ColumnStream`]
//! - **Configuration types**: [`CompressorConfig`], [`ParserConfig`], [`SimdConfig`]
//! - **Data types**: [`TabularData`], [`Column`], [`Value`]
//!
//! ### Concurrent Compression
//!
//! The [`AlsCompressor`] can be safely shared across threads:
//!
//! ```rust,ignore
//! use als_compression::AlsCompressor;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let compressor = Arc::new(AlsCompressor::new());
//!
//! let handles: Vec<_> = (0..4).map(|i| {
//!     let compressor = Arc::clone(&compressor);
//!     thread::spawn(move || {
//!         let csv = format!("id,value\n{},{}", i, i * 10);
//!         compressor.compress_csv(&csv)
//!     })
//! }).collect();
//!
//! for handle in handles {
//!     let result = handle.join().unwrap();
//!     assert!(result.is_ok());
//! }
//! ```
//!
//! ### Thread-Safe Statistics
//!
//! The [`CompressionStats`] type uses atomic operations for lock-free updates:
//!
//! ```rust
//! use als_compression::CompressionStats;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let stats = Arc::new(CompressionStats::new());
//!
//! // Multiple threads can update stats concurrently
//! let handles: Vec<_> = (0..4).map(|_| {
//!     let stats = Arc::clone(&stats);
//!     thread::spawn(move || {
//!         for _ in 0..100 {
//!             stats.add_input_bytes(10);
//!         }
//!     })
//! }).collect();
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! assert_eq!(stats.get_input_bytes(), 4000);
//! ```
//!
//! ### Adaptive HashMap
//!
//! The [`AdaptiveMap`] type automatically selects between `HashMap` (for small
//! datasets) and `DashMap` (for large datasets). The `DashMap` variant provides
//! lock-free concurrent access for high-throughput scenarios.
//!
//! ## Advanced Examples
//!
//! ### Pattern Detection
//!
//! ```rust,ignore
//! use als_compression::AlsCompressor;
//!
//! let compressor = AlsCompressor::new();
//!
//! // Sequential ranges are detected automatically
//! let csv = "id\n1\n2\n3\n4\n5";
//! let als = compressor.compress_csv(csv)?;
//! // Output: #id\n1>5
//!
//! // Repetitions are compressed with multipliers
//! let csv = "status\nactive\nactive\nactive";
//! let als = compressor.compress_csv(csv)?;
//! // Output: #status\nactive*3
//!
//! // Alternating patterns use toggle syntax
//! let csv = "flag\ntrue\nfalse\ntrue\nfalse";
//! let als = compressor.compress_csv(csv)?;
//! // Output: #flag\ntrue~false*4
//! ```
//!
//! ### Streaming Large Files
//!
//! ```rust,ignore
//! use als_compression::{StreamingCompressor, StreamingParser};
//! use std::fs::File;
//! use std::io::BufReader;
//!
//! // Stream compression
//! let file = File::open("large_data.csv")?;
//! let reader = BufReader::new(file);
//! let mut compressor = StreamingCompressor::new(reader);
//!
//! for chunk in compressor.compress_chunks() {
//!     let als_chunk = chunk?;
//!     // Process chunk...
//! }
//!
//! // Stream decompression
//! let file = File::open("compressed.als")?;
//! let reader = BufReader::new(file);
//! let mut parser = StreamingParser::new(reader);
//!
//! for row in parser.parse_rows() {
//!     let values = row?;
//!     // Process row...
//! }
//! ```
//!
//! ### Async Operations
//!
//! ```rust,ignore
//! use als_compression::{AlsCompressor, AlsParser};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let compressor = AlsCompressor::new();
//!     let parser = AlsParser::new();
//!
//!     // Async compression
//!     let csv = "id,name\n1,Alice\n2,Bob";
//!     let als = compressor.compress_csv_async(csv).await?;
//!
//!     // Async decompression
//!     let csv_result = parser.to_csv_async(&als).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Working with TabularData
//!
//! ```rust,ignore
//! use als_compression::{AlsCompressor, AlsParser, TabularData, Column, Value};
//! use std::borrow::Cow;
//!
//! // Create tabular data manually
//! let mut data = TabularData::new();
//! data.add_column(Column::new(
//!     Cow::Borrowed("id"),
//!     vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
//! ));
//! data.add_column(Column::new(
//!     Cow::Borrowed("name"),
//!     vec![
//!         Value::String(Cow::Borrowed("Alice")),
//!         Value::String(Cow::Borrowed("Bob")),
//!         Value::String(Cow::Borrowed("Charlie"))
//!     ]
//! ));
//!
//! // Compress directly
//! let compressor = AlsCompressor::new();
//! let doc = compressor.compress(&data)?;
//! ```
//!
//! ### Error Handling
//!
//! ```rust,ignore
//! use als_compression::{AlsParser, AlsError};
//!
//! let parser = AlsParser::new();
//! let result = parser.to_csv("invalid als format");
//!
//! match result {
//!     Ok(csv) => println!("Success: {}", csv),
//!     Err(AlsError::AlsSyntaxError { position, message }) => {
//!         eprintln!("Syntax error at position {}: {}", position, message);
//!     }
//!     Err(AlsError::ColumnMismatch { schema, data }) => {
//!         eprintln!("Column mismatch: expected {}, got {}", schema, data);
//!     }
//!     Err(e) => eprintln!("Error: {}", e),
//! }
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]

// Module declarations
pub mod als;
pub mod compress;
pub mod config;
pub mod convert;
pub mod error;
pub mod hashmap;
pub mod pattern;
pub mod simd;
pub mod streaming;

// Python bindings (optional)
#[cfg(feature = "python")]
pub mod python;

// C FFI bindings (optional)
#[cfg(feature = "ffi")]
pub mod ffi;

// Re-exports for convenience
pub use als::{
    decode_als_value, encode_als_value, escape_als_string, is_empty_token, is_null_token,
    needs_escaping, unescape_als_string, AlsDocument, AlsOperator, AlsParser, AlsPrettyPrinter,
    AlsSerializer, ColumnStream, FormatIndicator, Token, Tokenizer, VersionType, EMPTY_TOKEN,
    NULL_TOKEN,
};
pub use config::{CompressorConfig, ParserConfig, SimdConfig};
pub use convert::{Column, ColumnType, TabularData, Value, parse_syslog, to_syslog, MessageType, SyslogEntry, parse_syslog_optimized};
pub use error::{AlsError, Result};
pub use pattern::{
    CombinedDetector, DetectionResult, PatternDetector, PatternEngine, PatternType,
    RangeDetector, RepeatDetector, RunDetector, ToggleDetector,
};
pub use compress::{
    AlsCompressor, ColumnStats, CompressionReport, CompressionStats, DictionaryBuilder,
    DictionaryEntry, EnumDetector, StatsSnapshot,
};
pub use hashmap::AdaptiveMap;
pub use simd::{CpuFeatures, SimdDispatcher, SimdLevel};
pub use streaming::{StreamingCompressor, StreamingParser};

/// Thread safety verification module.
///
/// This module contains compile-time assertions that verify all public types
/// implement `Send` and `Sync` traits, ensuring they can be safely used in
/// multi-threaded contexts.
///
/// # Thread Safety Guarantees
///
/// All public types in this library are designed to be thread-safe:
///
/// ## Compression Types
///
/// - [`AlsCompressor`]: Safe to share across threads. Each compression operation
///   is independent and doesn't modify shared state.
///
/// - [`CompressionStats`]: Uses atomic operations for all counters, allowing
///   concurrent updates from multiple threads without locks.
///
/// - [`StatsSnapshot`], [`ColumnStats`], [`CompressionReport`]: Immutable value
///   types that can be safely shared.
///
/// ## Data Types
///
/// - [`AlsDocument`], [`ColumnStream`], [`AlsOperator`]: Immutable after creation,
///   safe to share across threads.
///
/// - [`TabularData`], [`Column`], [`Value`]: Data containers that are safe to
///   share when not being mutated.
///
/// ## Configuration Types
///
/// - [`CompressorConfig`], [`ParserConfig`], [`SimdConfig`]: Immutable configuration
///   types that can be safely shared.
///
/// ## Concurrent Data Structures
///
/// - [`AdaptiveMap`]: Automatically selects between `HashMap` (for small datasets)
///   and `DashMap` (for large datasets). The `DashMap` variant provides lock-free
///   concurrent access.
///
/// # Example: Parallel Compression
///
/// ```rust,ignore
/// use als_compression::AlsCompressor;
/// use std::sync::Arc;
/// use std::thread;
///
/// let compressor = Arc::new(AlsCompressor::new());
///
/// let handles: Vec<_> = (0..4).map(|i| {
///     let compressor = Arc::clone(&compressor);
///     thread::spawn(move || {
///         let csv = format!("id,value\n{},{}", i, i * 10);
///         compressor.compress_csv(&csv)
///     })
/// }).collect();
///
/// for handle in handles {
///     let result = handle.join().unwrap();
///     assert!(result.is_ok());
/// }
/// ```
#[cfg(test)]
mod thread_safety {
    use super::*;

    /// Compile-time assertion that a type is Send + Sync.
    fn assert_send_sync<T: Send + Sync>() {}

    /// Verify all public compression types are thread-safe.
    #[test]
    fn compression_types_are_send_sync() {
        assert_send_sync::<AlsCompressor>();
        assert_send_sync::<CompressionStats>();
        assert_send_sync::<StatsSnapshot>();
        assert_send_sync::<ColumnStats>();
        assert_send_sync::<CompressionReport>();
        assert_send_sync::<DictionaryBuilder>();
        assert_send_sync::<DictionaryEntry>();
        assert_send_sync::<EnumDetector>();
    }

    /// Verify all public ALS document types are thread-safe.
    #[test]
    fn als_types_are_send_sync() {
        assert_send_sync::<AlsDocument>();
        assert_send_sync::<AlsOperator>();
        assert_send_sync::<ColumnStream>();
        assert_send_sync::<FormatIndicator>();
        assert_send_sync::<AlsParser>();
        assert_send_sync::<AlsSerializer>();
        assert_send_sync::<AlsPrettyPrinter>();
        assert_send_sync::<Token>();
        assert_send_sync::<Tokenizer>();
        assert_send_sync::<VersionType>();
    }

    /// Verify all public configuration types are thread-safe.
    #[test]
    fn config_types_are_send_sync() {
        assert_send_sync::<CompressorConfig>();
        assert_send_sync::<ParserConfig>();
        assert_send_sync::<SimdConfig>();
    }

    /// Verify all public data types are thread-safe.
    #[test]
    fn data_types_are_send_sync() {
        assert_send_sync::<TabularData<'static>>();
        assert_send_sync::<Column<'static>>();
        assert_send_sync::<Value<'static>>();
        assert_send_sync::<ColumnType>();
    }

    /// Verify all public pattern types are thread-safe.
    #[test]
    fn pattern_types_are_send_sync() {
        assert_send_sync::<PatternType>();
        assert_send_sync::<DetectionResult>();
        assert_send_sync::<PatternEngine>();
        assert_send_sync::<RangeDetector>();
        assert_send_sync::<RepeatDetector>();
        assert_send_sync::<ToggleDetector>();
        assert_send_sync::<CombinedDetector>();
        assert_send_sync::<RunDetector>();
    }

    /// Verify all public SIMD types are thread-safe.
    #[test]
    fn simd_types_are_send_sync() {
        assert_send_sync::<SimdDispatcher>();
        assert_send_sync::<CpuFeatures>();
        assert_send_sync::<SimdLevel>();
    }

    /// Verify all public hashmap types are thread-safe.
    #[test]
    fn hashmap_types_are_send_sync() {
        assert_send_sync::<AdaptiveMap<String, i32>>();
    }

    /// Verify all public streaming types are thread-safe.
    #[test]
    fn streaming_types_are_send_sync() {
        use std::io::Cursor;
        assert_send_sync::<StreamingCompressor<Cursor<Vec<u8>>>>();
        assert_send_sync::<StreamingParser<Cursor<Vec<u8>>>>();
    }

    /// Verify error types are thread-safe.
    #[test]
    fn error_types_are_send_sync() {
        assert_send_sync::<AlsError>();
    }

    /// Test concurrent access to CompressionStats.
    #[test]
    fn test_concurrent_stats_access() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(CompressionStats::new());
        let num_threads = 4;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..iterations {
                        stats.add_input_bytes(1);
                        stats.add_output_bytes(1);
                        let _ = stats.compression_ratio();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            stats.get_input_bytes(),
            (num_threads * iterations) as u64
        );
    }

    /// Test concurrent compression operations.
    #[test]
    fn test_concurrent_compression() {
        use std::sync::Arc;
        use std::thread;

        let compressor = Arc::new(AlsCompressor::new());
        let num_threads = 4;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let compressor = Arc::clone(&compressor);
                thread::spawn(move || {
                    let csv = format!("id,value\n{},{}\n{},{}", i, i * 10, i + 1, (i + 1) * 10);
                    compressor.compress_csv(&csv)
                })
            })
            .collect();

        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.is_ok());
        }
    }
}
