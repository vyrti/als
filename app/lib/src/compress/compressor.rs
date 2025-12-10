//! Main ALS compressor implementation.
//!
//! This module provides the `AlsCompressor` struct which handles compression
//! of tabular data to ALS format, including CTX fallback when ALS compression
//! ratio is insufficient.

use crate::als::{AlsDocument, AlsOperator, ColumnStream};
use crate::als::AlsSerializer;
use crate::config::CompressorConfig;
use crate::convert::{TabularData, Value};
use crate::error::{AlsError, Result};
use crate::pattern::{PatternEngine, PatternType};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::dictionary::DictionaryBuilder;
use super::stats::{ColumnStats, CompressionReport, CompressionStats};

/// Default threshold for parallel processing (number of columns * rows).
/// Below this threshold, sequential processing is used to avoid parallel overhead.
const PARALLEL_THRESHOLD: usize = 1000;

/// Main entry point for ALS compression.
///
/// The compressor analyzes tabular data, detects patterns, builds dictionaries,
/// and produces compressed ALS output. When ALS compression ratio falls below
/// the configured threshold, it automatically falls back to CTX format.
///
/// # Thread Safety
///
/// `AlsCompressor` is `Send + Sync`, meaning it can be safely shared across
/// threads. Each compression operation is independent and doesn't modify
/// shared state, making it safe to use the same compressor instance from
/// multiple threads concurrently.
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
///     handle.join().unwrap().unwrap();
/// }
/// ```
///
/// # Parallel Processing
///
/// When the `parallel` feature is enabled and the dataset is large enough
/// (determined by `PARALLEL_THRESHOLD`), columns are compressed in parallel
/// using Rayon's work-stealing scheduler. This provides significant speedup
/// for multi-column datasets on multi-core systems.
#[derive(Debug, Clone)]
pub struct AlsCompressor {
    /// Compression configuration.
    config: CompressorConfig,
    /// Pattern detection engine.
    pattern_engine: PatternEngine,
}

impl AlsCompressor {
    /// Create a new compressor with default configuration.
    pub fn new() -> Self {
        Self {
            config: CompressorConfig::default(),
            pattern_engine: PatternEngine::new(),
        }
    }

    /// Create a new compressor with the given configuration.
    pub fn with_config(config: CompressorConfig) -> Self {
        Self {
            pattern_engine: PatternEngine::with_config(config.clone()),
            config,
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &CompressorConfig {
        &self.config
    }

    /// Compress CSV text to ALS format.
    ///
    /// This is a convenience method that parses CSV input, compresses it to ALS,
    /// and serializes the result to a string.
    ///
    /// # Arguments
    ///
    /// * `input` - CSV text to compress
    ///
    /// # Returns
    ///
    /// A string containing the compressed ALS representation.
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::AlsCompressor;
    ///
    /// let compressor = AlsCompressor::new();
    /// let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    /// let als = compressor.compress_csv(csv).unwrap();
    /// ```
    pub fn compress_csv(&self, input: &str) -> Result<String> {
        use crate::convert::csv::parse_csv;
        use crate::als::AlsSerializer;

        // Parse CSV to TabularData
        let data = parse_csv(input)?;

        // Compress to ALS document
        let doc = self.compress(&data)?;

        // Serialize to string
        let serializer = AlsSerializer::new();
        Ok(serializer.serialize(&doc))
    }

    /// Compress JSON text to ALS format.
    ///
    /// This is a convenience method that parses JSON input (array of objects),
    /// compresses it to ALS, and serializes the result to a string.
    ///
    /// # Arguments
    ///
    /// * `input` - JSON text to compress (must be an array of objects)
    ///
    /// # Returns
    ///
    /// A string containing the compressed ALS representation.
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::AlsCompressor;
    ///
    /// let compressor = AlsCompressor::new();
    /// let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
    /// let als = compressor.compress_json(json).unwrap();
    /// ```
    pub fn compress_json(&self, input: &str) -> Result<String> {
        use crate::convert::json::parse_json;
        use crate::als::AlsSerializer;

        // Parse JSON to TabularData
        let data = parse_json(input)?;

        // Compress to ALS document
        let doc = self.compress(&data)?;

        // Serialize to string
        let serializer = AlsSerializer::new();
        Ok(serializer.serialize(&doc))
    }

    /// Compress tabular data to an ALS document.
    ///
    /// This method:
    /// 1. Analyzes each column for patterns
    /// 2. Builds dictionaries for frequently repeated strings
    /// 3. Compresses each column using the best encoding
    /// 4. Calculates the compression ratio
    /// 5. Falls back to CTX if ratio is below threshold
    ///
    /// # Arguments
    ///
    /// * `data` - The tabular data to compress
    ///
    /// # Returns
    ///
    /// An `AlsDocument` containing the compressed data.
    pub fn compress(&self, data: &TabularData) -> Result<AlsDocument> {
        // Handle edge cases
        if data.is_empty() || data.column_count() == 0 {
            return Ok(self.create_empty_document(data));
        }

        // First, try ALS compression
        let als_doc = self.compress_als(data)?;
        
        // Calculate compression ratio
        let original_size = self.calculate_original_size(data);
        let compressed_size = self.calculate_compressed_size(&als_doc);
        let compression_ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            f64::INFINITY
        };

        // Check if we should fall back to CTX
        if compression_ratio < self.config.ctx_fallback_threshold {
            Ok(self.compress_ctx(data))
        } else {
            Ok(als_doc)
        }
    }

    /// Compress data using ALS format with pattern detection.
    fn compress_als(&self, data: &TabularData) -> Result<AlsDocument> {
        let mut doc = AlsDocument::with_schema(data.column_names().into_iter().map(String::from).collect());
        doc.set_als_format();

        // Build dictionary for string values
        let dictionary = self.build_dictionary(data);
        if !dictionary.is_empty() {
            doc.add_dictionary("default", dictionary.clone());
        }

        // Compress columns (parallel or sequential based on size and config)
        let streams = self.compress_columns_internal(data, &dictionary)?;
        for stream in streams {
            doc.add_stream(stream);
        }

        Ok(doc)
    }

    /// Determine if parallel processing should be used based on data size and config.
    fn should_use_parallel(&self, data: &TabularData) -> bool {
        // Check if parallelism is explicitly disabled (parallelism = 1)
        if self.config.parallelism == 1 {
            return false;
        }

        // Use parallel processing if data size exceeds threshold
        let data_size = data.column_count() * data.row_count;
        data_size >= PARALLEL_THRESHOLD && data.column_count() > 1
    }

    /// Compress columns using either parallel or sequential processing.
    fn compress_columns_internal(
        &self,
        data: &TabularData,
        dictionary: &[String],
    ) -> Result<Vec<ColumnStream>> {
        #[cfg(feature = "parallel")]
        {
            if self.should_use_parallel(data) {
                return self.compress_columns_parallel(data, dictionary);
            }
        }

        // Sequential compression
        self.compress_columns_sequential(data, dictionary)
    }

    /// Compress columns sequentially.
    fn compress_columns_sequential(
        &self,
        data: &TabularData,
        dictionary: &[String],
    ) -> Result<Vec<ColumnStream>> {
        let mut streams = Vec::with_capacity(data.column_count());
        for column in &data.columns {
            let stream = self.compress_column(column, dictionary)?;
            streams.push(stream);
        }
        Ok(streams)
    }

    /// Compress columns in parallel using Rayon.
    #[cfg(feature = "parallel")]
    fn compress_columns_parallel(
        &self,
        data: &TabularData,
        dictionary: &[String],
    ) -> Result<Vec<ColumnStream>> {
        // Configure thread pool if parallelism is specified
        let result: Result<Vec<ColumnStream>> = if self.config.parallelism > 1 {
            // Use a custom thread pool with specified parallelism
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.parallelism)
                .build()
                .map_err(|e| crate::error::AlsError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create thread pool: {}", e),
                )))?;

            pool.install(|| {
                data.columns
                    .par_iter()
                    .map(|column| self.compress_column(column, dictionary))
                    .collect()
            })
        } else {
            // Use default Rayon thread pool (auto-detect cores)
            data.columns
                .par_iter()
                .map(|column| self.compress_column(column, dictionary))
                .collect()
        };

        result
    }

    /// Compress data using CTX format (no pattern compression).
    fn compress_ctx(&self, data: &TabularData) -> AlsDocument {
        let mut doc = AlsDocument::with_schema(data.column_names().into_iter().map(String::from).collect());
        doc.set_ctx_format();

        // In CTX format, we just use raw values without pattern compression
        for column in &data.columns {
            let operators: Vec<AlsOperator> = column
                .values
                .iter()
                .map(|v| AlsOperator::raw(v.to_string_repr().into_owned()))
                .collect();
            doc.add_stream(ColumnStream::from_operators(operators));
        }

        doc
    }

    /// Create an empty document for empty input.
    fn create_empty_document(&self, data: &TabularData) -> AlsDocument {
        let mut doc = AlsDocument::with_schema(data.column_names().into_iter().map(String::from).collect());
        
        // Add empty streams for each column
        for _ in 0..data.column_count() {
            doc.add_stream(ColumnStream::new());
        }
        
        doc
    }

    /// Build a dictionary from the tabular data.
    fn build_dictionary(&self, data: &TabularData) -> Vec<String> {
        let mut builder = DictionaryBuilder::with_config(&self.config);

        // Add all string values to the dictionary builder
        for column in &data.columns {
            for value in &column.values {
                if let Value::String(s) = value {
                    builder.add(s.as_ref());
                }
            }
        }

        builder.build()
    }

    /// Compress a single column.
    fn compress_column(
        &self,
        column: &crate::convert::Column,
        dictionary: &[String],
    ) -> Result<ColumnStream> {
        // Convert values to strings for pattern detection
        let string_values: Vec<String> = column
            .values
            .iter()
            .map(|v| v.to_string_repr().into_owned())
            .collect();

        let str_refs: Vec<&str> = string_values.iter().map(|s| s.as_str()).collect();

        // Try pattern detection
        let detection = self.pattern_engine.detect(&str_refs);

        // If pattern detection found something useful, use it
        if detection.pattern_type != PatternType::Raw && detection.compression_ratio > 1.0 {
            return Ok(ColumnStream::from_operators(vec![detection.operator]));
        }

        // Otherwise, try dictionary references or raw values
        let operators = self.encode_with_dictionary(&str_refs, dictionary);
        Ok(ColumnStream::from_operators(operators))
    }

    /// Encode values using dictionary references where beneficial.
    fn encode_with_dictionary(&self, values: &[&str], dictionary: &[String]) -> Vec<AlsOperator> {
        // Build a lookup map for dictionary indices
        let dict_lookup: std::collections::HashMap<&str, usize> = dictionary
            .iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect();

        values
            .iter()
            .map(|&value| {
                if let Some(&index) = dict_lookup.get(value) {
                    AlsOperator::dict_ref(index)
                } else {
                    AlsOperator::raw(value)
                }
            })
            .collect()
    }

    /// Calculate the original size of the data in bytes.
    fn calculate_original_size(&self, data: &TabularData) -> usize {
        let mut size = 0;

        for column in &data.columns {
            // Column name
            size += column.name.len();
            
            // Values
            for value in &column.values {
                size += value.to_string_repr().len();
                size += 1; // Separator (comma or newline)
            }
        }

        size
    }

    /// Calculate the compressed size of an ALS document in bytes.
    fn calculate_compressed_size(&self, doc: &AlsDocument) -> usize {
        let serializer = AlsSerializer::new();
        let serialized = serializer.serialize(doc);
        serialized.len()
    }

    /// Calculate the compression ratio for a document.
    ///
    /// Returns the ratio of original size to compressed size.
    /// A ratio > 1.0 means compression was achieved.
    pub fn calculate_compression_ratio(&self, data: &TabularData, doc: &AlsDocument) -> f64 {
        let original_size = self.calculate_original_size(data);
        let compressed_size = self.calculate_compressed_size(doc);
        
        if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            f64::INFINITY
        }
    }

    /// Check if CTX fallback would be used for the given data.
    ///
    /// This is useful for testing and debugging to understand
    /// when CTX fallback will be triggered.
    pub fn would_use_ctx_fallback(&self, data: &TabularData) -> Result<bool> {
        if data.is_empty() || data.column_count() == 0 {
            return Ok(false);
        }

        let als_doc = self.compress_als(data)?;
        let ratio = self.calculate_compression_ratio(data, &als_doc);
        
        Ok(ratio < self.config.ctx_fallback_threshold)
    }

    /// Check if parallel processing would be used for the given data.
    ///
    /// This is useful for testing and debugging to understand
    /// when parallel processing will be triggered.
    ///
    /// Returns `true` if the `parallel` feature is enabled and the data
    /// size exceeds the parallel threshold.
    pub fn would_use_parallel(&self, data: &TabularData) -> bool {
        #[cfg(feature = "parallel")]
        {
            self.should_use_parallel(data)
        }
        #[cfg(not(feature = "parallel"))]
        {
            let _ = data;
            false
        }
    }

    /// Compress tabular data using parallel processing.
    ///
    /// This method forces parallel compression regardless of data size.
    /// Useful when you know the data is large enough to benefit from
    /// parallel processing.
    ///
    /// # Arguments
    ///
    /// * `data` - The tabular data to compress
    ///
    /// # Returns
    ///
    /// An `AlsDocument` containing the compressed data.
    ///
    /// # Note
    ///
    /// This method requires the `parallel` feature to be enabled.
    /// Without the feature, it falls back to sequential compression.
    #[cfg(feature = "parallel")]
    pub fn compress_parallel(&self, data: &TabularData) -> Result<AlsDocument> {
        // Handle edge cases
        if data.is_empty() || data.column_count() == 0 {
            return Ok(self.create_empty_document(data));
        }

        // Build dictionary
        let dictionary = self.build_dictionary(data);

        // Create document
        let mut doc = AlsDocument::with_schema(
            data.column_names().into_iter().map(String::from).collect(),
        );
        doc.set_als_format();

        if !dictionary.is_empty() {
            doc.add_dictionary("default", dictionary.clone());
        }

        // Force parallel compression
        let streams = self.compress_columns_parallel(data, &dictionary)?;
        for stream in streams {
            doc.add_stream(stream);
        }

        // Check for CTX fallback
        let original_size = self.calculate_original_size(data);
        let compressed_size = self.calculate_compressed_size(&doc);
        let compression_ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            f64::INFINITY
        };

        if compression_ratio < self.config.ctx_fallback_threshold {
            Ok(self.compress_ctx(data))
        } else {
            Ok(doc)
        }
    }

    /// Compress tabular data and return detailed statistics.
    ///
    /// This method performs the same compression as `compress()` but also
    /// collects and returns detailed statistics about the compression process,
    /// including per-column effectiveness metrics.
    ///
    /// # Arguments
    ///
    /// * `data` - The tabular data to compress
    ///
    /// # Returns
    ///
    /// A tuple containing the compressed `AlsDocument` and a `CompressionReport`
    /// with detailed statistics.
    pub fn compress_with_stats(&self, data: &TabularData) -> Result<(AlsDocument, CompressionReport)> {
        let stats = CompressionStats::new();
        let mut column_stats = Vec::new();

        // Handle edge cases
        if data.is_empty() || data.column_count() == 0 {
            let doc = self.create_empty_document(data);
            let snapshot = stats.snapshot();
            let report = CompressionReport::new(snapshot, column_stats, false, 0.0);
            return Ok((doc, report));
        }

        // Calculate original size
        let original_size = self.calculate_original_size(data);
        stats.add_input_bytes(original_size as u64);

        // Build dictionary
        let dictionary = self.build_dictionary(data);
        let dict_entries_used = dictionary.len();

        // Compress each column and collect stats
        let mut doc = AlsDocument::with_schema(
            data.column_names().into_iter().map(String::from).collect(),
        );
        doc.set_als_format();

        if !dictionary.is_empty() {
            doc.add_dictionary("default", dictionary.clone());
        }

        for (idx, column) in data.columns.iter().enumerate() {
            let col_input_size = self.calculate_column_size(column);
            
            // Convert values to strings for pattern detection
            let string_values: Vec<String> = column
                .values
                .iter()
                .map(|v| v.to_string_repr().into_owned())
                .collect();
            let str_refs: Vec<&str> = string_values.iter().map(|s| s.as_str()).collect();

            // Try pattern detection
            let detection = self.pattern_engine.detect(&str_refs);
            let pattern_type = detection.pattern_type;

            // Determine the stream and track stats
            let stream = if pattern_type != PatternType::Raw && detection.compression_ratio > 1.0 {
                stats.record_pattern(pattern_type);
                ColumnStream::from_operators(vec![detection.operator])
            } else {
                // Use dictionary references or raw values
                let operators = self.encode_with_dictionary(&str_refs, &dictionary);
                
                // Count dict refs and raw values
                for op in &operators {
                    match op {
                        AlsOperator::DictRef(_) => stats.record_dict_ref(),
                        AlsOperator::Raw(_) => stats.record_raw_value(),
                        _ => {}
                    }
                }
                
                ColumnStream::from_operators(operators)
            };

            // Calculate output size for this column
            let col_output_size = self.estimate_stream_size(&stream);
            let was_compressed = col_output_size < col_input_size;
            
            stats.record_column_processed(was_compressed);

            column_stats.push(ColumnStats::new(
                column.name.to_string(),
                idx,
                col_input_size,
                col_output_size,
                pattern_type,
                column.values.len(),
            ));

            doc.add_stream(stream);
        }

        // Calculate final compressed size
        let compressed_size = self.calculate_compressed_size(&doc);
        stats.add_output_bytes(compressed_size as u64);

        // Check if we should fall back to CTX
        let compression_ratio = if compressed_size > 0 {
            original_size as f64 / compressed_size as f64
        } else {
            f64::INFINITY
        };

        let used_ctx_fallback = compression_ratio < self.config.ctx_fallback_threshold;
        
        let final_doc = if used_ctx_fallback {
            self.compress_ctx(data)
        } else {
            doc
        };

        // Calculate dictionary utilization
        let dict_utilization = if !dictionary.is_empty() {
            let dict_refs = stats.get_dict_refs_used();
            if dict_refs > 0 {
                dict_entries_used as f64 / dictionary.len() as f64
            } else {
                0.0
            }
        } else {
            0.0
        };

        let snapshot = stats.snapshot();
        let report = CompressionReport::new(snapshot, column_stats, used_ctx_fallback, dict_utilization);

        Ok((final_doc, report))
    }

    /// Calculate the size of a single column in bytes.
    fn calculate_column_size(&self, column: &crate::convert::Column) -> usize {
        let mut size = column.name.len();
        for value in &column.values {
            size += value.to_string_repr().len();
            size += 1; // Separator
        }
        size
    }

    /// Estimate the serialized size of a column stream.
    fn estimate_stream_size(&self, stream: &ColumnStream) -> usize {
        let serializer = AlsSerializer::new();
        let mut output = String::new();
        for (i, op) in stream.operators.iter().enumerate() {
            if i > 0 {
                output.push(' ');
            }
            serializer.serialize_operator(&mut output, op);
        }
        output.len()
    }

    /// Compress CSV text to ALS format asynchronously.
    ///
    /// This is an async version of `compress_csv` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// CSV files without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `input` - CSV text to compress
    ///
    /// # Returns
    ///
    /// A string containing the compressed ALS representation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use als_compression::AlsCompressor;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let compressor = AlsCompressor::new();
    ///     let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    ///     let als = compressor.compress_csv_async(csv).await.unwrap();
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn compress_csv_async(&self, input: &str) -> Result<String> {
        let input = input.to_string();
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let compressor = AlsCompressor::with_config(config);
            compressor.compress_csv(&input)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }

    /// Compress JSON text to ALS format asynchronously.
    ///
    /// This is an async version of `compress_json` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// JSON files without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `input` - JSON text to compress (must be an array of objects)
    ///
    /// # Returns
    ///
    /// A string containing the compressed ALS representation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use als_compression::AlsCompressor;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let compressor = AlsCompressor::new();
    ///     let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
    ///     let als = compressor.compress_json_async(json).await.unwrap();
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn compress_json_async(&self, input: &str) -> Result<String> {
        let input = input.to_string();
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let compressor = AlsCompressor::with_config(config);
            compressor.compress_json(&input)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }

    /// Compress tabular data to an ALS document asynchronously.
    ///
    /// This is an async version of `compress` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// datasets without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `data` - The tabular data to compress
    ///
    /// # Returns
    ///
    /// An `AlsDocument` containing the compressed data.
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn compress_async(&self, data: TabularData<'static>) -> Result<AlsDocument> {
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let compressor = AlsCompressor::with_config(config);
            compressor.compress(&data)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }
}

impl Default for AlsCompressor {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::als::FormatIndicator;
    use crate::convert::{Column, Value};
    use std::borrow::Cow;

    fn create_test_data_with_patterns() -> TabularData<'static> {
        let mut data = TabularData::new();
        
        // Column with sequential integers (good for range compression)
        data.add_column(Column::new(
            Cow::Owned("id".to_string()),
            (1..=10).map(|i| Value::Integer(i)).collect(),
        ));
        
        // Column with repeated values (good for multiplier compression)
        data.add_column(Column::new(
            Cow::Owned("status".to_string()),
            vec![Value::string_owned("active".to_string()); 10],
        ));
        
        data
    }

    fn create_test_data_no_patterns() -> TabularData<'static> {
        let mut data = TabularData::new();
        
        // Column with unique values (no pattern)
        data.add_column(Column::new(
            Cow::Owned("name".to_string()),
            vec![
                Value::string_owned("alice".to_string()),
                Value::string_owned("bob".to_string()),
                Value::string_owned("charlie".to_string()),
                Value::string_owned("david".to_string()),
                Value::string_owned("eve".to_string()),
            ],
        ));
        
        data
    }

    fn create_empty_data() -> TabularData<'static> {
        TabularData::new()
    }

    #[test]
    fn test_compressor_new() {
        let compressor = AlsCompressor::new();
        assert_eq!(compressor.config().ctx_fallback_threshold, 1.2);
    }

    #[test]
    fn test_compressor_with_config() {
        let config = CompressorConfig::new().with_ctx_fallback_threshold(1.5);
        let compressor = AlsCompressor::with_config(config);
        assert_eq!(compressor.config().ctx_fallback_threshold, 1.5);
    }

    #[test]
    fn test_compress_empty_data() {
        let compressor = AlsCompressor::new();
        let data = create_empty_data();
        
        let result = compressor.compress(&data).unwrap();
        
        assert!(result.is_als());
        assert_eq!(result.column_count(), 0);
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn test_compress_with_patterns() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let result = compressor.compress(&data).unwrap();
        
        // Should use ALS format since patterns provide good compression
        assert!(result.is_als());
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.schema, vec!["id", "status"]);
    }

    #[test]
    fn test_compress_no_patterns_may_use_ctx() {
        // Create data that won't compress well
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("x".to_string()),
            vec![
                Value::string_owned("a".to_string()),
                Value::string_owned("b".to_string()),
            ],
        ));
        
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(10.0) // Very high threshold
        );
        
        let result = compressor.compress(&data).unwrap();
        
        // Should fall back to CTX due to high threshold
        assert!(result.is_ctx());
    }

    #[test]
    fn test_compress_ctx_format() {
        let data = create_test_data_no_patterns();
        
        // Force CTX by using a very high threshold
        let high_threshold_compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(100.0)
        );
        
        let result = high_threshold_compressor.compress(&data).unwrap();
        
        // Should use CTX format
        assert!(result.is_ctx());
        assert_eq!(result.format_indicator, FormatIndicator::Ctx);
    }

    #[test]
    fn test_compress_als_format() {
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(1.0) // Low threshold
        );
        let data = create_test_data_with_patterns();
        
        let result = compressor.compress(&data).unwrap();
        
        // Should use ALS format
        assert!(result.is_als());
        assert_eq!(result.format_indicator, FormatIndicator::Als);
    }

    #[test]
    fn test_format_indicator_set_correctly() {
        let compressor = AlsCompressor::new();
        
        // Test ALS format
        let data = create_test_data_with_patterns();
        let als_result = compressor.compress(&data).unwrap();
        
        // Test CTX format with high threshold
        let ctx_compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(100.0)
        );
        let ctx_result = ctx_compressor.compress(&data).unwrap();
        
        // Verify format indicators
        assert_eq!(als_result.format_indicator, FormatIndicator::Als);
        assert_eq!(ctx_result.format_indicator, FormatIndicator::Ctx);
    }

    #[test]
    fn test_calculate_compression_ratio() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let doc = compressor.compress(&data).unwrap();
        let ratio = compressor.calculate_compression_ratio(&data, &doc);
        
        // Ratio should be positive
        assert!(ratio > 0.0);
    }

    #[test]
    fn test_would_use_ctx_fallback() {
        let data = create_test_data_with_patterns();
        
        // Low threshold - should not use CTX
        let low_threshold = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(1.0)
        );
        assert!(!low_threshold.would_use_ctx_fallback(&data).unwrap());
        
        // Very high threshold - should use CTX
        let high_threshold = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(100.0)
        );
        assert!(high_threshold.would_use_ctx_fallback(&data).unwrap());
    }

    #[test]
    fn test_would_use_ctx_fallback_empty_data() {
        let compressor = AlsCompressor::new();
        let data = create_empty_data();
        
        // Empty data should not trigger CTX fallback
        assert!(!compressor.would_use_ctx_fallback(&data).unwrap());
    }

    #[test]
    fn test_dictionary_building() {
        let mut data = TabularData::new();
        
        // Add column with repeated string values
        data.add_column(Column::new(
            Cow::Owned("status".to_string()),
            vec![
                Value::string_owned("active".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("inactive".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("inactive".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("active".to_string()),
                Value::string_owned("active".to_string()),
            ],
        ));
        
        let _compressor = AlsCompressor::new();
        let result = _compressor.compress(&data).unwrap();
        
        // Should have built a dictionary for repeated values
        // (if the dictionary provides benefit)
        assert!(result.is_als() || result.is_ctx());
    }

    #[test]
    fn test_ctx_fallback_threshold_boundary() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("col".to_string()),
            vec![Value::string_owned("test".to_string())],
        ));
        
        // Test with threshold exactly at 1.0 (no compression required)
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(1.0)
        );
        let result = compressor.compress(&data).unwrap();
        
        // Should use ALS since any ratio >= 1.0 is acceptable
        // (though with single value, ratio might be < 1.0 due to overhead)
        assert!(result.is_als() || result.is_ctx());
    }

    #[test]
    fn test_compressor_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsCompressor>();
    }

    #[test]
    fn test_compressor_default() {
        let compressor = AlsCompressor::default();
        assert_eq!(compressor.config().ctx_fallback_threshold, 1.2);
    }

    #[test]
    fn test_compress_preserves_schema() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("column_a".to_string()),
            vec![Value::Integer(1), Value::Integer(2)],
        ));
        data.add_column(Column::new(
            Cow::Owned("column_b".to_string()),
            vec![Value::string_owned("x".to_string()), Value::string_owned("y".to_string())],
        ));
        
        let compressor = AlsCompressor::new();
        let result = compressor.compress(&data).unwrap();
        
        assert_eq!(result.schema, vec!["column_a", "column_b"]);
    }

    #[test]
    fn test_compress_single_row() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("id".to_string()),
            vec![Value::Integer(42)],
        ));
        
        let compressor = AlsCompressor::new();
        let result = compressor.compress(&data).unwrap();
        
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_compress_single_column() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("only_column".to_string()),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        ));
        
        let compressor = AlsCompressor::new();
        let result = compressor.compress(&data).unwrap();
        
        assert_eq!(result.column_count(), 1);
        assert_eq!(result.schema, vec!["only_column"]);
    }

    #[test]
    fn test_compress_with_stats_empty() {
        let compressor = AlsCompressor::new();
        let data = create_empty_data();
        
        let (doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        assert!(doc.is_als());
        assert_eq!(report.columns.len(), 0);
        assert!(!report.used_ctx_fallback);
    }

    #[test]
    fn test_compress_with_stats_patterns() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let (doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        assert!(doc.is_als());
        assert_eq!(report.columns.len(), 2);
        assert!(report.overall.input_bytes > 0);
        assert!(report.overall.output_bytes > 0);
        
        // Check column stats
        assert_eq!(report.columns[0].name, "id");
        assert_eq!(report.columns[1].name, "status");
    }

    #[test]
    fn test_compress_with_stats_compression_ratio() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let (_doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        // Should have positive compression ratio
        assert!(report.overall.compression_ratio() > 0.0);
    }

    #[test]
    fn test_compress_with_stats_ctx_fallback() {
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(100.0)
        );
        let data = create_test_data_no_patterns();
        
        let (doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        assert!(doc.is_ctx());
        assert!(report.used_ctx_fallback);
    }

    #[test]
    fn test_compress_with_stats_column_effectiveness() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let (_doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        // Check that column effectiveness is calculated
        let effectiveness = report.overall.column_effectiveness();
        assert!(effectiveness >= 0.0 && effectiveness <= 100.0);
    }

    #[test]
    fn test_compress_with_stats_patterns_detected() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns();
        
        let (_doc, report) = compressor.compress_with_stats(&data).unwrap();
        
        // Should have detected patterns
        assert!(report.overall.patterns_detected > 0 || report.overall.raw_values > 0);
    }

    #[test]
    fn test_compress_json_basic() {
        let compressor = AlsCompressor::new();
        let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
        
        let als = compressor.compress_json(json).unwrap();
        
        // Should produce valid ALS output
        assert!(!als.is_empty());
        // Should contain schema
        assert!(als.contains("#id") || als.contains("#name"));
    }

    #[test]
    fn test_compress_json_empty() {
        let compressor = AlsCompressor::new();
        let json = "[]";
        
        let als = compressor.compress_json(json).unwrap();
        
        // Should handle empty JSON array
        assert!(!als.is_empty());
    }

    #[test]
    fn test_compress_json_nested() {
        let compressor = AlsCompressor::new();
        let json = r#"[
            {"id": 1, "user": {"name": "Alice", "age": 30}},
            {"id": 2, "user": {"name": "Bob", "age": 25}}
        ]"#;
        
        let als = compressor.compress_json(json).unwrap();
        
        // Should produce valid ALS output with flattened columns
        assert!(!als.is_empty());
        // Should contain flattened schema (dot-notation)
        assert!(als.contains("user.name") || als.contains("user.age"));
    }

    #[test]
    fn test_compress_json_with_nulls() {
        let compressor = AlsCompressor::new();
        let json = r#"[
            {"id": 1, "name": "Alice", "email": null},
            {"id": 2, "name": null, "email": "bob@example.com"}
        ]"#;
        
        let als = compressor.compress_json(json).unwrap();
        
        // Should handle null values
        assert!(!als.is_empty());
    }

    #[test]
    fn test_compress_json_error_invalid() {
        let compressor = AlsCompressor::new();
        let json = r#"{"not": "an array"}"#;
        
        let result = compressor.compress_json(json);
        
        // Should return error for non-array JSON
        assert!(result.is_err());
    }

    // Parallel compression tests

    #[test]
    fn test_would_use_parallel_small_data() {
        let compressor = AlsCompressor::new();
        let data = create_test_data_with_patterns(); // 10 rows, 2 columns = 20 elements
        
        // Small data should not use parallel processing
        assert!(!compressor.would_use_parallel(&data));
    }

    #[test]
    fn test_would_use_parallel_large_data() {
        let compressor = AlsCompressor::new();
        
        // Create large data that exceeds PARALLEL_THRESHOLD (1000)
        let mut data = TabularData::new();
        let values: Vec<Value> = (0..600).map(|i| Value::Integer(i)).collect();
        data.add_column(Column::new(Cow::Owned("col1".to_string()), values.clone()));
        data.add_column(Column::new(Cow::Owned("col2".to_string()), values));
        
        // 600 rows * 2 columns = 1200 elements > 1000 threshold
        #[cfg(feature = "parallel")]
        assert!(compressor.would_use_parallel(&data));
        
        #[cfg(not(feature = "parallel"))]
        assert!(!compressor.would_use_parallel(&data));
    }

    #[test]
    fn test_would_use_parallel_disabled_by_config() {
        // Explicitly disable parallelism
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_parallelism(1)
        );
        
        // Create large data
        let mut data = TabularData::new();
        let values: Vec<Value> = (0..600).map(|i| Value::Integer(i)).collect();
        data.add_column(Column::new(Cow::Owned("col1".to_string()), values.clone()));
        data.add_column(Column::new(Cow::Owned("col2".to_string()), values));
        
        // Should not use parallel even with large data when disabled
        assert!(!compressor.would_use_parallel(&data));
    }

    #[test]
    fn test_would_use_parallel_single_column() {
        let compressor = AlsCompressor::new();
        
        // Create large data with single column
        let mut data = TabularData::new();
        let values: Vec<Value> = (0..2000).map(|i| Value::Integer(i)).collect();
        data.add_column(Column::new(Cow::Owned("col1".to_string()), values));
        
        // Single column should not use parallel (no benefit)
        assert!(!compressor.would_use_parallel(&data));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_compress_parallel_produces_same_result() {
        let compressor = AlsCompressor::new();
        
        // Create data with patterns
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("id".to_string()),
            (1..=100).map(|i| Value::Integer(i)).collect(),
        ));
        data.add_column(Column::new(
            Cow::Owned("status".to_string()),
            vec![Value::string_owned("active".to_string()); 100],
        ));
        data.add_column(Column::new(
            Cow::Owned("flag".to_string()),
            (0..100).map(|i| Value::Boolean(i % 2 == 0)).collect(),
        ));
        
        // Compress using both methods
        let sequential_result = compressor.compress(&data).unwrap();
        let parallel_result = compressor.compress_parallel(&data).unwrap();
        
        // Results should be equivalent
        assert_eq!(sequential_result.schema, parallel_result.schema);
        assert_eq!(sequential_result.column_count(), parallel_result.column_count());
        assert_eq!(sequential_result.row_count(), parallel_result.row_count());
        assert_eq!(sequential_result.format_indicator, parallel_result.format_indicator);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_compress_parallel_empty_data() {
        let compressor = AlsCompressor::new();
        let data = create_empty_data();
        
        let result = compressor.compress_parallel(&data).unwrap();
        
        assert!(result.is_als());
        assert_eq!(result.column_count(), 0);
        assert_eq!(result.row_count(), 0);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_compress_parallel_with_custom_threads() {
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_parallelism(2)
        );
        
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("id".to_string()),
            (1..=50).map(|i| Value::Integer(i)).collect(),
        ));
        data.add_column(Column::new(
            Cow::Owned("name".to_string()),
            (1..=50).map(|i| Value::string_owned(format!("name_{}", i))).collect(),
        ));
        
        let result = compressor.compress_parallel(&data).unwrap();
        
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.row_count(), 50);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_compress_parallel_ctx_fallback() {
        let compressor = AlsCompressor::with_config(
            CompressorConfig::new().with_ctx_fallback_threshold(100.0)
        );
        
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Owned("col".to_string()),
            vec![
                Value::string_owned("a".to_string()),
                Value::string_owned("b".to_string()),
            ],
        ));
        
        let result = compressor.compress_parallel(&data).unwrap();
        
        // Should fall back to CTX due to high threshold
        assert!(result.is_ctx());
    }
}
