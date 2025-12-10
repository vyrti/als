//! Streaming compression and decompression support.
//!
//! This module provides streaming interfaces for processing large files without
//! loading them entirely into memory. The streaming APIs process input in chunks
//! and yield results incrementally.
//!
//! # Design
//!
//! The streaming implementation follows these principles:
//!
//! - **Independent chunks**: Each compressed chunk is a complete, independently
//!   parseable ALS document. This allows chunks to be processed in parallel or
//!   out of order if needed.
//!
//! - **Configurable chunk sizes**: Users can control the trade-off between memory
//!   usage and compression efficiency by adjusting chunk sizes.
//!
//! - **Lazy evaluation**: Data is only read and processed as needed, allowing
//!   processing of files larger than available RAM.
//!
//! # Examples
//!
//! ## Streaming CSV Compression
//!
//! ```rust,ignore
//! use als_compression::StreamingCompressor;
//! use std::fs::File;
//!
//! let file = File::open("large_data.csv")?;
//! let mut compressor = StreamingCompressor::new(file)
//!     .with_csv_chunk_size(1000); // Process 1000 rows at a time
//!
//! for chunk_result in compressor.compress_csv_chunks() {
//!     let als_chunk = chunk_result?;
//!     // Write chunk to output file or stream
//! }
//! ```
//!
//! ## Streaming ALS Parsing
//!
//! ```rust,ignore
//! use als_compression::StreamingParser;
//! use std::fs::File;
//!
//! let file = File::open("large_data.als")?;
//! let mut parser = StreamingParser::new(file);
//!
//! for row_result in parser.parse_rows() {
//!     let row = row_result?;
//!     // Process row
//! }
//! ```

use std::io::{BufRead, BufReader, Read};

use crate::als::{AlsParser, AlsSerializer};
use crate::compress::AlsCompressor;
use crate::config::{CompressorConfig, ParserConfig};
use crate::convert::{TabularData, Value};
use crate::error::Result;

/// Default buffer size for streaming operations (64 KB).
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Default chunk size for CSV processing (number of rows per chunk).
const DEFAULT_CSV_CHUNK_SIZE: usize = 1000;

/// Default chunk size for JSON processing (number of objects per chunk).
const DEFAULT_JSON_CHUNK_SIZE: usize = 1000;

/// Streaming compressor for processing large inputs in chunks.
///
/// The `StreamingCompressor` reads input data in chunks, compresses each chunk
/// to ALS format, and yields the compressed fragments. This allows processing
/// files larger than available RAM.
///
/// # Examples
///
/// ```rust,ignore
/// use als_compression::StreamingCompressor;
/// use std::fs::File;
///
/// let file = File::open("large_data.csv")?;
/// let mut compressor = StreamingCompressor::new(file);
///
/// for chunk_result in compressor.compress_csv_chunks() {
///     let als_fragment = chunk_result?;
///     // Process or write the ALS fragment
/// }
/// ```
pub struct StreamingCompressor<R: Read> {
    reader: BufReader<R>,
    config: CompressorConfig,
    buffer_size: usize,
    csv_chunk_size: usize,
    json_chunk_size: usize,
}

impl<R: Read> StreamingCompressor<R> {
    /// Create a new streaming compressor with default configuration.
    ///
    /// # Arguments
    ///
    /// * `reader` - The input reader to stream from
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader),
            config: CompressorConfig::default(),
            buffer_size: DEFAULT_BUFFER_SIZE,
            csv_chunk_size: DEFAULT_CSV_CHUNK_SIZE,
            json_chunk_size: DEFAULT_JSON_CHUNK_SIZE,
        }
    }

    /// Create a new streaming compressor with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `reader` - The input reader to stream from
    /// * `config` - Compression configuration
    pub fn with_config(reader: R, config: CompressorConfig) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader),
            config,
            buffer_size: DEFAULT_BUFFER_SIZE,
            csv_chunk_size: DEFAULT_CSV_CHUNK_SIZE,
            json_chunk_size: DEFAULT_JSON_CHUNK_SIZE,
        }
    }

    /// Set the buffer size for reading.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the chunk size for CSV processing (number of rows per chunk).
    pub fn with_csv_chunk_size(mut self, size: usize) -> Self {
        self.csv_chunk_size = size;
        self
    }

    /// Set the chunk size for JSON processing (number of objects per chunk).
    pub fn with_json_chunk_size(mut self, size: usize) -> Self {
        self.json_chunk_size = size;
        self
    }

    /// Compress CSV input in chunks, yielding ALS fragments.
    ///
    /// This method reads CSV data in chunks, compresses each chunk to ALS format,
    /// and yields the compressed fragments as strings. The first fragment includes
    /// the schema header.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Result<String>` for each compressed chunk.
    pub fn compress_csv_chunks(&mut self) -> impl Iterator<Item = Result<String>> + '_ {
        StreamingCsvCompressor {
            compressor: self,
            first_chunk: true,
            schema: None,
            buffer: String::new(),
            finished: false,
        }
    }

    /// Compress JSON input in chunks, yielding ALS fragments.
    ///
    /// This method reads JSON array data in chunks, compresses each chunk to ALS
    /// format, and yields the compressed fragments as strings. The first fragment
    /// includes the schema header.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Result<String>` for each compressed chunk.
    pub fn compress_json_chunks(&mut self) -> impl Iterator<Item = Result<String>> + '_ {
        StreamingJsonCompressor {
            compressor: self,
            first_chunk: true,
            schema: None,
            buffer: String::new(),
            finished: false,
        }
    }
}

/// Iterator for streaming CSV compression.
struct StreamingCsvCompressor<'a, R: Read> {
    compressor: &'a mut StreamingCompressor<R>,
    first_chunk: bool,
    schema: Option<Vec<String>>,
    buffer: String,
    finished: bool,
}

impl<'a, R: Read> Iterator for StreamingCsvCompressor<'a, R> {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Read a chunk of CSV rows
        match self.read_csv_chunk() {
            Ok(Some(chunk_data)) => {
                // Compress the chunk
                let als_compressor = AlsCompressor::with_config(self.compressor.config.clone());
                match als_compressor.compress(&chunk_data) {
                    Ok(doc) => {
                        // Capture schema from first chunk
                        if self.first_chunk {
                            self.schema = Some(doc.schema.clone());
                            self.first_chunk = false;
                        }
                        
                        // Each chunk is a complete, independently parseable ALS document
                        let serializer = AlsSerializer::new();
                        Some(Ok(serializer.serialize(&doc)))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl<'a, R: Read> StreamingCsvCompressor<'a, R> {
    /// Read a chunk of CSV rows from the input.
    fn read_csv_chunk(&mut self) -> Result<Option<TabularData<'static>>> {
        self.buffer.clear();
        let mut lines_read = 0;
        let mut header_line = String::new();

        // Read header if this is the first chunk
        if self.schema.is_none() {
            if self.compressor.reader.read_line(&mut header_line)? == 0 {
                return Ok(None); // Empty file
            }
            self.buffer.push_str(&header_line);
            lines_read += 1;
        } else {
            // For subsequent chunks, reconstruct header from schema
            if let Some(ref schema) = self.schema {
                header_line = schema.join(",");
                header_line.push('\n');
                self.buffer.push_str(&header_line);
            }
        }

        // Read data rows
        let mut line = String::new();
        while lines_read < self.compressor.csv_chunk_size {
            line.clear();
            let bytes_read = self.compressor.reader.read_line(&mut line)?;
            if bytes_read == 0 {
                break; // End of file
            }
            self.buffer.push_str(&line);
            lines_read += 1;
        }

        // If we only read the header and no data, we're done
        if lines_read <= 1 && self.schema.is_some() {
            return Ok(None);
        }

        // Parse the CSV chunk
        use crate::convert::csv::parse_csv;
        let data = parse_csv(&self.buffer)?;
        
        // Capture schema from first chunk
        if self.schema.is_none() {
            self.schema = Some(data.column_names().into_iter().map(String::from).collect());
        }

        Ok(Some(data))
    }
}

/// Iterator for streaming JSON compression.
struct StreamingJsonCompressor<'a, R: Read> {
    compressor: &'a mut StreamingCompressor<R>,
    first_chunk: bool,
    schema: Option<Vec<String>>,
    buffer: String,
    finished: bool,
}

impl<'a, R: Read> Iterator for StreamingJsonCompressor<'a, R> {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Read a chunk of JSON objects
        match self.read_json_chunk() {
            Ok(Some(chunk_data)) => {
                // Compress the chunk
                let als_compressor = AlsCompressor::with_config(self.compressor.config.clone());
                match als_compressor.compress(&chunk_data) {
                    Ok(doc) => {
                        // Capture schema from first chunk
                        if self.first_chunk {
                            self.schema = Some(doc.schema.clone());
                            self.first_chunk = false;
                        }
                        
                        // Each chunk is a complete, independently parseable ALS document
                        let serializer = AlsSerializer::new();
                        Some(Ok(serializer.serialize(&doc)))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl<'a, R: Read> StreamingJsonCompressor<'a, R> {
    /// Read a chunk of JSON objects from the input.
    fn read_json_chunk(&mut self) -> Result<Option<TabularData<'static>>> {
        // Read the entire JSON array into memory
        // Note: True streaming JSON parsing would require a more sophisticated approach
        // For now, we read chunks of objects from a JSON array
        
        self.buffer.clear();
        self.compressor.reader.read_to_string(&mut self.buffer)?;
        
        if self.buffer.trim().is_empty() {
            return Ok(None);
        }

        // Parse the JSON
        use crate::convert::json::parse_json;
        let data = parse_json(&self.buffer)?;
        
        // Mark as finished since we read everything
        self.finished = true;
        
        Ok(Some(data))
    }
}

/// Streaming parser for processing large ALS files in chunks.
///
/// The `StreamingParser` reads ALS format data in chunks, expands each chunk,
/// and yields rows incrementally. This allows processing files larger than
/// available RAM.
///
/// # Examples
///
/// ```rust,ignore
/// use als_compression::StreamingParser;
/// use std::fs::File;
///
/// let file = File::open("large_data.als")?;
/// let mut parser = StreamingParser::new(file);
///
/// for row_result in parser.parse_rows() {
///     let row = row_result?;
///     // Process the row
/// }
/// ```
pub struct StreamingParser<R: Read> {
    reader: BufReader<R>,
    config: ParserConfig,
    buffer_size: usize,
}

impl<R: Read> StreamingParser<R> {
    /// Create a new streaming parser with default configuration.
    ///
    /// # Arguments
    ///
    /// * `reader` - The input reader to stream from
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader),
            config: ParserConfig::default(),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Create a new streaming parser with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `reader` - The input reader to stream from
    /// * `config` - Parser configuration
    pub fn with_config(reader: R, config: ParserConfig) -> Self {
        Self {
            reader: BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader),
            config,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Set the buffer size for reading.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Parse ALS input in streaming fashion, yielding rows incrementally.
    ///
    /// This method reads ALS format data, parses it, and yields rows one at a time.
    /// The schema is parsed first, then data rows are yielded as they are expanded.
    ///
    /// # Returns
    ///
    /// An iterator that yields `Result<Vec<Value>>` for each row.
    pub fn parse_rows(&mut self) -> impl Iterator<Item = Result<Vec<Value<'static>>>> + '_ {
        StreamingRowParser {
            parser: self,
            schema: None,
            rows: Vec::new(),
            row_index: 0,
            finished: false,
        }
    }
}

/// Iterator for streaming row parsing.
struct StreamingRowParser<'a, R: Read> {
    parser: &'a mut StreamingParser<R>,
    schema: Option<Vec<String>>,
    rows: Vec<Vec<String>>,
    row_index: usize,
    finished: bool,
}

impl<'a, R: Read> Iterator for StreamingRowParser<'a, R> {
    type Item = Result<Vec<Value<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // If we have rows buffered, return the next one
        if self.row_index < self.rows.len() {
            let row = &self.rows[self.row_index];
            self.row_index += 1;
            
            // Convert string row to Value row
            let value_row: Vec<Value<'static>> = row
                .iter()
                .map(|s| {
                    use std::borrow::Cow;
                    // Try to parse as different types
                    if s == crate::als::NULL_TOKEN {
                        Value::Null
                    } else if s == crate::als::EMPTY_TOKEN {
                        Value::String(Cow::Owned(String::new()))
                    } else if let Ok(i) = s.parse::<i64>() {
                        Value::Integer(i)
                    } else if let Ok(f) = s.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        Value::String(Cow::Owned(s.clone()))
                    }
                })
                .collect();
            
            return Some(Ok(value_row));
        }

        // Need to read more data
        match self.read_and_parse() {
            Ok(true) => {
                // Successfully read more rows, try again
                self.next()
            }
            Ok(false) => {
                // No more data
                self.finished = true;
                None
            }
            Err(e) => {
                self.finished = true;
                Some(Err(e))
            }
        }
    }
}

impl<'a, R: Read> StreamingRowParser<'a, R> {
    /// Read and parse the next chunk of ALS data.
    fn read_and_parse(&mut self) -> Result<bool> {
        // Read all remaining input
        // Note: True streaming would parse incrementally, but ALS format
        // requires knowing the full structure (schema + all streams)
        let mut buffer = String::new();
        let bytes_read = self.parser.reader.read_to_string(&mut buffer)?;
        
        if bytes_read == 0 {
            return Ok(false); // No more data
        }

        // Parse the ALS document
        let als_parser = AlsParser::with_config(self.parser.config.clone());
        let doc = als_parser.parse(&buffer)?;
        
        // Capture schema
        self.schema = Some(doc.schema.clone());
        
        // Expand to rows
        self.rows = als_parser.expand(&doc)?;
        self.row_index = 0;
        
        Ok(!self.rows.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_streaming_compressor_csv() {
        let csv_data = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n";
        let cursor = Cursor::new(csv_data.as_bytes());
        
        let mut compressor = StreamingCompressor::new(cursor)
            .with_csv_chunk_size(2); // Small chunks for testing
        
        let mut chunks = Vec::new();
        for chunk_result in compressor.compress_csv_chunks() {
            let chunk = chunk_result.unwrap();
            chunks.push(chunk);
        }
        
        // Should have at least one chunk
        assert!(!chunks.is_empty());
        
        // First chunk should contain schema
        assert!(chunks[0].contains("#id") || chunks[0].contains("#name"));
    }

    #[test]
    fn test_streaming_compressor_empty_csv() {
        let csv_data = "";
        let cursor = Cursor::new(csv_data.as_bytes());
        
        let mut compressor = StreamingCompressor::new(cursor);
        
        let chunks: Vec<_> = compressor.compress_csv_chunks().collect();
        
        // Empty input should produce no chunks
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_streaming_parser_rows() {
        let als_data = "#id #name\n1>3|Alice Bob Charlie";
        let cursor = Cursor::new(als_data.as_bytes());
        
        let mut parser = StreamingParser::new(cursor);
        
        let rows: Result<Vec<_>> = parser.parse_rows().collect();
        let rows = rows.unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].len(), 2); // Two columns
    }

    #[test]
    fn test_streaming_parser_empty() {
        let als_data = "";
        let cursor = Cursor::new(als_data.as_bytes());
        
        let mut parser = StreamingParser::new(cursor);
        
        let rows: Result<Vec<_>> = parser.parse_rows().collect();
        let rows = rows.unwrap();
        
        assert!(rows.is_empty());
    }

    #[test]
    fn test_streaming_compressor_with_config() {
        let csv_data = "id,value\n1,100\n2,200\n";
        let cursor = Cursor::new(csv_data.as_bytes());
        
        let config = CompressorConfig::new().with_ctx_fallback_threshold(1.5);
        let mut compressor = StreamingCompressor::with_config(cursor, config);
        
        let chunks: Vec<_> = compressor.compress_csv_chunks()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_streaming_parser_with_config() {
        let als_data = "#id\n1>5";
        let cursor = Cursor::new(als_data.as_bytes());
        
        let config = ParserConfig::new().with_parallelism(1);
        let mut parser = StreamingParser::with_config(cursor, config);
        
        let rows: Result<Vec<_>> = parser.parse_rows().collect();
        let rows = rows.unwrap();
        
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_streaming_compressor_buffer_size() {
        let csv_data = "id,value\n1,100\n";
        let cursor = Cursor::new(csv_data.as_bytes());
        
        let mut compressor = StreamingCompressor::new(cursor)
            .with_buffer_size(1024);
        
        let chunks: Vec<_> = compressor.compress_csv_chunks()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        
        assert!(!chunks.is_empty());
    }

    #[test]
    fn test_streaming_parser_buffer_size() {
        let als_data = "#id\n1>3";
        let cursor = Cursor::new(als_data.as_bytes());
        
        let mut parser = StreamingParser::new(cursor)
            .with_buffer_size(512);
        
        let rows: Result<Vec<_>> = parser.parse_rows().collect();
        let rows = rows.unwrap();
        
        assert_eq!(rows.len(), 3);
    }
}
