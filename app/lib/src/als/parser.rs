//! ALS format parser.
//!
//! This module provides the parser for converting ALS format text into
//! `AlsDocument` structures and expanding them to tabular data.

use crate::config::ParserConfig;
use crate::error::{AlsError, Result};

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::document::{AlsDocument, ColumnStream, FormatIndicator};
use super::operator::AlsOperator;
use super::tokenizer::{Token, Tokenizer, VersionType};

/// Default threshold for parallel decompression (number of columns * estimated rows).
/// Below this threshold, sequential processing is used to avoid parallel overhead.
const PARALLEL_EXPAND_THRESHOLD: usize = 1000;

/// ALS format parser.
///
/// Parses ALS format text into `AlsDocument` structures and can expand
/// them to tabular data (CSV, JSON).
///
/// # Parallel Processing
///
/// When the `parallel` feature is enabled and the dataset is large enough
/// (determined by `PARALLEL_EXPAND_THRESHOLD`), columns are expanded in parallel
/// using Rayon's work-stealing scheduler. This provides significant speedup
/// for multi-column datasets on multi-core systems.
pub struct AlsParser {
    config: ParserConfig,
}

impl AlsParser {
    /// Current maximum supported ALS version.
    pub const MAX_SUPPORTED_VERSION: u8 = 1;

    /// Create a new parser with default configuration.
    pub fn new() -> Self {
        Self {
            config: ParserConfig::default(),
        }
    }

    /// Create a new parser with the given configuration.
    pub fn with_config(config: ParserConfig) -> Self {
        Self { config }
    }

    /// Parse ALS format text into an `AlsDocument`.
    pub fn parse(&self, input: &str) -> Result<AlsDocument> {
        let mut tokenizer = Tokenizer::new(input);
        self.parse_document(&mut tokenizer)
    }

    /// Parse a complete ALS document from the tokenizer.
    fn parse_document(&self, tokenizer: &mut Tokenizer) -> Result<AlsDocument> {
        let mut doc = AlsDocument::new();

        // Parse optional version
        self.skip_whitespace_tokens(tokenizer)?;
        if let Token::Version(version_type) = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume version
            match version_type {
                VersionType::Als(v) => {
                    if v > Self::MAX_SUPPORTED_VERSION {
                        return Err(AlsError::VersionMismatch {
                            expected: Self::MAX_SUPPORTED_VERSION,
                            found: v,
                        });
                    }
                    doc.version = v;
                    doc.format_indicator = FormatIndicator::Als;
                }
                VersionType::Ctx => {
                    doc.format_indicator = FormatIndicator::Ctx;
                }
            }
            self.skip_whitespace_tokens(tokenizer)?;
        }

        // Parse optional dictionaries
        while let Token::DictionaryHeader { name, values } = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume dictionary header
            doc.dictionaries.insert(name, values);
            self.skip_whitespace_tokens(tokenizer)?;
        }

        // Parse schema
        while let Token::SchemaColumn(name) = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume schema column
            doc.schema.push(name);
        }
        self.skip_whitespace_tokens(tokenizer)?;

        // Parse streams
        if !doc.schema.is_empty() {
            let streams = self.parse_streams(tokenizer, doc.schema.len())?;
            doc.streams = streams;
        }

        Ok(doc)
    }

    /// Skip newline tokens.
    fn skip_whitespace_tokens(&self, tokenizer: &mut Tokenizer) -> Result<()> {
        loop {
            match tokenizer.peek_token()? {
                Token::Newline => {
                    tokenizer.next_token()?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// Parse column streams separated by |.
    fn parse_streams(&self, tokenizer: &mut Tokenizer, expected_columns: usize) -> Result<Vec<ColumnStream>> {
        let mut streams = Vec::with_capacity(expected_columns);
        let mut current_stream = ColumnStream::new();

        loop {
            let token = tokenizer.next_token()?;
            
            match token {
                Token::Eof => {
                    // End of input - save current stream if not empty
                    if !current_stream.is_empty() || streams.is_empty() {
                        streams.push(current_stream);
                    }
                    break;
                }
                Token::ColumnSeparator => {
                    // Save current stream and start new one
                    streams.push(current_stream);
                    current_stream = ColumnStream::new();
                }
                Token::Newline => {
                    // Skip newlines in stream section
                    continue;
                }
                _ => {
                    // Parse an element and add to current stream
                    let operator = self.parse_element(tokenizer, token)?;
                    current_stream.push(operator);
                }
            }
        }

        // Validate column count
        if streams.len() != expected_columns && expected_columns > 0 {
            return Err(AlsError::ColumnMismatch {
                schema: expected_columns,
                data: streams.len(),
            });
        }

        Ok(streams)
    }

    /// Parse a single element (operator or value).
    fn parse_element(&self, tokenizer: &mut Tokenizer, first_token: Token) -> Result<AlsOperator> {
        match first_token {
            Token::Integer(n) => self.parse_integer_element(tokenizer, n),
            Token::Float(f) => self.parse_float_element(tokenizer, f),
            Token::RawValue(s) => self.parse_raw_element(tokenizer, s),
            Token::DictRef(idx) => Ok(AlsOperator::dict_ref(idx)),
            Token::OpenParen => self.parse_grouped_element(tokenizer),
            _ => Err(AlsError::AlsSyntaxError {
                position: tokenizer.position(),
                message: format!("Unexpected token: {:?}", first_token),
            }),
        }
    }

    /// Parse an element starting with an integer (could be range, multiply, or raw).
    fn parse_integer_element(&self, tokenizer: &mut Tokenizer, start: i64) -> Result<AlsOperator> {
        match tokenizer.peek_token()? {
            Token::RangeOp => {
                tokenizer.next_token()?; // consume >
                self.parse_range(tokenizer, start)
            }
            Token::MultiplyOp => {
                tokenizer.next_token()?; // consume *
                let count = self.expect_integer(tokenizer)?;
                Ok(AlsOperator::multiply(AlsOperator::raw(start.to_string()), count as usize))
            }
            Token::ToggleOp => {
                tokenizer.next_token()?; // consume ~
                self.parse_toggle(tokenizer, start.to_string())
            }
            _ => Ok(AlsOperator::raw(start.to_string())),
        }
    }

    /// Parse an element starting with a float.
    fn parse_float_element(&self, tokenizer: &mut Tokenizer, value: f64) -> Result<AlsOperator> {
        match tokenizer.peek_token()? {
            Token::MultiplyOp => {
                tokenizer.next_token()?; // consume *
                let count = self.expect_integer(tokenizer)?;
                Ok(AlsOperator::multiply(AlsOperator::raw(value.to_string()), count as usize))
            }
            Token::ToggleOp => {
                tokenizer.next_token()?; // consume ~
                self.parse_toggle(tokenizer, value.to_string())
            }
            _ => Ok(AlsOperator::raw(value.to_string())),
        }
    }

    /// Parse an element starting with a raw value.
    fn parse_raw_element(&self, tokenizer: &mut Tokenizer, value: String) -> Result<AlsOperator> {
        match tokenizer.peek_token()? {
            Token::MultiplyOp => {
                tokenizer.next_token()?; // consume *
                let count = self.expect_integer(tokenizer)?;
                Ok(AlsOperator::multiply(AlsOperator::raw(value), count as usize))
            }
            Token::ToggleOp => {
                tokenizer.next_token()?; // consume ~
                self.parse_toggle(tokenizer, value)
            }
            _ => Ok(AlsOperator::raw(value)),
        }
    }

    /// Parse a range expression: start>end or start>end:step
    fn parse_range(&self, tokenizer: &mut Tokenizer, start: i64) -> Result<AlsOperator> {
        let end = self.expect_integer(tokenizer)?;
        
        let step = if let Token::StepSeparator = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume :
            self.expect_integer(tokenizer)?
        } else {
            if end >= start { 1 } else { -1 }
        };

        // Check for multiply after range
        let range_op = AlsOperator::range_safe_with_limit(
            start,
            end,
            step,
            self.config.max_range_expansion,
        )?;

        if let Token::MultiplyOp = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume *
            let count = self.expect_integer(tokenizer)?;
            Ok(AlsOperator::multiply(range_op, count as usize))
        } else {
            Ok(range_op)
        }
    }

    /// Parse a toggle expression: val1~val2[~val3...]*count
    fn parse_toggle(&self, tokenizer: &mut Tokenizer, first_value: String) -> Result<AlsOperator> {
        let mut values = vec![first_value];
        
        // Parse second value
        let second = self.expect_value(tokenizer)?;
        values.push(second);

        // Parse additional toggle values
        while let Token::ToggleOp = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume ~
            let next_value = self.expect_value(tokenizer)?;
            values.push(next_value);
        }

        // Parse optional count
        let count = if let Token::MultiplyOp = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume *
            self.expect_integer(tokenizer)? as usize
        } else {
            values.len() // Default to one cycle
        };

        Ok(AlsOperator::toggle_multi(values, count))
    }

    /// Parse a grouped element: (element)
    fn parse_grouped_element(&self, tokenizer: &mut Tokenizer) -> Result<AlsOperator> {
        let inner_token = tokenizer.next_token()?;
        let inner = self.parse_element(tokenizer, inner_token)?;
        
        // Expect closing paren
        match tokenizer.next_token()? {
            Token::CloseParen => {}
            other => {
                return Err(AlsError::AlsSyntaxError {
                    position: tokenizer.position(),
                    message: format!("Expected ')' but found {:?}", other),
                });
            }
        }

        // Check for multiply after group
        if let Token::MultiplyOp = tokenizer.peek_token()? {
            tokenizer.next_token()?; // consume *
            let count = self.expect_integer(tokenizer)?;
            Ok(AlsOperator::multiply(inner, count as usize))
        } else {
            Ok(inner)
        }
    }

    /// Expect and consume an integer token.
    fn expect_integer(&self, tokenizer: &mut Tokenizer) -> Result<i64> {
        match tokenizer.next_token()? {
            Token::Integer(n) => Ok(n),
            other => Err(AlsError::AlsSyntaxError {
                position: tokenizer.position(),
                message: format!("Expected integer but found {:?}", other),
            }),
        }
    }

    /// Expect and consume a value token (integer, float, or raw).
    fn expect_value(&self, tokenizer: &mut Tokenizer) -> Result<String> {
        match tokenizer.next_token()? {
            Token::Integer(n) => Ok(n.to_string()),
            Token::Float(f) => Ok(f.to_string()),
            Token::RawValue(s) => Ok(s),
            other => Err(AlsError::AlsSyntaxError {
                position: tokenizer.position(),
                message: format!("Expected value but found {:?}", other),
            }),
        }
    }

    /// Expand an ALS document to a vector of rows.
    ///
    /// Each row is a vector of string values.
    ///
    /// When the `parallel` feature is enabled and the data is large enough,
    /// columns are expanded in parallel for better performance.
    pub fn expand(&self, doc: &AlsDocument) -> Result<Vec<Vec<String>>> {
        if doc.streams.is_empty() {
            return Ok(Vec::new());
        }

        // Get the default dictionary for resolving references
        let default_dict = doc.default_dictionary();

        // Expand all columns (parallel or sequential based on size)
        let expanded_columns = self.expand_columns_internal(doc, default_dict)?;

        // Validate all columns have the same length
        if let Some(first) = expanded_columns.first() {
            let expected_len = first.len();
            for col in expanded_columns.iter() {
                if col.len() != expected_len {
                    return Err(AlsError::ColumnMismatch {
                        schema: expected_len,
                        data: col.len(),
                    });
                }
            }
        }

        // Transpose columns to rows
        let row_count = expanded_columns.first().map(|c| c.len()).unwrap_or(0);
        let mut rows = Vec::with_capacity(row_count);
        
        for row_idx in 0..row_count {
            let row: Vec<String> = expanded_columns
                .iter()
                .map(|col| col[row_idx].clone())
                .collect();
            rows.push(row);
        }

        Ok(rows)
    }

    /// Determine if parallel processing should be used for expansion.
    fn should_use_parallel_expand(&self, doc: &AlsDocument) -> bool {
        // Check if parallelism is explicitly disabled (parallelism = 1)
        if self.config.parallelism == 1 {
            return false;
        }

        // Need at least 2 columns for parallel benefit
        if doc.streams.len() < 2 {
            return false;
        }

        // Estimate the data size based on stream complexity
        let estimated_size: usize = doc.streams.iter()
            .map(|s| s.expanded_count())
            .sum::<usize>() * doc.streams.len();

        estimated_size >= PARALLEL_EXPAND_THRESHOLD
    }

    /// Expand columns using either parallel or sequential processing.
    fn expand_columns_internal(
        &self,
        doc: &AlsDocument,
        default_dict: Option<&Vec<String>>,
    ) -> Result<Vec<Vec<String>>> {
        #[cfg(feature = "parallel")]
        {
            if self.should_use_parallel_expand(doc) {
                return self.expand_columns_parallel(doc, default_dict);
            }
        }

        // Sequential expansion
        self.expand_columns_sequential(doc, default_dict)
    }

    /// Expand columns sequentially.
    fn expand_columns_sequential(
        &self,
        doc: &AlsDocument,
        default_dict: Option<&Vec<String>>,
    ) -> Result<Vec<Vec<String>>> {
        let mut expanded_columns: Vec<Vec<String>> = Vec::with_capacity(doc.streams.len());
        for stream in &doc.streams {
            let column_values = stream.expand(default_dict.map(|v| v.as_slice()))?;
            expanded_columns.push(column_values);
        }
        Ok(expanded_columns)
    }

    /// Expand columns in parallel using Rayon.
    #[cfg(feature = "parallel")]
    fn expand_columns_parallel(
        &self,
        doc: &AlsDocument,
        default_dict: Option<&Vec<String>>,
    ) -> Result<Vec<Vec<String>>> {
        let dict_slice = default_dict.map(|v| v.as_slice());

        // Configure thread pool if parallelism is specified
        let result: Result<Vec<Vec<String>>> = if self.config.parallelism > 1 {
            // Use a custom thread pool with specified parallelism
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.parallelism)
                .build()
                .map_err(|e| AlsError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create thread pool: {}", e),
                )))?;

            pool.install(|| {
                doc.streams
                    .par_iter()
                    .map(|stream| stream.expand(dict_slice))
                    .collect()
            })
        } else {
            // Use default Rayon thread pool (auto-detect cores)
            doc.streams
                .par_iter()
                .map(|stream| stream.expand(dict_slice))
                .collect()
        };

        result
    }

    /// Check if parallel processing would be used for the given document.
    ///
    /// This is useful for testing and debugging to understand
    /// when parallel processing will be triggered.
    pub fn would_use_parallel(&self, doc: &AlsDocument) -> bool {
        #[cfg(feature = "parallel")]
        {
            self.should_use_parallel_expand(doc)
        }
        #[cfg(not(feature = "parallel"))]
        {
            let _ = doc;
            false
        }
    }

    /// Expand an ALS document using parallel processing.
    ///
    /// This method forces parallel expansion regardless of data size.
    /// Useful when you know the data is large enough to benefit from
    /// parallel processing.
    ///
    /// # Note
    ///
    /// This method requires the `parallel` feature to be enabled.
    /// Without the feature, it falls back to sequential expansion.
    #[cfg(feature = "parallel")]
    pub fn expand_parallel(&self, doc: &AlsDocument) -> Result<Vec<Vec<String>>> {
        if doc.streams.is_empty() {
            return Ok(Vec::new());
        }

        let default_dict = doc.default_dictionary();
        let expanded_columns = self.expand_columns_parallel(doc, default_dict)?;

        // Validate all columns have the same length
        if let Some(first) = expanded_columns.first() {
            let expected_len = first.len();
            for col in expanded_columns.iter() {
                if col.len() != expected_len {
                    return Err(AlsError::ColumnMismatch {
                        schema: expected_len,
                        data: col.len(),
                    });
                }
            }
        }

        // Transpose columns to rows
        let row_count = expanded_columns.first().map(|c| c.len()).unwrap_or(0);
        let mut rows = Vec::with_capacity(row_count);
        
        for row_idx in 0..row_count {
            let row: Vec<String> = expanded_columns
                .iter()
                .map(|col| col[row_idx].clone())
                .collect();
            rows.push(row);
        }

        Ok(rows)
    }

    /// Parse ALS and expand directly to rows.
    pub fn parse_and_expand(&self, input: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let doc = self.parse(input)?;
        let rows = self.expand(&doc)?;
        Ok((doc.schema.clone(), rows))
    }

    /// Parse ALS format and convert to CSV.
    ///
    /// This is a convenience method that parses ALS input, expands it to tabular data,
    /// and serializes the result to CSV format.
    ///
    /// # Arguments
    ///
    /// * `input` - ALS text to parse
    ///
    /// # Returns
    ///
    /// A string containing the CSV representation.
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::AlsParser;
    ///
    /// let parser = AlsParser::new();
    /// let als = "#id #name\n1>3|Alice Bob Charlie";
    /// let csv = parser.to_csv(als).unwrap();
    /// assert!(csv.contains("id,name"));
    /// ```
    pub fn to_csv(&self, input: &str) -> Result<String> {
        use crate::convert::csv::to_csv;
        use crate::convert::{Column, TabularData, Value};
        use std::borrow::Cow;

        // Parse ALS document
        let doc = self.parse(input)?;

        // Expand to rows
        let rows = self.expand(&doc)?;

        // Convert to TabularData
        let mut data = TabularData::with_capacity(doc.schema.len());

        if !rows.is_empty() {
            // Transpose rows to columns
            for (col_idx, col_name) in doc.schema.iter().enumerate() {
                let col_values: Vec<Value> = rows
                    .iter()
                    .map(|row| {
                        let value_str = &row[col_idx];
                        // Check for special tokens first
                        if value_str == crate::als::NULL_TOKEN {
                            Value::Null
                        } else if value_str == crate::als::EMPTY_TOKEN {
                            Value::String(Cow::Owned(String::new()))
                        } else if value_str.is_empty() {
                            // Empty string without token (shouldn't happen but handle it)
                            Value::Null
                        } else if let Ok(i) = value_str.parse::<i64>() {
                            Value::Integer(i)
                        } else if let Ok(f) = value_str.parse::<f64>() {
                            Value::Float(f)
                        } else if let Some(b) = parse_boolean_value(value_str) {
                            Value::Boolean(b)
                        } else {
                            Value::String(Cow::Owned(value_str.clone()))
                        }
                    })
                    .collect();

                data.add_column(Column::new(Cow::Owned(col_name.clone()), col_values));
            }
        } else {
            // Empty data - just add columns with no values
            for col_name in &doc.schema {
                data.add_column(Column::new(Cow::Owned(col_name.clone()), Vec::new()));
            }
        }

        // Convert to CSV
        to_csv(&data)
    }

    /// Parse ALS format and convert directly to JSON.
    ///
    /// This is a convenience method that parses ALS input, expands it to
    /// tabular data, and converts it to JSON format.
    ///
    /// # Arguments
    ///
    /// * `input` - ALS format text to parse
    ///
    /// # Returns
    ///
    /// A JSON string representation of the data.
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::AlsParser;
    ///
    /// let parser = AlsParser::new();
    /// let als = "#id #name\n1>3|Alice Bob Charlie";
    /// let json = parser.to_json(als).unwrap();
    /// assert!(json.contains("\"id\""));
    /// ```
    pub fn to_json(&self, input: &str) -> Result<String> {
        use crate::convert::json::to_json;
        use crate::convert::{Column, TabularData, Value};
        use std::borrow::Cow;

        // Parse ALS document
        let doc = self.parse(input)?;

        // Expand to rows
        let rows = self.expand(&doc)?;

        // Convert to TabularData
        let mut data = TabularData::with_capacity(doc.schema.len());

        if !rows.is_empty() {
            // Transpose rows to columns
            for (col_idx, col_name) in doc.schema.iter().enumerate() {
                let col_values: Vec<Value> = rows
                    .iter()
                    .map(|row| {
                        let value_str = &row[col_idx];
                        // Check for special tokens first
                        if value_str == crate::als::NULL_TOKEN {
                            Value::Null
                        } else if value_str == crate::als::EMPTY_TOKEN {
                            Value::String(Cow::Owned(String::new()))
                        } else if value_str.is_empty() {
                            // Empty string without token (shouldn't happen but handle it)
                            Value::Null
                        } else if let Ok(i) = value_str.parse::<i64>() {
                            Value::Integer(i)
                        } else if let Ok(f) = value_str.parse::<f64>() {
                            Value::Float(f)
                        } else if let Some(b) = parse_boolean_value(value_str) {
                            Value::Boolean(b)
                        } else {
                            Value::String(Cow::Owned(value_str.clone()))
                        }
                    })
                    .collect();

                data.add_column(Column::new(Cow::Owned(col_name.clone()), col_values));
            }
        } else {
            // Empty data - just add columns with no values
            for col_name in &doc.schema {
                data.add_column(Column::new(Cow::Owned(col_name.clone()), Vec::new()));
            }
        }

        // Convert to JSON
        to_json(&data)
    }

    /// Parse ALS format text into an `AlsDocument` asynchronously.
    ///
    /// This is an async version of `parse` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// ALS files without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `input` - ALS format text to parse
    ///
    /// # Returns
    ///
    /// An `AlsDocument` containing the parsed data.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use als_compression::AlsParser;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let parser = AlsParser::new();
    ///     let als = "#id #name\n1>3|alice bob charlie";
    ///     let doc = parser.parse_async(als).await.unwrap();
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn parse_async(&self, input: &str) -> Result<AlsDocument> {
        let input = input.to_string();
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let parser = AlsParser::with_config(config);
            parser.parse(&input)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }

    /// Parse ALS format and convert to CSV asynchronously.
    ///
    /// This is an async version of `to_csv` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// ALS files without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `input` - ALS text to parse
    ///
    /// # Returns
    ///
    /// A string containing the CSV representation.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use als_compression::AlsParser;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let parser = AlsParser::new();
    ///     let als = "#id #name\n1>3|Alice Bob Charlie";
    ///     let csv = parser.to_csv_async(als).await.unwrap();
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn to_csv_async(&self, input: &str) -> Result<String> {
        let input = input.to_string();
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let parser = AlsParser::with_config(config);
            parser.to_csv(&input)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }

    /// Parse ALS format and convert to JSON asynchronously.
    ///
    /// This is an async version of `to_json` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// ALS files without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `input` - ALS format text to parse
    ///
    /// # Returns
    ///
    /// A JSON string representation of the data.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use als_compression::AlsParser;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let parser = AlsParser::new();
    ///     let als = "#id #name\n1>3|alice bob charlie";
    ///     let json = parser.to_json_async(als).await.unwrap();
    /// }
    /// ```
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn to_json_async(&self, input: &str) -> Result<String> {
        let input = input.to_string();
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let parser = AlsParser::with_config(config);
            parser.to_json(&input)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }

    /// Expand an ALS document to a vector of rows asynchronously.
    ///
    /// This is an async version of `expand` that allows integration with
    /// async runtimes like Tokio. It's particularly useful for processing large
    /// ALS documents without blocking the async executor.
    ///
    /// # Arguments
    ///
    /// * `doc` - The ALS document to expand
    ///
    /// # Returns
    ///
    /// A vector of rows, where each row is a vector of string values.
    ///
    /// # Note
    ///
    /// This method requires the `async` feature to be enabled.
    #[cfg(feature = "async")]
    pub async fn expand_async(&self, doc: AlsDocument) -> Result<Vec<Vec<String>>> {
        let config = self.config.clone();
        
        // Spawn blocking task to avoid blocking the async executor
        tokio::task::spawn_blocking(move || {
            let parser = AlsParser::with_config(config);
            parser.expand(&doc)
        })
        .await
        .map_err(|e| AlsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Task join error: {}", e),
        )))?
    }
}

/// Parse a string as a boolean value (helper for to_csv).
fn parse_boolean_value(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "yes" | "y" | "t" => Some(true),
        "false" | "no" | "n" | "f" => Some(false),
        _ => None,
    }
}

impl Default for AlsParser {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_document() {
        let parser = AlsParser::new();
        let doc = parser.parse("").unwrap();
        assert!(doc.schema.is_empty());
        assert!(doc.streams.is_empty());
    }

    #[test]
    fn test_parse_version_als() {
        let parser = AlsParser::new();
        let doc = parser.parse("!v1\n#col\n1").unwrap();
        assert_eq!(doc.version, 1);
        assert_eq!(doc.format_indicator, FormatIndicator::Als);
    }

    #[test]
    fn test_parse_version_ctx() {
        let parser = AlsParser::new();
        let doc = parser.parse("!ctx\n#col\n1").unwrap();
        assert_eq!(doc.format_indicator, FormatIndicator::Ctx);
    }

    #[test]
    fn test_parse_unsupported_version() {
        let parser = AlsParser::new();
        let result = parser.parse("!v99\n#col\n1");
        assert!(matches!(result, Err(AlsError::VersionMismatch { .. })));
    }

    #[test]
    fn test_parse_dictionary() {
        let parser = AlsParser::new();
        let doc = parser.parse("$default:apple|banana|cherry\n#col\n_0").unwrap();
        assert!(doc.dictionaries.contains_key("default"));
        assert_eq!(doc.dictionaries["default"], vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn test_parse_schema() {
        let parser = AlsParser::new();
        let doc = parser.parse("#name #age #city\n1|2|3").unwrap();
        assert_eq!(doc.schema, vec!["name", "age", "city"]);
    }

    #[test]
    fn test_parse_raw_values() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\nhello world foo").unwrap();
        assert_eq!(doc.streams.len(), 1);
        assert_eq!(doc.streams[0].expanded_count(), 3);
    }

    #[test]
    fn test_parse_range() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n1>5").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["1", "2", "3", "4", "5"]);
    }

    #[test]
    fn test_parse_range_with_step() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n10>50:10").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["10", "20", "30", "40", "50"]);
    }

    #[test]
    fn test_parse_descending_range() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n5>1:-1").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["5", "4", "3", "2", "1"]);
    }

    #[test]
    fn test_parse_multiply() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\nhello*3").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["hello", "hello", "hello"]);
    }

    #[test]
    fn test_parse_toggle() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\nT~F*4").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["T", "F", "T", "F"]);
    }

    #[test]
    fn test_parse_dict_ref() {
        let parser = AlsParser::new();
        let doc = parser.parse("$default:red|green|blue\n#col\n_0 _1 _2").unwrap();
        let dict = doc.default_dictionary().unwrap();
        let expanded = doc.streams[0].expand(Some(dict)).unwrap();
        assert_eq!(expanded, vec!["red", "green", "blue"]);
    }

    #[test]
    fn test_parse_multiple_columns() {
        let parser = AlsParser::new();
        let doc = parser.parse("#id #name\n1>3|alice bob charlie").unwrap();
        assert_eq!(doc.streams.len(), 2);
        
        let col1 = doc.streams[0].expand(None).unwrap();
        let col2 = doc.streams[1].expand(None).unwrap();
        
        assert_eq!(col1, vec!["1", "2", "3"]);
        assert_eq!(col2, vec!["alice", "bob", "charlie"]);
    }

    #[test]
    fn test_parse_grouped_multiply() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n(1>3)*2").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["1", "2", "3", "1", "2", "3"]);
    }

    #[test]
    fn test_parse_range_multiply() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n1>3*2").unwrap();
        let expanded = doc.streams[0].expand(None).unwrap();
        assert_eq!(expanded, vec!["1", "2", "3", "1", "2", "3"]);
    }

    #[test]
    fn test_expand_to_rows() {
        let parser = AlsParser::new();
        let doc = parser.parse("#id #name\n1>3|alice bob charlie").unwrap();
        let rows = parser.expand(&doc).unwrap();
        
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "alice"]);
        assert_eq!(rows[1], vec!["2", "bob"]);
        assert_eq!(rows[2], vec!["3", "charlie"]);
    }

    #[test]
    fn test_parse_and_expand() {
        let parser = AlsParser::new();
        let (schema, rows) = parser.parse_and_expand("#id #name\n1>2|alice bob").unwrap();
        
        assert_eq!(schema, vec!["id", "name"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["1", "alice"]);
        assert_eq!(rows[1], vec!["2", "bob"]);
    }

    #[test]
    fn test_column_mismatch_error() {
        let parser = AlsParser::new();
        let result = parser.parse("#col1 #col2 #col3\n1|2");
        assert!(matches!(result, Err(AlsError::ColumnMismatch { .. })));
    }

    #[test]
    fn test_parse_complex_document() {
        let input = r#"!v1
$default:active|inactive|pending
#id #name #status
1>5|alice*2 bob*2 charlie|_0 _1 _0 _1 _2"#;
        
        let parser = AlsParser::new();
        let doc = parser.parse(input).unwrap();
        
        assert_eq!(doc.version, 1);
        assert_eq!(doc.schema, vec!["id", "name", "status"]);
        assert_eq!(doc.streams.len(), 3);
        
        let rows = parser.expand(&doc).unwrap();
        
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], vec!["1", "alice", "active"]);
        assert_eq!(rows[1], vec!["2", "alice", "inactive"]);
        assert_eq!(rows[2], vec!["3", "bob", "active"]);
        assert_eq!(rows[3], vec!["4", "bob", "inactive"]);
        assert_eq!(rows[4], vec!["5", "charlie", "pending"]);
    }

    #[test]
    fn test_parser_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsParser>();
    }

    #[test]
    fn test_version_detection_v1() {
        let parser = AlsParser::new();
        let doc = parser.parse("!v1\n#col\n1").unwrap();
        assert_eq!(doc.version, 1);
        assert!(doc.is_als());
    }

    #[test]
    fn test_version_detection_ctx() {
        let parser = AlsParser::new();
        let doc = parser.parse("!ctx\n#col\n1").unwrap();
        assert!(doc.is_ctx());
    }

    #[test]
    fn test_version_detection_no_version() {
        // When no version is specified, default to v1 ALS
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n1").unwrap();
        assert_eq!(doc.version, 1);
        assert!(doc.is_als());
    }

    #[test]
    fn test_version_future_version_error() {
        let parser = AlsParser::new();
        let result = parser.parse("!v2\n#col\n1");
        assert!(matches!(result, Err(AlsError::VersionMismatch { expected: 1, found: 2 })));
    }

    #[test]
    fn test_version_very_high_version_error() {
        let parser = AlsParser::new();
        let result = parser.parse("!v255\n#col\n1");
        assert!(matches!(result, Err(AlsError::VersionMismatch { expected: 1, found: 255 })));
    }

    #[test]
    fn test_to_json_basic() {
        let parser = AlsParser::new();
        let als = "#id #name\n1>3|alice bob charlie";
        let json = parser.to_json(als).unwrap();
        
        // Parse the JSON to verify it's valid
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array());
        
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 3);
        
        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["name"], "alice");
        assert_eq!(array[1]["id"], 2);
        assert_eq!(array[1]["name"], "bob");
        assert_eq!(array[2]["id"], 3);
        assert_eq!(array[2]["name"], "charlie");
    }

    #[test]
    fn test_to_json_empty() {
        let parser = AlsParser::new();
        let als = "";
        let json = parser.to_json(als).unwrap();
        
        assert_eq!(json, "[]");
    }

    #[test]
    fn test_to_json_with_nulls() {
        let parser = AlsParser::new();
        // Use the actual NULL_TOKEN value which is "\\0"
        let als = "#col\n\\\\0 value \\\\0";
        let json = parser.to_json(als).unwrap();
        
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let array = parsed.as_array().unwrap();
        
        assert_eq!(array.len(), 3);
        assert!(array[0]["col"].is_null());
        assert_eq!(array[1]["col"], "value");
        assert!(array[2]["col"].is_null());
    }

    #[test]
    fn test_to_json_with_types() {
        let parser = AlsParser::new();
        let als = "#int #float #bool #str\n42|3.14|true|hello";
        let json = parser.to_json(als).unwrap();
        
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let array = parsed.as_array().unwrap();
        
        assert_eq!(array.len(), 1);
        assert_eq!(array[0]["int"], 42);
        assert_eq!(array[0]["float"], 3.14);
        assert_eq!(array[0]["bool"], true);
        assert_eq!(array[0]["str"], "hello");
    }

    #[test]
    fn test_to_json_nested_reconstruction() {
        let parser = AlsParser::new();
        let als = "#id #user.name #user.age\n1|alice|30";
        let json = parser.to_json(als).unwrap();
        
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let array = parsed.as_array().unwrap();
        
        assert_eq!(array.len(), 1);
        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["user"]["name"], "alice");
        assert_eq!(array[0]["user"]["age"], 30);
    }

    #[test]
    fn test_to_json_with_dictionary() {
        let parser = AlsParser::new();
        let als = "$default:active|inactive\n#id #status\n1>2|_0 _1";
        let json = parser.to_json(als).unwrap();
        
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let array = parsed.as_array().unwrap();
        
        assert_eq!(array.len(), 2);
        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["status"], "active");
        assert_eq!(array[1]["id"], 2);
        assert_eq!(array[1]["status"], "inactive");
    }

    // Parallel decompression tests

    #[test]
    fn test_would_use_parallel_small_doc() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n1>5").unwrap();
        
        // Small data should not use parallel processing
        assert!(!parser.would_use_parallel(&doc));
    }

    #[test]
    fn test_would_use_parallel_single_column() {
        let parser = AlsParser::new();
        let doc = parser.parse("#col\n1>1000").unwrap();
        
        // Single column should not use parallel (no benefit)
        assert!(!parser.would_use_parallel(&doc));
    }

    #[test]
    fn test_would_use_parallel_disabled_by_config() {
        use crate::config::ParserConfig;
        
        // Explicitly disable parallelism
        let parser = AlsParser::with_config(
            ParserConfig::new().with_parallelism(1)
        );
        let doc = parser.parse("#col1 #col2\n1>100|1>100").unwrap();
        
        // Should not use parallel even with large data when disabled
        assert!(!parser.would_use_parallel(&doc));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_would_use_parallel_large_doc() {
        let parser = AlsParser::new();
        // Create a document with multiple columns and many rows
        // 500 * 3 = 1500 > 1000 threshold
        let doc = parser.parse("#col1 #col2 #col3\n1>500|1>500|1>500").unwrap();
        
        assert!(parser.would_use_parallel(&doc));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_expand_parallel_produces_same_result() {
        let parser = AlsParser::new();
        let als = "#id #name #status\n1>50|alice*25 bob*25|active*50";
        let doc = parser.parse(als).unwrap();
        
        // Expand using both methods
        let sequential_result = parser.expand(&doc).unwrap();
        let parallel_result = parser.expand_parallel(&doc).unwrap();
        
        // Results should be identical
        assert_eq!(sequential_result.len(), parallel_result.len());
        for (seq_row, par_row) in sequential_result.iter().zip(parallel_result.iter()) {
            assert_eq!(seq_row, par_row);
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_expand_parallel_empty_doc() {
        let parser = AlsParser::new();
        let doc = parser.parse("").unwrap();
        
        let result = parser.expand_parallel(&doc).unwrap();
        
        assert!(result.is_empty());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_expand_parallel_with_dictionary() {
        let parser = AlsParser::new();
        // Use proper ALS syntax: dictionary refs with multiplier need parentheses or separate tokens
        let als = "$default:red|green|blue\n#col1 #col2\n(_0)*10 (_1)*10 (_2)*10|1>30";
        let doc = parser.parse(als).unwrap();
        
        let result = parser.expand_parallel(&doc).unwrap();
        
        assert_eq!(result.len(), 30);
        assert_eq!(result[0][0], "red");
        assert_eq!(result[10][0], "green");
        assert_eq!(result[20][0], "blue");
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_expand_parallel_with_custom_threads() {
        use crate::config::ParserConfig;
        
        let parser = AlsParser::with_config(
            ParserConfig::new().with_parallelism(2)
        );
        let als = "#col1 #col2\n1>50|alice*50";
        let doc = parser.parse(als).unwrap();
        
        let result = parser.expand_parallel(&doc).unwrap();
        
        assert_eq!(result.len(), 50);
        assert_eq!(result[0], vec!["1", "alice"]);
        assert_eq!(result[49], vec!["50", "alice"]);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn test_expand_parallel_complex_patterns() {
        let parser = AlsParser::new();
        let als = "#id #toggle #range\n1>20|(T~F*20)|10>200:10";
        let doc = parser.parse(als).unwrap();
        
        let sequential = parser.expand(&doc).unwrap();
        let parallel = parser.expand_parallel(&doc).unwrap();
        
        // Both should produce identical results
        assert_eq!(sequential, parallel);
        assert_eq!(sequential.len(), 20);
    }
}
