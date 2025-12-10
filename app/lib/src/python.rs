//! Python bindings for the ALS compression library using PyO3.
//!
//! This module provides Python-friendly wrappers around the core ALS compression
//! functionality, allowing Python developers to use the library in data science
//! and ML workflows.
//!
//! # Features
//!
//! - **PyAlsCompressor**: Compress CSV and JSON data to ALS format
//! - **PyAlsParser**: Parse ALS format back to CSV or JSON
//! - **Error handling**: Python exceptions with descriptive messages
//! - **Type conversion**: Automatic conversion between Rust and Python types
//!
//! # Example Usage (Python)
//!
//! ```python
//! from als_compression import AlsCompressor, AlsParser
//!
//! # Compression
//! compressor = AlsCompressor()
//! als = compressor.compress_csv("id,name\n1,Alice\n2,Bob")
//! print(f"Compressed: {als}")
//!
//! # Decompression
//! parser = AlsParser()
//! csv = parser.to_csv(als)
//! print(f"CSV: {csv}")
//! ```

use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::PyAny;
use crate::{AlsCompressor as RustAlsCompressor, AlsParser as RustAlsParser, AlsError, CompressorConfig, ParserConfig};

/// Python wrapper for AlsCompressor.
///
/// This class provides methods to compress CSV and JSON data to ALS format.
///
/// # Examples (Python)
///
/// ```python
/// from als_compression import AlsCompressor
///
/// compressor = AlsCompressor()
///
/// # Compress CSV
/// csv_data = "id,name\n1,Alice\n2,Bob\n3,Charlie"
/// als = compressor.compress_csv(csv_data)
///
/// # Compress JSON
/// json_data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
/// als = compressor.compress_json(json_data)
/// ```
#[pyclass(name = "AlsCompressor")]
pub struct PyAlsCompressor {
    inner: RustAlsCompressor,
}

#[pymethods]
impl PyAlsCompressor {
    /// Create a new AlsCompressor with default configuration.
    ///
    /// Returns:
    ///     AlsCompressor: A new compressor instance
    ///
    /// Example:
    ///     >>> compressor = AlsCompressor()
    #[new]
    fn new() -> Self {
        PyAlsCompressor {
            inner: RustAlsCompressor::new(),
        }
    }

    /// Create a new AlsCompressor with custom configuration.
    ///
    /// Args:
    ///     ctx_fallback_threshold (float, optional): Minimum compression ratio before
    ///         falling back to CTX format. Default: 1.2
    ///     min_pattern_length (int, optional): Minimum pattern length to consider.
    ///         Default: 3
    ///     parallelism (int, optional): Number of threads for parallel processing.
    ///         0 means auto-detect. Default: 0
    ///
    /// Returns:
    ///     AlsCompressor: A new compressor instance with custom configuration
    ///
    /// Example:
    ///     >>> compressor = AlsCompressor.with_config(
    ///     ...     ctx_fallback_threshold=1.5,
    ///     ...     min_pattern_length=4,
    ///     ...     parallelism=4
    ///     ... )
    #[staticmethod]
    #[pyo3(signature = (ctx_fallback_threshold=None, min_pattern_length=None, parallelism=None))]
    fn with_config(
        ctx_fallback_threshold: Option<f64>,
        min_pattern_length: Option<usize>,
        parallelism: Option<usize>,
    ) -> Self {
        let mut config = CompressorConfig::default();
        
        if let Some(threshold) = ctx_fallback_threshold {
            config = config.with_ctx_fallback_threshold(threshold);
        }
        if let Some(length) = min_pattern_length {
            config = config.with_min_pattern_length(length);
        }
        if let Some(threads) = parallelism {
            config = config.with_parallelism(threads);
        }
        
        PyAlsCompressor {
            inner: RustAlsCompressor::with_config(config),
        }
    }

    /// Compress CSV data to ALS format.
    ///
    /// Args:
    ///     csv_data (str): CSV data as a string
    ///
    /// Returns:
    ///     str: Compressed data in ALS format
    ///
    /// Raises:
    ///     ValueError: If the CSV data is malformed
    ///     RuntimeError: If compression fails
    ///
    /// Example:
    ///     >>> compressor = AlsCompressor()
    ///     >>> csv = "id,name\\n1,Alice\\n2,Bob\\n3,Charlie"
    ///     >>> als = compressor.compress_csv(csv)
    ///     >>> print(als)
    ///     #id #name
    ///     1>3|Alice Bob Charlie
    fn compress_csv(&self, csv_data: &str) -> PyResult<String> {
        self.inner
            .compress_csv(csv_data)
            .map_err(convert_als_error)
    }

    /// Compress JSON data to ALS format.
    ///
    /// The JSON data should be an array of objects with consistent keys.
    ///
    /// Args:
    ///     json_data (str): JSON data as a string (array of objects)
    ///
    /// Returns:
    ///     str: Compressed data in ALS format
    ///
    /// Raises:
    ///     ValueError: If the JSON data is malformed
    ///     RuntimeError: If compression fails
    ///
    /// Example:
    ///     >>> compressor = AlsCompressor()
    ///     >>> json = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
    ///     >>> als = compressor.compress_json(json)
    fn compress_json(&self, json_data: &str) -> PyResult<String> {
        self.inner
            .compress_json(json_data)
            .map_err(convert_als_error)
    }

    /// Get a string representation of the compressor.
    fn __repr__(&self) -> String {
        "AlsCompressor()".to_string()
    }

    /// Get a string representation of the compressor.
    fn __str__(&self) -> String {
        "ALS Compressor".to_string()
    }

    /// Compress a pandas DataFrame to ALS format.
    ///
    /// This method accepts a pandas DataFrame and converts it to ALS format.
    /// The DataFrame is first converted to CSV internally, then compressed.
    ///
    /// Args:
    ///     dataframe: A pandas DataFrame object
    ///
    /// Returns:
    ///     str: Compressed data in ALS format
    ///
    /// Raises:
    ///     ValueError: If the DataFrame cannot be converted
    ///     RuntimeError: If compression fails
    ///
    /// Example:
    ///     >>> import pandas as pd
    ///     >>> from als_compression import AlsCompressor
    ///     >>> df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    ///     >>> compressor = AlsCompressor()
    ///     >>> als = compressor.compress_dataframe(df)
    fn compress_dataframe(&self, py: Python, dataframe: &Bound<'_, PyAny>) -> PyResult<String> {
        // Import pandas to check if the object is a DataFrame
        let pandas = py.import("pandas")?;
        let dataframe_class = pandas.getattr("DataFrame")?;
        
        // Check if the input is a DataFrame
        if !dataframe.is_instance(&dataframe_class)? {
            return Err(PyValueError::new_err(
                "Input must be a pandas DataFrame"
            ));
        }
        
        // Convert DataFrame to CSV string
        // Call df.to_csv(index=False) to get CSV without row indices
        let to_csv = dataframe.getattr("to_csv")?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("index", false)?;
        let csv_data: String = to_csv.call((), Some(&kwargs))?.extract()?;
        
        // Compress the CSV data
        self.inner
            .compress_csv(&csv_data)
            .map_err(convert_als_error)
    }

    /// Compress a numpy array to ALS format.
    ///
    /// This method accepts a 2D numpy array and converts it to ALS format.
    /// The array is treated as a table where each column is compressed independently.
    ///
    /// Args:
    ///     array: A 2D numpy array
    ///     column_names (list[str], optional): Column names for the array.
    ///         If not provided, columns will be named col0, col1, etc.
    ///
    /// Returns:
    ///     str: Compressed data in ALS format
    ///
    /// Raises:
    ///     ValueError: If the array is not 2D or cannot be converted
    ///     RuntimeError: If compression fails
    ///
    /// Example:
    ///     >>> import numpy as np
    ///     >>> from als_compression import AlsCompressor
    ///     >>> arr = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    ///     >>> compressor = AlsCompressor()
    ///     >>> als = compressor.compress_array(arr, column_names=['a', 'b', 'c'])
    #[pyo3(signature = (array, column_names=None))]
    fn compress_array(
        &self,
        py: Python,
        array: &Bound<'_, PyAny>,
        column_names: Option<Vec<String>>,
    ) -> PyResult<String> {
        // Import numpy
        let numpy = py.import("numpy")?;
        let ndarray_class = numpy.getattr("ndarray")?;
        
        // Check if the input is a numpy array
        if !array.is_instance(&ndarray_class)? {
            return Err(PyValueError::new_err(
                "Input must be a numpy array"
            ));
        }
        
        // Check if the array is 2D
        let shape: Vec<usize> = array.getattr("shape")?.extract()?;
        if shape.len() != 2 {
            return Err(PyValueError::new_err(
                "Array must be 2-dimensional"
            ));
        }
        
        let num_rows = shape[0];
        let num_cols = shape[1];
        
        // Generate column names if not provided
        let col_names = if let Some(names) = column_names {
            if names.len() != num_cols {
                return Err(PyValueError::new_err(format!(
                    "Number of column names ({}) must match number of columns ({})",
                    names.len(),
                    num_cols
                )));
            }
            names
        } else {
            (0..num_cols).map(|i| format!("col{}", i)).collect()
        };
        
        // Convert array to CSV format
        let mut csv_data = col_names.join(",") + "\n";
        
        // Iterate through rows
        for row_idx in 0..num_rows {
            let row = array.call_method1("__getitem__", (row_idx,))?;
            let mut row_values = Vec::new();
            
            for col_idx in 0..num_cols {
                let value = row.call_method1("__getitem__", (col_idx,))?;
                let value_str: String = value.str()?.extract()?;
                row_values.push(value_str);
            }
            
            csv_data.push_str(&row_values.join(","));
            if row_idx < num_rows - 1 {
                csv_data.push('\n');
            }
        }
        
        // Compress the CSV data
        self.inner
            .compress_csv(&csv_data)
            .map_err(convert_als_error)
    }
}

/// Python wrapper for AlsParser.
///
/// This class provides methods to parse ALS format and convert it back to
/// CSV or JSON.
///
/// # Examples (Python)
///
/// ```python
/// from als_compression import AlsParser
///
/// parser = AlsParser()
///
/// # Parse to CSV
/// als = "#id #name\n1>3|Alice Bob Charlie"
/// csv = parser.to_csv(als)
///
/// # Parse to JSON
/// json = parser.to_json(als)
/// ```
#[pyclass(name = "AlsParser")]
pub struct PyAlsParser {
    inner: RustAlsParser,
}

#[pymethods]
impl PyAlsParser {
    /// Create a new AlsParser with default configuration.
    ///
    /// Returns:
    ///     AlsParser: A new parser instance
    ///
    /// Example:
    ///     >>> parser = AlsParser()
    #[new]
    fn new() -> Self {
        PyAlsParser {
            inner: RustAlsParser::new(),
        }
    }

    /// Create a new AlsParser with custom configuration.
    ///
    /// Args:
    ///     parallelism (int, optional): Number of threads for parallel processing.
    ///         0 means auto-detect. Default: 0
    ///
    /// Returns:
    ///     AlsParser: A new parser instance with custom configuration
    ///
    /// Example:
    ///     >>> parser = AlsParser.with_config(parallelism=4)
    #[staticmethod]
    #[pyo3(signature = (parallelism=None))]
    fn with_config(parallelism: Option<usize>) -> Self {
        let mut config = ParserConfig::default();
        
        if let Some(threads) = parallelism {
            config = config.with_parallelism(threads);
        }
        
        PyAlsParser {
            inner: RustAlsParser::with_config(config),
        }
    }

    /// Parse ALS format and convert to CSV.
    ///
    /// Args:
    ///     als_data (str): ALS format data as a string
    ///
    /// Returns:
    ///     str: CSV data
    ///
    /// Raises:
    ///     ValueError: If the ALS data is malformed
    ///     RuntimeError: If parsing fails
    ///
    /// Example:
    ///     >>> parser = AlsParser()
    ///     >>> als = "#id #name\\n1>3|Alice Bob Charlie"
    ///     >>> csv = parser.to_csv(als)
    ///     >>> print(csv)
    ///     id,name
    ///     1,Alice
    ///     2,Bob
    ///     3,Charlie
    fn to_csv(&self, als_data: &str) -> PyResult<String> {
        self.inner
            .to_csv(als_data)
            .map_err(convert_als_error)
    }

    /// Parse ALS format and convert to JSON.
    ///
    /// Args:
    ///     als_data (str): ALS format data as a string
    ///
    /// Returns:
    ///     str: JSON data (array of objects)
    ///
    /// Raises:
    ///     ValueError: If the ALS data is malformed
    ///     RuntimeError: If parsing fails
    ///
    /// Example:
    ///     >>> parser = AlsParser()
    ///     >>> als = "#id #name\\n1>3|Alice Bob Charlie"
    ///     >>> json = parser.to_json(als)
    ///     >>> print(json)
    ///     [{"id":1,"name":"Alice"},{"id":2,"name":"Bob"},{"id":3,"name":"Charlie"}]
    fn to_json(&self, als_data: &str) -> PyResult<String> {
        self.inner
            .to_json(als_data)
            .map_err(convert_als_error)
    }

    /// Get a string representation of the parser.
    fn __repr__(&self) -> String {
        "AlsParser()".to_string()
    }

    /// Get a string representation of the parser.
    fn __str__(&self) -> String {
        "ALS Parser".to_string()
    }

    /// Parse ALS format and convert to a pandas DataFrame.
    ///
    /// This method parses ALS format data and returns a pandas DataFrame.
    /// The ALS data is first converted to CSV, then loaded into a DataFrame.
    ///
    /// Args:
    ///     als_data (str): ALS format data as a string
    ///
    /// Returns:
    ///     DataFrame: A pandas DataFrame
    ///
    /// Raises:
    ///     ValueError: If the ALS data is malformed
    ///     RuntimeError: If parsing fails
    ///
    /// Example:
    ///     >>> from als_compression import AlsParser
    ///     >>> parser = AlsParser()
    ///     >>> als = "#id #name\\n1>3|Alice Bob Charlie"
    ///     >>> df = parser.to_dataframe(als)
    ///     >>> print(df)
    ///        id     name
    ///     0   1    Alice
    ///     1   2      Bob
    ///     2   3  Charlie
    fn to_dataframe<'py>(&self, py: Python<'py>, als_data: &str) -> PyResult<Bound<'py, PyAny>> {
        // Parse ALS to CSV
        let csv_data = self.inner
            .to_csv(als_data)
            .map_err(convert_als_error)?;
        
        // Import pandas
        let pandas = py.import("pandas")?;
        
        // Import io.StringIO for reading CSV from string
        let io = py.import("io")?;
        let string_io = io.getattr("StringIO")?;
        let csv_buffer = string_io.call1((csv_data,))?;
        
        // Call pd.read_csv(csv_buffer)
        let read_csv = pandas.getattr("read_csv")?;
        let dataframe = read_csv.call1((csv_buffer,))?;
        
        Ok(dataframe)
    }

    /// Parse ALS format and convert to a numpy array.
    ///
    /// This method parses ALS format data and returns a 2D numpy array.
    /// Column names are discarded in the conversion.
    ///
    /// Args:
    ///     als_data (str): ALS format data as a string
    ///
    /// Returns:
    ///     ndarray: A 2D numpy array
    ///
    /// Raises:
    ///     ValueError: If the ALS data is malformed
    ///     RuntimeError: If parsing fails
    ///
    /// Example:
    ///     >>> from als_compression import AlsParser
    ///     >>> parser = AlsParser()
    ///     >>> als = "#id #value\\n1>3|10 20 30"
    ///     >>> arr = parser.to_array(als)
    ///     >>> print(arr)
    ///     [[ 1 10]
    ///      [ 2 20]
    ///      [ 3 30]]
    fn to_array<'py>(&self, py: Python<'py>, als_data: &str) -> PyResult<Bound<'py, PyAny>> {
        // Parse ALS to CSV
        let csv_data = self.inner
            .to_csv(als_data)
            .map_err(convert_als_error)?;
        
        // Import pandas to parse CSV
        let pandas = py.import("pandas")?;
        let io = py.import("io")?;
        let string_io = io.getattr("StringIO")?;
        let csv_buffer = string_io.call1((csv_data,))?;
        
        // Read CSV into DataFrame
        let read_csv = pandas.getattr("read_csv")?;
        let dataframe = read_csv.call1((csv_buffer,))?;
        
        // Convert DataFrame to numpy array using .values
        let values = dataframe.getattr("values")?;
        
        Ok(values)
    }
}

/// Convert AlsError to Python exception.
fn convert_als_error(error: AlsError) -> PyErr {
    match error {
        AlsError::CsvParseError { line, column, message } => {
            PyValueError::new_err(format!(
                "CSV parsing error at line {}, column {}: {}",
                line, column, message
            ))
        }
        AlsError::JsonParseError(e) => {
            PyValueError::new_err(format!("JSON parsing error: {}", e))
        }
        AlsError::AlsSyntaxError { position, message } => {
            PyValueError::new_err(format!(
                "ALS syntax error at position {}: {}",
                position, message
            ))
        }
        AlsError::InvalidDictRef { index, size } => {
            PyValueError::new_err(format!(
                "Invalid dictionary reference: _{} (dictionary has {} entries)",
                index, size
            ))
        }
        AlsError::RangeOverflow { start, end, step } => {
            PyValueError::new_err(format!(
                "Range overflow: {} to {} with step {} would produce too many values",
                start, end, step
            ))
        }
        AlsError::VersionMismatch { expected, found } => {
            PyValueError::new_err(format!(
                "Version mismatch: expected <= {}, found {}",
                expected, found
            ))
        }
        AlsError::ColumnMismatch { schema, data } => {
            PyValueError::new_err(format!(
                "Column count mismatch: schema has {} columns, data has {} columns",
                schema, data
            ))
        }
        AlsError::IoError(e) => {
            PyRuntimeError::new_err(format!("IO error: {}", e))
        }
    }
}

/// Python module definition.
///
/// This function is called by PyO3 to initialize the Python module.
#[pymodule]
fn als_compression(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAlsCompressor>()?;
    m.add_class::<PyAlsParser>()?;
    
    // Add module-level documentation
    m.add("__doc__", "ALS (Adaptive Logic Stream) compression library for structured data.\n\n\
        This library provides high-performance compression for CSV and JSON data using\n\
        algorithmic pattern description rather than raw enumeration.\n\n\
        Classes:\n\
        - AlsCompressor: Compress CSV and JSON data to ALS format\n\
        - AlsParser: Parse ALS format back to CSV or JSON\n\n\
        Example:\n\
            >>> from als_compression import AlsCompressor, AlsParser\n\
            >>> compressor = AlsCompressor()\n\
            >>> als = compressor.compress_csv('id,name\\\\n1,Alice\\\\n2,Bob')\n\
            >>> parser = AlsParser()\n\
            >>> csv = parser.to_csv(als)\n")?;
    
    Ok(())
}
