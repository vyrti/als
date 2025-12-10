//! Error types for the ALS compression library.
//!
//! This module defines all error types that can occur during compression,
//! decompression, parsing, and serialization operations.

use thiserror::Error;

/// Main error type for the ALS compression library.
///
/// All operations that can fail return `Result<T, AlsError>`.
#[derive(Debug, Error)]
pub enum AlsError {
    /// Error parsing CSV input.
    ///
    /// Contains the line and column where the error occurred, along with
    /// a descriptive message.
    #[error("CSV parsing error at line {line}, column {column}: {message}")]
    CsvParseError {
        /// Line number where the error occurred (1-indexed)
        line: usize,
        /// Column number where the error occurred (1-indexed)
        column: usize,
        /// Description of the parsing error
        message: String,
    },

    /// Error parsing log file input.
    ///
    /// Contains the line number and a descriptive message.
    #[error("Log parsing error at line {line}: {message}")]
    LogParseError {
        /// Line number where the error occurred (1-indexed)
        line: usize,
        /// Description of the parsing error
        message: String,
    },

    /// Error parsing JSON input.
    ///
    /// Wraps errors from the `serde_json` crate.
    #[error("JSON parsing error: {0}")]
    JsonParseError(#[from] serde_json::Error),

    /// Error parsing ALS syntax.
    ///
    /// Contains the position in the input where the error occurred.
    #[error("ALS syntax error at position {position}: {message}")]
    AlsSyntaxError {
        /// Byte position in the input where the error occurred
        position: usize,
        /// Description of the syntax error
        message: String,
    },

    /// Invalid dictionary reference.
    ///
    /// Occurs when an ALS document references a dictionary index that
    /// doesn't exist.
    #[error("Invalid dictionary reference: _{index} (dictionary has {size} entries)")]
    InvalidDictRef {
        /// The invalid dictionary index that was referenced
        index: usize,
        /// The actual size of the dictionary
        size: usize,
    },

    /// Range expansion would overflow.
    ///
    /// Occurs when a range operator would produce too many values,
    /// potentially causing memory exhaustion.
    #[error("Range overflow: {start} to {end} with step {step} would produce too many values")]
    RangeOverflow {
        /// Start value of the range
        start: i64,
        /// End value of the range
        end: i64,
        /// Step value of the range
        step: i64,
    },

    /// Version mismatch between parser and ALS document.
    ///
    /// Occurs when attempting to parse an ALS document with a version
    /// that is not supported by this parser.
    #[error("Version mismatch: expected <= {expected}, found {found}")]
    VersionMismatch {
        /// Maximum version supported by this parser
        expected: u8,
        /// Version found in the ALS document
        found: u8,
    },

    /// Column count mismatch.
    ///
    /// Occurs when the number of columns in the schema doesn't match
    /// the number of data streams.
    #[error("Column count mismatch: schema has {schema} columns, data has {data} columns")]
    ColumnMismatch {
        /// Number of columns defined in the schema
        schema: usize,
        /// Number of data columns found
        data: usize,
    },

    /// I/O error.
    ///
    /// Wraps errors from standard I/O operations.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Type alias for Results using `AlsError`.
pub type Result<T> = std::result::Result<T, AlsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_parse_error_display() {
        let error = AlsError::CsvParseError {
            line: 5,
            column: 10,
            message: "unexpected character".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("line 5"));
        assert!(display.contains("column 10"));
        assert!(display.contains("unexpected character"));
    }

    #[test]
    fn test_als_syntax_error_display() {
        let error = AlsError::AlsSyntaxError {
            position: 42,
            message: "expected '>' but found '*'".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("position 42"));
        assert!(display.contains("expected '>' but found '*'"));
    }

    #[test]
    fn test_invalid_dict_ref_display() {
        let error = AlsError::InvalidDictRef {
            index: 5,
            size: 3,
        };
        let display = format!("{}", error);
        assert!(display.contains("_5"));
        assert!(display.contains("3 entries"));
    }

    #[test]
    fn test_range_overflow_display() {
        let error = AlsError::RangeOverflow {
            start: 1,
            end: 1_000_000_000,
            step: 1,
        };
        let display = format!("{}", error);
        assert!(display.contains("1 to 1000000000"));
        assert!(display.contains("step 1"));
    }

    #[test]
    fn test_version_mismatch_display() {
        let error = AlsError::VersionMismatch {
            expected: 1,
            found: 2,
        };
        let display = format!("{}", error);
        assert!(display.contains("expected <= 1"));
        assert!(display.contains("found 2"));
    }

    #[test]
    fn test_column_mismatch_display() {
        let error = AlsError::ColumnMismatch {
            schema: 3,
            data: 5,
        };
        let display = format!("{}", error);
        assert!(display.contains("schema has 3"));
        assert!(display.contains("data has 5"));
    }

    #[test]
    fn test_json_parse_error_from() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json")
            .unwrap_err();
        let als_error: AlsError = json_error.into();
        assert!(matches!(als_error, AlsError::JsonParseError(_)));
    }

    #[test]
    fn test_io_error_from() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let als_error: AlsError = io_error.into();
        assert!(matches!(als_error, AlsError::IoError(_)));
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsError>();
    }
}
