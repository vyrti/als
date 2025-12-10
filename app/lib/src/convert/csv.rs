//! CSV parsing and writing.
//!
//! This module provides functions for converting between CSV format and
//! `TabularData` structures.

use crate::convert::{Column, TabularData, Value};
use crate::error::{AlsError, Result};
use std::borrow::Cow;

/// Parse CSV text into `TabularData`.
///
/// This function parses CSV input and infers column types from the data.
/// It handles edge cases like empty input, single row, and single column.
///
/// # Arguments
///
/// * `input` - CSV text to parse
///
/// # Returns
///
/// A `TabularData` structure containing the parsed data.
///
/// # Examples
///
/// ```
/// use als_compression::convert::csv::parse_csv;
///
/// let csv = "id,name\n1,Alice\n2,Bob";
/// let data = parse_csv(csv).unwrap();
/// assert_eq!(data.column_count(), 2);
/// assert_eq!(data.row_count, 2);
/// ```
pub fn parse_csv(input: &str) -> Result<TabularData<'static>> {
    // Handle empty input
    if input.trim().is_empty() {
        return Ok(TabularData::new());
    }

    // Use csv crate to parse
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // Require consistent column count
        .from_reader(input.as_bytes());

    // Get headers
    let headers = reader.headers().map_err(|e| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: format!("Failed to read headers: {}", e),
    })?;

    let column_count = headers.len();
    
    // Handle single column edge case
    if column_count == 0 {
        return Ok(TabularData::new());
    }

    // Initialize columns with headers
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); column_count];
    let column_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    // Read all records
    for (line_num, result) in reader.records().enumerate() {
        let record = result.map_err(|e| AlsError::CsvParseError {
            line: line_num + 2, // +2 because line 1 is headers, and enumerate starts at 0
            column: 0,
            message: format!("Failed to parse record: {}", e),
        })?;

        // Validate column count
        if record.len() != column_count {
            return Err(AlsError::CsvParseError {
                line: line_num + 2,
                column: record.len(),
                message: format!(
                    "Column count mismatch: expected {}, found {}",
                    column_count,
                    record.len()
                ),
            });
        }

        // Add values to columns
        for (col_idx, field) in record.iter().enumerate() {
            columns[col_idx].push(field.to_string());
        }
    }

    // Handle single row edge case - still valid
    // Convert to TabularData with type inference
    let mut data = TabularData::with_capacity(column_count);

    for (col_idx, col_values) in columns.into_iter().enumerate() {
        let column_name = &column_names[col_idx];
        let typed_values = infer_and_convert_values(&col_values);
        data.add_column(Column::new(
            Cow::Owned(column_name.clone()),
            typed_values,
        ));
    }

    Ok(data)
}

/// Infer types and convert string values to typed `Value` enum.
///
/// This function attempts to parse each value as:
/// 1. Null (empty string)
/// 2. Integer (i64)
/// 3. Float (f64)
/// 4. Boolean (true/false, yes/no, 1/0) - but only non-numeric booleans
/// 5. String (fallback)
fn infer_and_convert_values(values: &[String]) -> Vec<Value<'static>> {
    values
        .iter()
        .map(|s| {
            // Check for null/empty (don't trim for this check)
            if s.is_empty() {
                return Value::Null;
            }

            let trimmed = s.trim();

            // Try to parse as integer first (before boolean, since "1" and "0" are valid integers)
            if let Ok(i) = trimmed.parse::<i64>() {
                return Value::Integer(i);
            }

            // Try to parse as float
            if let Ok(f) = trimmed.parse::<f64>() {
                return Value::Float(f);
            }

            // Check for boolean (non-numeric forms only at this point)
            if let Some(b) = parse_boolean(trimmed) {
                return Value::Boolean(b);
            }

            // Default to string
            Value::String(Cow::Owned(s.clone()))
        })
        .collect()
}

/// Parse a string as a boolean value.
///
/// Recognizes: true, false, yes, no, y, n, t, f, 1, 0 (case-insensitive).
fn parse_boolean(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "yes" | "y" | "t" | "1" => Some(true),
        "false" | "no" | "n" | "f" | "0" => Some(false),
        _ => None,
    }
}

/// Convert `TabularData` to CSV format.
///
/// This function serializes tabular data to CSV text format.
///
/// # Arguments
///
/// * `data` - The tabular data to convert
///
/// # Returns
///
/// A CSV string representation of the data.
///
/// # Examples
///
/// ```
/// use als_compression::convert::{TabularData, Column, Value};
/// use als_compression::convert::csv::to_csv;
/// use std::borrow::Cow;
///
/// let mut data = TabularData::new();
/// data.add_column(Column::new(
///     Cow::Borrowed("id"),
///     vec![Value::Integer(1), Value::Integer(2)],
/// ));
/// data.add_column(Column::new(
///     Cow::Borrowed("name"),
///     vec![Value::string("Alice"), Value::string("Bob")],
/// ));
///
/// let csv = to_csv(&data).unwrap();
/// assert!(csv.contains("id,name"));
/// assert!(csv.contains("1,Alice"));
/// ```
pub fn to_csv(data: &TabularData) -> Result<String> {
    // Handle empty data
    if data.is_empty() || data.column_count() == 0 {
        return Ok(String::new());
    }

    let mut writer = csv::Writer::from_writer(Vec::new());

    // Write headers
    let headers: Vec<&str> = data.column_names();
    writer
        .write_record(&headers)
        .map_err(|e| AlsError::CsvParseError {
            line: 0,
            column: 0,
            message: format!("Failed to write headers: {}", e),
        })?;

    // Write rows
    for row_idx in 0..data.row_count {
        let row: Vec<String> = data
            .columns
            .iter()
            .map(|col| value_to_csv_string(&col.values[row_idx]))
            .collect();

        writer
            .write_record(&row)
            .map_err(|e| AlsError::CsvParseError {
                line: row_idx + 2, // +2 for header and 0-indexing
                column: 0,
                message: format!("Failed to write row: {}", e),
            })?;
    }

    // Flush and get the result
    writer.flush().map_err(|e| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: format!("Failed to flush writer: {}", e),
    })?;

    let bytes = writer.into_inner().map_err(|e| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: format!("Failed to get writer buffer: {}", e),
    })?;

    String::from_utf8(bytes).map_err(|e| AlsError::CsvParseError {
        line: 0,
        column: 0,
        message: format!("Failed to convert to UTF-8: {}", e),
    })
}

/// Convert a `Value` to its CSV string representation.
fn value_to_csv_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Boolean(b) => b.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ColumnType;

    #[test]
    fn test_parse_csv_basic() {
        let csv = "id,name\n1,Alice\n2,Bob";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.column_count(), 2);
        assert_eq!(data.row_count, 2);
        assert_eq!(data.column_names(), vec!["id", "name"]);

        // Check first row
        let row0 = data.get_row(0).unwrap();
        assert_eq!(row0[0].as_integer(), Some(1));
        assert_eq!(row0[1].as_str(), Some("Alice"));

        // Check second row
        let row1 = data.get_row(1).unwrap();
        assert_eq!(row1[0].as_integer(), Some(2));
        assert_eq!(row1[1].as_str(), Some("Bob"));
    }

    #[test]
    fn test_parse_csv_empty() {
        let csv = "";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.column_count(), 0);
        assert_eq!(data.row_count, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_csv_single_row() {
        let csv = "id,name\n1,Alice";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.column_count(), 2);
        assert_eq!(data.row_count, 1);
    }

    #[test]
    fn test_parse_csv_single_column() {
        let csv = "id\n1\n2\n3";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.column_count(), 1);
        assert_eq!(data.row_count, 3);
        assert_eq!(data.column_names(), vec!["id"]);
    }

    #[test]
    fn test_parse_csv_type_inference_integer() {
        let csv = "num\n42\n-10\n0";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.columns[0].inferred_type, ColumnType::Integer);
        assert_eq!(data.columns[0].values[0].as_integer(), Some(42));
        assert_eq!(data.columns[0].values[1].as_integer(), Some(-10));
        assert_eq!(data.columns[0].values[2].as_integer(), Some(0));
    }

    #[test]
    fn test_parse_csv_type_inference_float() {
        let csv = "num\n3.14\n-2.5\n0.0";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.columns[0].inferred_type, ColumnType::Float);
        assert_eq!(data.columns[0].values[0].as_float(), Some(3.14));
    }

    #[test]
    fn test_parse_csv_type_inference_boolean() {
        let csv = "flag\ntrue\nfalse\ntrue";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.columns[0].inferred_type, ColumnType::Boolean);
        assert_eq!(data.columns[0].values[0].as_boolean(), Some(true));
        assert_eq!(data.columns[0].values[1].as_boolean(), Some(false));
    }

    #[test]
    fn test_parse_csv_type_inference_string() {
        let csv = "name\nAlice\nBob\nCharlie";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.columns[0].inferred_type, ColumnType::String);
        assert_eq!(data.columns[0].values[0].as_str(), Some("Alice"));
    }

    #[test]
    fn test_parse_csv_type_inference_mixed() {
        let csv = "mixed\n42\nhello\n3.14";
        let data = parse_csv(csv).unwrap();

        // Mixed types should result in String type
        assert_eq!(data.columns[0].inferred_type, ColumnType::String);
    }

    #[test]
    fn test_parse_csv_null_values() {
        // CSV with empty fields (represented as empty strings between commas or at line ends)
        let csv = "col1,col2,col3\n,value,\n1,,2";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.row_count, 2);
        
        // First row: empty, "value", empty
        assert!(data.columns[0].values[0].is_null());
        assert_eq!(data.columns[1].values[0].as_str(), Some("value"));
        assert!(data.columns[2].values[0].is_null());
        
        // Second row: 1, empty, 2
        assert_eq!(data.columns[0].values[1].as_integer(), Some(1));
        assert!(data.columns[1].values[1].is_null());
        assert_eq!(data.columns[2].values[1].as_integer(), Some(2));
    }

    #[test]
    fn test_parse_csv_boolean_variants() {
        // Note: "1" and "0" are parsed as integers, not booleans
        let csv = "bool\ntrue\nyes\ny\nt\nfalse\nno\nn\nf";
        let data = parse_csv(csv).unwrap();

        assert_eq!(data.columns[0].inferred_type, ColumnType::Boolean);
        assert_eq!(data.columns[0].values[0].as_boolean(), Some(true));
        assert_eq!(data.columns[0].values[1].as_boolean(), Some(true));
        assert_eq!(data.columns[0].values[2].as_boolean(), Some(true));
        assert_eq!(data.columns[0].values[3].as_boolean(), Some(true));
        assert_eq!(data.columns[0].values[4].as_boolean(), Some(false));
        assert_eq!(data.columns[0].values[5].as_boolean(), Some(false));
        assert_eq!(data.columns[0].values[6].as_boolean(), Some(false));
        assert_eq!(data.columns[0].values[7].as_boolean(), Some(false));
    }

    #[test]
    fn test_parse_csv_error_column_mismatch() {
        let csv = "a,b\n1,2\n3"; // Second row has only 1 column
        let result = parse_csv(csv);

        assert!(result.is_err());
        match result {
            Err(AlsError::CsvParseError { line, .. }) => {
                assert_eq!(line, 3); // Line 3 (header is 1, first data row is 2)
            }
            _ => panic!("Expected CsvParseError"),
        }
    }

    #[test]
    fn test_to_csv_basic() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("id"),
            vec![Value::Integer(1), Value::Integer(2)],
        ));
        data.add_column(Column::new(
            Cow::Borrowed("name"),
            vec![Value::string("Alice"), Value::string("Bob")],
        ));

        let csv = to_csv(&data).unwrap();

        assert!(csv.contains("id,name"));
        assert!(csv.contains("1,Alice"));
        assert!(csv.contains("2,Bob"));
    }

    #[test]
    fn test_to_csv_empty() {
        let data = TabularData::new();
        let csv = to_csv(&data).unwrap();

        assert_eq!(csv, "");
    }

    #[test]
    fn test_to_csv_single_row() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("id"),
            vec![Value::Integer(42)],
        ));

        let csv = to_csv(&data).unwrap();

        assert!(csv.contains("id"));
        assert!(csv.contains("42"));
    }

    #[test]
    fn test_to_csv_single_column() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("only"),
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        ));

        let csv = to_csv(&data).unwrap();

        assert!(csv.contains("only"));
        assert!(csv.contains("1"));
        assert!(csv.contains("2"));
        assert!(csv.contains("3"));
    }

    #[test]
    fn test_to_csv_null_values() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("col"),
            vec![Value::Null, Value::Integer(1), Value::Null],
        ));

        let csv = to_csv(&data).unwrap();

        // Null values should be empty strings in CSV
        // The CSV writer may quote empty fields, so we just check that they parse back correctly
        let reparsed = parse_csv(&csv).unwrap();
        assert_eq!(reparsed.row_count, 3);
        assert!(reparsed.columns[0].values[0].is_null());
        assert_eq!(reparsed.columns[0].values[1].as_integer(), Some(1));
        assert!(reparsed.columns[0].values[2].is_null());
    }

    #[test]
    fn test_to_csv_boolean_values() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("flag"),
            vec![Value::Boolean(true), Value::Boolean(false)],
        ));

        let csv = to_csv(&data).unwrap();

        assert!(csv.contains("true"));
        assert!(csv.contains("false"));
    }

    #[test]
    fn test_to_csv_float_values() {
        let mut data = TabularData::new();
        data.add_column(Column::new(
            Cow::Borrowed("num"),
            vec![Value::Float(3.14), Value::Float(-2.5)],
        ));

        let csv = to_csv(&data).unwrap();

        assert!(csv.contains("3.14"));
        assert!(csv.contains("-2.5"));
    }

    #[test]
    fn test_csv_round_trip() {
        let original_csv = "id,name,active\n1,Alice,true\n2,Bob,false\n3,Charlie,true";
        let data = parse_csv(original_csv).unwrap();
        let output_csv = to_csv(&data).unwrap();

        // Parse the output again to verify
        let data2 = parse_csv(&output_csv).unwrap();

        assert_eq!(data.column_count(), data2.column_count());
        assert_eq!(data.row_count, data2.row_count);
        assert_eq!(data.column_names(), data2.column_names());
    }

    #[test]
    fn test_parse_boolean_function() {
        assert_eq!(parse_boolean("true"), Some(true));
        assert_eq!(parse_boolean("TRUE"), Some(true));
        assert_eq!(parse_boolean("yes"), Some(true));
        assert_eq!(parse_boolean("YES"), Some(true));
        assert_eq!(parse_boolean("y"), Some(true));
        assert_eq!(parse_boolean("Y"), Some(true));
        assert_eq!(parse_boolean("t"), Some(true));
        assert_eq!(parse_boolean("T"), Some(true));
        assert_eq!(parse_boolean("1"), Some(true));

        assert_eq!(parse_boolean("false"), Some(false));
        assert_eq!(parse_boolean("FALSE"), Some(false));
        assert_eq!(parse_boolean("no"), Some(false));
        assert_eq!(parse_boolean("NO"), Some(false));
        assert_eq!(parse_boolean("n"), Some(false));
        assert_eq!(parse_boolean("N"), Some(false));
        assert_eq!(parse_boolean("f"), Some(false));
        assert_eq!(parse_boolean("F"), Some(false));
        assert_eq!(parse_boolean("0"), Some(false));

        assert_eq!(parse_boolean("maybe"), None);
        assert_eq!(parse_boolean("2"), None);
        assert_eq!(parse_boolean(""), None);
    }

    #[test]
    fn test_value_to_csv_string() {
        assert_eq!(value_to_csv_string(&Value::Null), "");
        assert_eq!(value_to_csv_string(&Value::Integer(42)), "42");
        assert_eq!(value_to_csv_string(&Value::Float(3.14)), "3.14");
        assert_eq!(value_to_csv_string(&Value::string("hello")), "hello");
        assert_eq!(value_to_csv_string(&Value::Boolean(true)), "true");
        assert_eq!(value_to_csv_string(&Value::Boolean(false)), "false");
    }

    #[test]
    fn test_parse_csv_whitespace_trimming() {
        let csv = "col\n  42  \n  hello  ";
        let data = parse_csv(csv).unwrap();

        // Integers should be parsed even with whitespace
        assert_eq!(data.columns[0].values[0].as_integer(), Some(42));
        // Strings preserve original spacing
        assert_eq!(data.columns[0].values[1].as_str(), Some("  hello  "));
    }
}
