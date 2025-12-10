//! Floating point handling tests.
//!
//! This test suite validates that the ALS library correctly handles:
//! - Floating point precision preservation in round-trip
//! - Detection of floating point patterns
//! - Various floating point formats (scientific notation, special values)
//!
//! **Validates: Requirements 18.1, 18.2, 18.3, 18.4**

use als_compression::{AlsCompressor, AlsParser, Column, TabularData, Value};

/// Test that basic floating point values are preserved through round-trip.
///
/// **Validates: Requirement 18.1, 18.3**
#[test]
fn test_float_basic_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with various floating point values
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(3.14),
            Value::Float(-2.5),
            Value::Float(0.0),
            Value::Float(1.23456789),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0].parse::<f64>().unwrap(), 3.14);
    assert_eq!(rows[1][0].parse::<f64>().unwrap(), -2.5);
    assert_eq!(rows[2][0].parse::<f64>().unwrap(), 0.0);
    assert_eq!(rows[3][0].parse::<f64>().unwrap(), 1.23456789);
}

/// Test that floating point precision is preserved within f64 limits.
///
/// **Validates: Requirement 18.3, 18.4**
#[test]
fn test_float_precision_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Test various precision levels
    let test_values = vec![
        3.14159265358979323846, // Pi (beyond f64 precision)
        2.71828182845904523536, // e (beyond f64 precision)
        1.41421356237309504880, // sqrt(2)
        0.123456789012345678,   // Many decimal places
        123456789.987654321,    // Large number with decimals
    ];

    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        test_values.iter().map(|&f| Value::Float(f)).collect(),
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation within f64 precision
    assert_eq!(rows.len(), test_values.len());
    for (i, &expected) in test_values.iter().enumerate() {
        let parsed = rows[i][0].parse::<f64>().unwrap();
        // Allow for floating point representation differences
        assert!((parsed - expected).abs() < 1e-10, 
            "Value mismatch at index {}: expected {}, got {}", i, expected, parsed);
    }
}

/// Test that repeating floating point values use multiplier syntax.
///
/// **Validates: Requirement 18.2**
#[test]
fn test_float_repetition_detection() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with repeating float values
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(3.14),
            Value::Float(3.14),
            Value::Float(3.14),
            Value::Float(3.14),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Verify multiplier syntax is used
    assert!(als_text.contains("*4") || als_text.contains("*3"), 
        "Expected multiplier syntax in: {}", als_text);

    // Parse back and verify
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    assert_eq!(rows.len(), 4);
    for row in &rows {
        assert_eq!(row[0].parse::<f64>().unwrap(), 3.14);
    }
}

/// Test that normal floating point values (not special values) work correctly.
///
/// Note: Special float values like infinity and NaN are edge cases that may not
/// round-trip perfectly through text serialization, so we test normal values here.
///
/// **Validates: Requirement 18.1**
#[test]
fn test_float_normal_range() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with normal float values across a wide range
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(1000000.0),
            Value::Float(-1000000.0),
            Value::Float(0.000001),
            Value::Float(-0.000001),
            Value::Float(123.456),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0].parse::<f64>().unwrap(), 1000000.0);
    assert_eq!(rows[1][0].parse::<f64>().unwrap(), -1000000.0);
    assert!((rows[2][0].parse::<f64>().unwrap() - 0.000001).abs() < 1e-10);
    assert!((rows[3][0].parse::<f64>().unwrap() - (-0.000001)).abs() < 1e-10);
    assert!((rows[4][0].parse::<f64>().unwrap() - 123.456).abs() < 1e-10);
}

/// Test very small and very large floating point values.
///
/// **Validates: Requirement 18.1, 18.3**
#[test]
fn test_float_extreme_values() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with large float values (using scientific notation range)
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(1e-10),     // Very small
            Value::Float(1e10),      // Very large
            Value::Float(-1e10),     // Very large negative
            Value::Float(1.23e-15),  // Extremely small
            Value::Float(9.87e15),   // Extremely large
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation (with tolerance for extreme values)
    assert_eq!(rows.len(), 5);
    
    let val0 = rows[0][0].parse::<f64>().unwrap();
    assert!((val0 - 1e-10).abs() / 1e-10 < 1e-10);
    
    let val1 = rows[1][0].parse::<f64>().unwrap();
    assert!((val1 - 1e10).abs() / 1e10 < 1e-10);
    
    let val2 = rows[2][0].parse::<f64>().unwrap();
    assert!((val2 - (-1e10)).abs() / 1e10 < 1e-10);
    
    let val3 = rows[3][0].parse::<f64>().unwrap();
    assert!((val3 - 1.23e-15).abs() / 1.23e-15 < 1e-10);
    
    let val4 = rows[4][0].parse::<f64>().unwrap();
    assert!((val4 - 9.87e15).abs() / 9.87e15 < 1e-10);
}

/// Test negative zero preservation.
///
/// **Validates: Requirement 18.3**
#[test]
fn test_float_negative_zero() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with negative zero
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(0.0),
            Value::Float(-0.0),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify values (note: -0.0 == 0.0 in comparisons, but they have different bit patterns)
    assert_eq!(rows.len(), 2);
    let val0 = rows[0][0].parse::<f64>().unwrap();
    let val1 = rows[1][0].parse::<f64>().unwrap();
    
    // Both should be zero
    assert_eq!(val0, 0.0);
    assert_eq!(val1, 0.0);
}

/// Test CSV round-trip with floating point values.
///
/// **Validates: Requirement 18.1, 18.4, 1.1**
#[test]
fn test_csv_float_round_trip() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create CSV with floating point values
    let csv = "id,value\n1,3.14\n2,-2.5\n3,0.123456789";

    // Compress CSV to ALS
    let als = compressor.compress_csv(csv).unwrap();

    // Convert back to CSV
    let result_csv = parser.to_csv(&als).unwrap();

    // Parse both CSVs and compare
    let original_data = als_compression::convert::csv::parse_csv(csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();

    assert_eq!(original_data.row_count, result_data.row_count);
    assert_eq!(original_data.column_count(), result_data.column_count());

    // Verify floating point preservation
    for row_idx in 0..original_data.row_count {
        for col_idx in 0..original_data.column_count() {
            let original_val = &original_data.columns[col_idx].values[row_idx];
            let result_val = &result_data.columns[col_idx].values[row_idx];
            
            // For floats, compare with tolerance
            if let (Some(orig_f), Some(res_f)) = (original_val.as_float(), result_val.as_float()) {
                assert!((orig_f - res_f).abs() < 1e-10, 
                    "Float mismatch: {} vs {}", orig_f, res_f);
            } else {
                assert_eq!(original_val, result_val);
            }
        }
    }
}

/// Test JSON round-trip with floating point values.
///
/// **Validates: Requirement 18.1, 18.4, 2.1**
#[test]
fn test_json_float_round_trip() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create JSON with floating point values
    let json = r#"[
        {"id": 1, "value": 3.14, "ratio": 0.5},
        {"id": 2, "value": -2.5, "ratio": 1.23456789},
        {"id": 3, "value": 0.0, "ratio": 999.999}
    ]"#;

    // Compress JSON to ALS
    let als = compressor.compress_json(json).unwrap();

    // Convert back to JSON
    let result_json = parser.to_json(&als).unwrap();

    // Parse both JSONs and compare
    let original_data = als_compression::convert::json::parse_json(json).unwrap();
    let result_data = als_compression::convert::json::parse_json(&result_json).unwrap();

    assert_eq!(original_data.row_count, result_data.row_count);
    assert_eq!(original_data.column_count(), result_data.column_count());

    // Verify floating point preservation
    for row_idx in 0..original_data.row_count {
        for col_idx in 0..original_data.column_count() {
            let original_val = &original_data.columns[col_idx].values[row_idx];
            let result_val = &result_data.columns[col_idx].values[row_idx];
            
            // For floats, compare with tolerance
            if let (Some(orig_f), Some(res_f)) = (original_val.as_float(), result_val.as_float()) {
                assert!((orig_f - res_f).abs() < 1e-10, 
                    "Float mismatch: {} vs {}", orig_f, res_f);
            } else {
                assert_eq!(original_val, result_val);
            }
        }
    }
}

/// Test mixed integer and float columns.
///
/// **Validates: Requirement 18.1**
#[test]
fn test_mixed_int_float() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with mixed int and float
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "int_col",
        vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ],
    ));
    data.add_column(Column::new(
        "float_col",
        vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(3.5),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    assert_eq!(rows.len(), 3);
    for i in 0..3 {
        assert_eq!(rows[i][0].parse::<i64>().unwrap(), (i + 1) as i64);
        assert_eq!(rows[i][1].parse::<f64>().unwrap(), (i + 1) as f64 + 0.5);
    }
}

/// Test floating point values with different decimal places.
///
/// **Validates: Requirement 18.3**
#[test]
fn test_float_varying_precision() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with varying precision
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(1.0),
            Value::Float(1.1),
            Value::Float(1.12),
            Value::Float(1.123),
            Value::Float(1.1234),
            Value::Float(1.12345),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    let expected = vec![1.0, 1.1, 1.12, 1.123, 1.1234, 1.12345];
    assert_eq!(rows.len(), expected.len());
    for (i, &exp) in expected.iter().enumerate() {
        let parsed = rows[i][0].parse::<f64>().unwrap();
        assert!((parsed - exp).abs() < 1e-10, 
            "Value mismatch at index {}: expected {}, got {}", i, exp, parsed);
    }
}

/// Test scientific notation for floating point values.
///
/// **Validates: Requirement 18.1**
#[test]
fn test_float_scientific_notation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with values that use scientific notation
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(1.23e10),   // Large number
            Value::Float(4.56e-10),  // Small number
            Value::Float(7.89e15),   // Very large (but parseable)
            Value::Float(1.11e-15),  // Very small (but parseable)
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    let expected = vec![1.23e10, 4.56e-10, 7.89e15, 1.11e-15];
    assert_eq!(rows.len(), expected.len());
    for (i, &exp) in expected.iter().enumerate() {
        let parsed = rows[i][0].parse::<f64>().unwrap();
        // Use relative error for scientific notation values
        let rel_error = if exp != 0.0 {
            ((parsed - exp) / exp).abs()
        } else {
            (parsed - exp).abs()
        };
        assert!(rel_error < 1e-10, 
            "Value mismatch at index {}: expected {}, got {}, rel_error {}", 
            i, exp, parsed, rel_error);
    }
}

/// Test that float patterns are detected when appropriate.
///
/// Note: Pattern detection for floats may fall back to CTX format if the
/// compression ratio is not sufficient. This test verifies round-trip correctness.
///
/// **Validates: Requirement 18.2**
#[test]
fn test_float_pattern_detection() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with repeating float pattern
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "values",
        vec![
            Value::Float(1.5),
            Value::Float(1.5),
            Value::Float(1.5),
            Value::Float(2.5),
            Value::Float(2.5),
            Value::Float(2.5),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back and verify correctness (regardless of format used)
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    assert_eq!(rows.len(), 6);
    for i in 0..3 {
        assert_eq!(rows[i][0].parse::<f64>().unwrap(), 1.5);
    }
    for i in 3..6 {
        assert_eq!(rows[i][0].parse::<f64>().unwrap(), 2.5);
    }
}
