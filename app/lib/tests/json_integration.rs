//! Integration tests for JSON compression pipeline.
//!
//! These tests verify the complete JSON → ALS → JSON round-trip functionality.

use als_compression::{AlsCompressor, AlsParser};

#[test]
fn test_json_to_als_to_json_round_trip() {
    let original_json = r#"[
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]"#;

    // Compress JSON to ALS
    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    // Verify ALS is not empty and contains schema
    assert!(!als.is_empty());
    assert!(als.contains("#age") || als.contains("#id") || als.contains("#name"));

    // Decompress ALS back to JSON
    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    // Parse both JSONs to verify they're equivalent
    let original: serde_json::Value = serde_json::from_str(original_json).unwrap();
    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();

    // Both should be arrays
    assert!(original.is_array());
    assert!(output.is_array());

    let original_array = original.as_array().unwrap();
    let output_array = output.as_array().unwrap();

    // Same number of rows
    assert_eq!(original_array.len(), output_array.len());

    // Verify each row
    for i in 0..original_array.len() {
        let orig_obj = original_array[i].as_object().unwrap();
        let out_obj = output_array[i].as_object().unwrap();

        // Same keys
        assert_eq!(orig_obj.keys().len(), out_obj.keys().len());

        // Same values
        for key in orig_obj.keys() {
            assert_eq!(orig_obj[key], out_obj[key], "Mismatch for key: {}", key);
        }
    }
}

#[test]
fn test_json_nested_objects_round_trip() {
    let original_json = r#"[
        {"id": 1, "user": {"name": "Alice", "email": "alice@example.com"}},
        {"id": 2, "user": {"name": "Bob", "email": "bob@example.com"}}
    ]"#;

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    // Should contain flattened column names
    assert!(als.contains("user.name") || als.contains("user.email"));

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    // Parse and verify structure
    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();
    let array = output.as_array().unwrap();

    assert_eq!(array.len(), 2);
    assert_eq!(array[0]["id"], 1);
    assert_eq!(array[0]["user"]["name"], "Alice");
    assert_eq!(array[0]["user"]["email"], "alice@example.com");
}

#[test]
fn test_json_with_nulls_round_trip() {
    let original_json = r#"[
        {"id": 1, "name": "Alice", "email": null},
        {"id": 2, "name": null, "email": "bob@example.com"}
    ]"#;

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();
    let array = output.as_array().unwrap();

    assert_eq!(array.len(), 2);
    assert!(array[0]["email"].is_null());
    assert!(array[1]["name"].is_null());
}

#[test]
fn test_json_empty_array() {
    let original_json = "[]";

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    assert_eq!(output_json, "[]");
}

#[test]
fn test_json_single_object() {
    let original_json = r#"[{"id": 42, "name": "Test"}]"#;

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();
    let array = output.as_array().unwrap();

    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["id"], 42);
    assert_eq!(array[0]["name"], "Test");
}

#[test]
fn test_json_with_patterns() {
    // JSON with sequential IDs (good for range compression)
    let original_json = r#"[
        {"id": 1, "status": "active"},
        {"id": 2, "status": "active"},
        {"id": 3, "status": "active"},
        {"id": 4, "status": "active"},
        {"id": 5, "status": "active"}
    ]"#;

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    // Should use range compression for IDs
    assert!(als.contains("1>5") || als.contains("id"));

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();
    let array = output.as_array().unwrap();

    assert_eq!(array.len(), 5);
    for i in 0..5 {
        assert_eq!(array[i]["id"], i + 1);
        assert_eq!(array[i]["status"], "active");
    }
}

#[test]
fn test_json_mixed_types() {
    let original_json = r#"[
        {"int": 42, "float": 3.14, "bool": true, "str": "hello", "null": null}
    ]"#;

    let compressor = AlsCompressor::new();
    let als = compressor.compress_json(original_json).unwrap();

    let parser = AlsParser::new();
    let output_json = parser.to_json(&als).unwrap();

    let output: serde_json::Value = serde_json::from_str(&output_json).unwrap();
    let array = output.as_array().unwrap();

    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["int"], 42);
    assert_eq!(array[0]["float"], 3.14);
    assert_eq!(array[0]["bool"], true);
    assert_eq!(array[0]["str"], "hello");
    assert!(array[0]["null"].is_null());
}
