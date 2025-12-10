//! Integration tests for async compression and decompression.
//!
//! These tests verify that async methods work correctly and integrate
//! properly with the Tokio runtime.

#![cfg(feature = "async")]

use als_compression::{AlsCompressor, AlsParser};

#[tokio::test]
async fn test_compress_csv_async_basic() {
    let compressor = AlsCompressor::new();
    let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    
    let als = compressor.compress_csv_async(csv).await.unwrap();
    
    // Should produce valid ALS output
    assert!(!als.is_empty());
    // Should contain schema
    assert!(als.contains("#id") || als.contains("#name"));
}

#[tokio::test]
async fn test_compress_csv_async_with_patterns() {
    let compressor = AlsCompressor::new();
    let csv = "id,status\n1,active\n2,active\n3,active\n4,active\n5,active";
    
    let als = compressor.compress_csv_async(csv).await.unwrap();
    
    // Should produce valid ALS output
    assert!(!als.is_empty());
    // Should detect patterns (range for id, multiplier for status)
    assert!(als.contains(">") || als.contains("*"));
}

#[tokio::test]
async fn test_compress_csv_async_empty() {
    let compressor = AlsCompressor::new();
    let csv = "";
    
    let als = compressor.compress_csv_async(csv).await.unwrap();
    
    // Should handle empty CSV
    assert!(!als.is_empty());
}

#[tokio::test]
async fn test_compress_json_async_basic() {
    let compressor = AlsCompressor::new();
    let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
    
    let als = compressor.compress_json_async(json).await.unwrap();
    
    // Should produce valid ALS output
    assert!(!als.is_empty());
    // Should contain schema
    assert!(als.contains("#id") || als.contains("#name"));
}

#[tokio::test]
async fn test_compress_json_async_with_patterns() {
    let compressor = AlsCompressor::new();
    let json = r#"[
        {"id": 1, "status": "active"},
        {"id": 2, "status": "active"},
        {"id": 3, "status": "active"},
        {"id": 4, "status": "active"},
        {"id": 5, "status": "active"}
    ]"#;
    
    let als = compressor.compress_json_async(json).await.unwrap();
    
    // Should produce valid ALS output
    assert!(!als.is_empty());
    // Should detect patterns
    assert!(als.contains(">") || als.contains("*"));
}

#[tokio::test]
async fn test_compress_json_async_nested() {
    let compressor = AlsCompressor::new();
    let json = r#"[
        {"id": 1, "user": {"name": "Alice", "age": 30}},
        {"id": 2, "user": {"name": "Bob", "age": 25}}
    ]"#;
    
    let als = compressor.compress_json_async(json).await.unwrap();
    
    // Should produce valid ALS output with flattened columns
    assert!(!als.is_empty());
    // Should contain flattened schema (dot-notation)
    assert!(als.contains("user.name") || als.contains("user.age"));
}

#[tokio::test]
async fn test_compress_json_async_with_nulls() {
    let compressor = AlsCompressor::new();
    let json = r#"[
        {"id": 1, "name": "Alice", "email": null},
        {"id": 2, "name": null, "email": "bob@example.com"}
    ]"#;
    
    let als = compressor.compress_json_async(json).await.unwrap();
    
    // Should handle null values
    assert!(!als.is_empty());
}

#[tokio::test]
async fn test_compress_json_async_error_invalid() {
    let compressor = AlsCompressor::new();
    let json = r#"{"not": "an array"}"#;
    
    let result = compressor.compress_json_async(json).await;
    
    // Should return error for non-array JSON
    assert!(result.is_err());
}

#[tokio::test]
async fn test_parse_async_basic() {
    let parser = AlsParser::new();
    let als = "#id #name\n1>3|alice bob charlie";
    
    let doc = parser.parse_async(als).await.unwrap();
    
    assert_eq!(doc.schema, vec!["id", "name"]);
    assert_eq!(doc.streams.len(), 2);
}

#[tokio::test]
async fn test_parse_async_with_dictionary() {
    let parser = AlsParser::new();
    let als = "$default:red|green|blue\n#col\n_0 _1 _2";
    
    let doc = parser.parse_async(als).await.unwrap();
    
    assert!(doc.dictionaries.contains_key("default"));
    assert_eq!(doc.dictionaries["default"], vec!["red", "green", "blue"]);
}

#[tokio::test]
async fn test_parse_async_version() {
    let parser = AlsParser::new();
    let als = "!v1\n#col\n1>5";
    
    let doc = parser.parse_async(als).await.unwrap();
    
    assert_eq!(doc.version, 1);
}

#[tokio::test]
async fn test_to_csv_async_basic() {
    let parser = AlsParser::new();
    let als = "#id #name\n1>3|alice bob charlie";
    
    let csv = parser.to_csv_async(als).await.unwrap();
    
    // Should contain CSV header
    assert!(csv.contains("id,name"));
    // Should contain data rows
    assert!(csv.contains("1,alice"));
    assert!(csv.contains("2,bob"));
    assert!(csv.contains("3,charlie"));
}

#[tokio::test]
async fn test_to_csv_async_empty() {
    let parser = AlsParser::new();
    let als = "";
    
    let csv = parser.to_csv_async(als).await.unwrap();
    
    // Empty input produces empty CSV (no header, no data)
    assert_eq!(csv, "");
}

#[tokio::test]
async fn test_to_csv_async_with_patterns() {
    let parser = AlsParser::new();
    let als = "#id #status\n1>5|active*5";
    
    let csv = parser.to_csv_async(als).await.unwrap();
    
    // Should expand patterns correctly
    assert!(csv.contains("id,status"));
    assert!(csv.contains("1,active"));
    assert!(csv.contains("5,active"));
}

#[tokio::test]
async fn test_to_json_async_basic() {
    let parser = AlsParser::new();
    let als = "#id #name\n1>3|alice bob charlie";
    
    let json = parser.to_json_async(als).await.unwrap();
    
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

#[tokio::test]
async fn test_to_json_async_empty() {
    let parser = AlsParser::new();
    let als = "";
    
    let json = parser.to_json_async(als).await.unwrap();
    
    assert_eq!(json, "[]");
}

#[tokio::test]
async fn test_to_json_async_with_types() {
    let parser = AlsParser::new();
    let als = "#int #float #bool #str\n42|3.14|true|hello";
    
    let json = parser.to_json_async(als).await.unwrap();
    
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let array = parsed.as_array().unwrap();
    
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["int"], 42);
    assert_eq!(array[0]["float"], 3.14);
    assert_eq!(array[0]["bool"], true);
    assert_eq!(array[0]["str"], "hello");
}

#[tokio::test]
async fn test_to_json_async_nested_reconstruction() {
    let parser = AlsParser::new();
    let als = "#id #user.name #user.age\n1|alice|30";
    
    let json = parser.to_json_async(als).await.unwrap();
    
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let array = parsed.as_array().unwrap();
    
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["id"], 1);
    assert_eq!(array[0]["user"]["name"], "alice");
    assert_eq!(array[0]["user"]["age"], 30);
}

#[tokio::test]
async fn test_expand_async_basic() {
    let parser = AlsParser::new();
    let als = "#id #name\n1>3|alice bob charlie";
    let doc = parser.parse(als).unwrap();
    
    let rows = parser.expand_async(doc).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "alice"]);
    assert_eq!(rows[1], vec!["2", "bob"]);
    assert_eq!(rows[2], vec!["3", "charlie"]);
}

#[tokio::test]
async fn test_expand_async_with_dictionary() {
    let parser = AlsParser::new();
    let als = "$default:red|green|blue\n#col\n_0 _1 _2";
    let doc = parser.parse(als).unwrap();
    
    let rows = parser.expand_async(doc).await.unwrap();
    
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["red"]);
    assert_eq!(rows[1], vec!["green"]);
    assert_eq!(rows[2], vec!["blue"]);
}

#[tokio::test]
async fn test_round_trip_csv_async() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "id,name,status\n1,Alice,active\n2,Bob,active\n3,Charlie,inactive";
    
    // Compress
    let als = compressor.compress_csv_async(original_csv).await.unwrap();
    
    // Decompress
    let result_csv = parser.to_csv_async(&als).await.unwrap();
    
    // Parse both CSVs to compare (order and formatting may differ)
    let original_lines: Vec<&str> = original_csv.lines().collect();
    let result_lines: Vec<&str> = result_csv.lines().collect();
    
    // Should have same number of lines
    assert_eq!(original_lines.len(), result_lines.len());
    
    // Header should match
    assert_eq!(original_lines[0], result_lines[0]);
}

#[tokio::test]
async fn test_round_trip_json_async() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_json = r#"[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]"#;
    
    // Compress
    let als = compressor.compress_json_async(original_json).await.unwrap();
    
    // Decompress
    let result_json = parser.to_json_async(&als).await.unwrap();
    
    // Parse both JSONs to compare
    let original: serde_json::Value = serde_json::from_str(original_json).unwrap();
    let result: serde_json::Value = serde_json::from_str(&result_json).unwrap();
    
    // Should be equivalent
    assert_eq!(original, result);
}

#[tokio::test]
async fn test_concurrent_compression() {
    let compressor = std::sync::Arc::new(AlsCompressor::new());
    
    let mut handles = vec![];
    
    for i in 0..10 {
        let compressor = compressor.clone();
        let handle = tokio::spawn(async move {
            let csv = format!("id,value\n{},{}", i, i * 10);
            compressor.compress_csv_async(&csv).await
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_concurrent_decompression() {
    let parser = std::sync::Arc::new(AlsParser::new());
    
    let mut handles = vec![];
    
    for i in 0..10 {
        let parser = parser.clone();
        let handle = tokio::spawn(async move {
            let als = format!("#id #value\n{}|{}", i, i * 10);
            parser.to_csv_async(&als).await
        });
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_async_with_large_data() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    // Create a large CSV
    let mut csv = String::from("id,value\n");
    for i in 0..1000 {
        csv.push_str(&format!("{},{}\n", i, i * 2));
    }
    
    // Compress
    let als = compressor.compress_csv_async(&csv).await.unwrap();
    
    // Decompress
    let result_csv = parser.to_csv_async(&als).await.unwrap();
    
    // Should have same number of lines
    let original_lines = csv.lines().count();
    let result_lines = result_csv.lines().count();
    assert_eq!(original_lines, result_lines);
}

#[tokio::test]
async fn test_async_cancellation_safety() {
    use tokio::time::{timeout, Duration};
    
    let compressor = AlsCompressor::new();
    
    // Create a moderately sized CSV
    let mut csv = String::from("id,value\n");
    for i in 0..100 {
        csv.push_str(&format!("{},{}\n", i, i * 2));
    }
    
    // Try to compress with a very short timeout (may or may not complete)
    let result = timeout(
        Duration::from_micros(1),
        compressor.compress_csv_async(&csv)
    ).await;
    
    // Either it completes successfully or times out - both are acceptable
    // The important thing is that it doesn't panic or leave resources in a bad state
    match result {
        Ok(Ok(_)) => {
            // Completed successfully
        }
        Ok(Err(_)) => {
            // Completed with an error
        }
        Err(_) => {
            // Timed out - this is expected and acceptable
        }
    }
}
