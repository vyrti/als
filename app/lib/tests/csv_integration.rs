//! Integration tests for CSV compression pipeline.

use als_compression::{AlsCompressor, AlsParser};

#[test]
fn test_csv_compression_basic() {
    let compressor = AlsCompressor::new();
    let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    
    let als = compressor.compress_csv(csv).unwrap();
    
    // Should produce valid ALS output
    assert!(!als.is_empty());
    assert!(als.contains("#id"));
    assert!(als.contains("#name"));
}

#[test]
fn test_csv_round_trip_basic() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    // Should have same structure
    assert_eq!(original_data.column_count(), result_data.column_count());
    assert_eq!(original_data.row_count, result_data.row_count);
    assert_eq!(original_data.column_names(), result_data.column_names());
}

#[test]
fn test_csv_round_trip_with_patterns() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    // CSV with sequential integers (good for range compression)
    let original_csv = "id,value\n1,100\n2,100\n3,100\n4,100\n5,100";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Should use range compression for id column
    assert!(als.contains(">") || als.contains("*"));
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    // Should have same structure
    assert_eq!(original_data.column_count(), result_data.column_count());
    assert_eq!(original_data.row_count, result_data.row_count);
    
    // Check values match
    for row_idx in 0..original_data.row_count {
        let orig_row = original_data.get_row(row_idx).unwrap();
        let result_row = result_data.get_row(row_idx).unwrap();
        
        for col_idx in 0..original_data.column_count() {
            assert_eq!(
                orig_row[col_idx].to_string_repr(),
                result_row[col_idx].to_string_repr(),
                "Mismatch at row {}, col {}",
                row_idx,
                col_idx
            );
        }
    }
}

#[test]
fn test_csv_round_trip_empty() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Both should be empty
    assert_eq!(result_csv.trim(), "");
}

#[test]
fn test_csv_round_trip_single_row() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "id,name\n42,Alice";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    assert_eq!(original_data.row_count, 1);
    assert_eq!(result_data.row_count, 1);
}

#[test]
fn test_csv_round_trip_single_column() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "id\n1\n2\n3";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    assert_eq!(original_data.column_count(), 1);
    assert_eq!(result_data.column_count(), 1);
    assert_eq!(original_data.row_count, 3);
    assert_eq!(result_data.row_count, 3);
}

#[test]
fn test_csv_round_trip_with_nulls() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "a,b,c\n1,,3\n,2,\n4,5,6";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    assert_eq!(original_data.column_count(), result_data.column_count());
    assert_eq!(original_data.row_count, result_data.row_count);
    
    // Check null values are preserved
    assert!(original_data.columns[1].values[0].is_null());
    assert!(result_data.columns[1].values[0].is_null());
}

#[test]
fn test_csv_round_trip_with_types() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();
    
    let original_csv = "int,float,bool,str\n42,3.14,true,hello\n-10,-2.5,false,world";
    
    // Compress to ALS
    let als = compressor.compress_csv(original_csv).unwrap();
    
    // Decompress back to CSV
    let result_csv = parser.to_csv(&als).unwrap();
    
    // Parse both CSVs to compare
    let original_data = als_compression::convert::csv::parse_csv(original_csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();
    
    assert_eq!(original_data.column_count(), result_data.column_count());
    assert_eq!(original_data.row_count, result_data.row_count);
    
    // Check types are preserved
    assert!(original_data.columns[0].values[0].is_integer());
    assert!(result_data.columns[0].values[0].is_integer());
    
    assert!(original_data.columns[1].values[0].is_float());
    assert!(result_data.columns[1].values[0].is_float());
    
    assert!(original_data.columns[2].values[0].is_boolean());
    assert!(result_data.columns[2].values[0].is_boolean());
    
    assert!(original_data.columns[3].values[0].is_string());
    assert!(result_data.columns[3].values[0].is_string());
}

#[test]
fn test_csv_compression_ratio() {
    let compressor = AlsCompressor::new();
    
    // CSV with highly compressible data
    let csv = "id,status\n1,active\n2,active\n3,active\n4,active\n5,active\n6,active\n7,active\n8,active\n9,active\n10,active";
    
    let als = compressor.compress_csv(csv).unwrap();
    
    // ALS should be significantly smaller than original CSV
    assert!(als.len() < csv.len());
}

#[test]
fn test_csv_error_handling_malformed() {
    let compressor = AlsCompressor::new();
    
    // Malformed CSV (inconsistent column count)
    let csv = "a,b\n1,2\n3"; // Second row has only 1 column
    
    let result = compressor.compress_csv(csv);
    
    // Should return an error
    assert!(result.is_err());
}
