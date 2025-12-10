//! Integration tests for streaming compression and decompression.

use als_compression::{StreamingCompressor, StreamingParser};
use std::io::Cursor;

#[test]
fn test_streaming_csv_round_trip() {
    // Create a CSV with multiple rows
    let csv_data = "\
id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
4,David,400
5,Eve,500
";

    // Compress using streaming compressor
    let cursor = Cursor::new(csv_data.as_bytes());
    let mut compressor = StreamingCompressor::new(cursor).with_csv_chunk_size(2);

    let mut compressed_chunks = Vec::new();
    for chunk_result in compressor.compress_csv_chunks() {
        let chunk = chunk_result.unwrap();
        compressed_chunks.push(chunk);
    }

    // Should have multiple chunks due to small chunk size
    assert!(!compressed_chunks.is_empty());

    // Each chunk is independently parseable
    // In real usage, chunks would be processed as they arrive
    let mut total_row_count = 0;
    for chunk in &compressed_chunks {
        let cursor = Cursor::new(chunk.as_bytes());
        let mut parser = StreamingParser::new(cursor);

        for row_result in parser.parse_rows() {
            let row = row_result.unwrap();
            assert_eq!(row.len(), 3); // Three columns
            total_row_count += 1;
        }
    }

    // Should have parsed all rows (5 rows total)
    assert_eq!(total_row_count, 5);
}

#[test]
fn test_streaming_large_csv() {
    // Generate a larger CSV
    let mut csv_data = String::from("id,value\n");
    for i in 1..=100 {
        csv_data.push_str(&format!("{},{}\n", i, i * 10));
    }

    // Compress using streaming compressor
    let cursor = Cursor::new(csv_data.as_bytes());
    let mut compressor = StreamingCompressor::new(cursor).with_csv_chunk_size(10);

    let mut chunk_count = 0;
    for chunk_result in compressor.compress_csv_chunks() {
        chunk_result.unwrap();
        chunk_count += 1;
    }

    // Should have multiple chunks
    assert!(chunk_count > 1);
}

#[test]
fn test_streaming_parser_incremental() {
    // Create ALS data
    let als_data = "#id #name #status\n1>5|Alice Bob Charlie David Eve|active*5";

    // Parse using streaming parser
    let cursor = Cursor::new(als_data.as_bytes());
    let mut parser = StreamingParser::new(cursor);

    let rows: Vec<_> = parser.parse_rows().collect::<Result<Vec<_>, _>>().unwrap();

    // Should have 5 rows
    assert_eq!(rows.len(), 5);

    // Each row should have 3 columns
    for row in &rows {
        assert_eq!(row.len(), 3);
    }
}

#[test]
fn test_streaming_empty_input() {
    // Test empty CSV
    let csv_data = "";
    let cursor = Cursor::new(csv_data.as_bytes());
    let mut compressor = StreamingCompressor::new(cursor);

    let chunks: Vec<_> = compressor.compress_csv_chunks().collect();
    assert!(chunks.is_empty());

    // Test empty ALS
    let als_data = "";
    let cursor = Cursor::new(als_data.as_bytes());
    let mut parser = StreamingParser::new(cursor);

    let rows: Vec<_> = parser.parse_rows().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(rows.is_empty());
}

#[test]
fn test_streaming_with_patterns() {
    // CSV with patterns that should compress well
    let csv_data = "\
id,status
1,active
2,active
3,active
4,active
5,active
";

    // Compress
    let cursor = Cursor::new(csv_data.as_bytes());
    let mut compressor = StreamingCompressor::new(cursor);

    let mut compressed = String::new();
    for chunk_result in compressor.compress_csv_chunks() {
        compressed.push_str(&chunk_result.unwrap());
    }

    // Should contain range and multiplier patterns
    assert!(compressed.contains("1>5") || compressed.contains("active*5"));
}

#[test]
fn test_streaming_buffer_sizes() {
    let csv_data = "id,value\n1,100\n2,200\n3,300\n";

    // Test with different buffer sizes
    for buffer_size in [64, 256, 1024] {
        let cursor = Cursor::new(csv_data.as_bytes());
        let mut compressor = StreamingCompressor::new(cursor).with_buffer_size(buffer_size);

        let chunks: Vec<_> = compressor
            .compress_csv_chunks()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(!chunks.is_empty());
    }
}

#[test]
fn test_streaming_compressor_configuration() {
    use als_compression::CompressorConfig;

    let csv_data = "id,value\n1,100\n2,200\n";
    let cursor = Cursor::new(csv_data.as_bytes());

    let config = CompressorConfig::new()
        .with_ctx_fallback_threshold(1.5)
        .with_min_pattern_length(2);

    let mut compressor = StreamingCompressor::with_config(cursor, config);

    let chunks: Vec<_> = compressor
        .compress_csv_chunks()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(!chunks.is_empty());
}

#[test]
fn test_streaming_parser_configuration() {
    use als_compression::ParserConfig;

    let als_data = "#id\n1>10";
    let cursor = Cursor::new(als_data.as_bytes());

    let config = ParserConfig::new().with_parallelism(1);
    let mut parser = StreamingParser::with_config(cursor, config);

    let rows: Vec<_> = parser.parse_rows().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(rows.len(), 10);
}
