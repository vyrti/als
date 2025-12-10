//! Unicode and special character handling tests.
//!
//! This test suite validates that the ALS library correctly handles:
//! - UTF-8 encoding preservation
//! - Emoji and special Unicode symbols
//! - Right-to-left (RTL) text
//! - Combining characters and grapheme clusters
//! - Various Unicode scripts and languages
//!
//! **Validates: Requirements 33.1, 33.2, 33.3, 33.4**

use als_compression::{AlsCompressor, AlsParser, Column, TabularData, Value};

/// Test that basic Unicode characters (non-ASCII) are preserved through round-trip.
///
/// **Validates: Requirement 33.1**
#[test]
fn test_unicode_basic_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with various Unicode characters
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("héllo".to_string()),
            Value::string_owned("wörld".to_string()),
            Value::string_owned("café".to_string()),
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
    assert_eq!(rows[0][0], "héllo");
    assert_eq!(rows[1][0], "wörld");
    assert_eq!(rows[2][0], "café");
}

/// Test that emoji are preserved through round-trip.
///
/// **Validates: Requirement 33.2**
#[test]
fn test_emoji_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with various emoji
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "emoji",
        vec![
            Value::string_owned("🎉".to_string()),
            Value::string_owned("😀".to_string()),
            Value::string_owned("🚀".to_string()),
            Value::string_owned("❤️".to_string()),
            Value::string_owned("🌟".to_string()),
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
    assert_eq!(rows[0][0], "🎉");
    assert_eq!(rows[1][0], "😀");
    assert_eq!(rows[2][0], "🚀");
    assert_eq!(rows[3][0], "❤️");
    assert_eq!(rows[4][0], "🌟");
}

/// Test that multi-byte emoji sequences are preserved.
///
/// **Validates: Requirement 33.2**
#[test]
fn test_complex_emoji_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with complex emoji (skin tones, ZWJ sequences)
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "emoji",
        vec![
            Value::string_owned("👨‍👩‍👧‍👦".to_string()), // Family emoji (ZWJ sequence)
            Value::string_owned("👍🏽".to_string()),        // Thumbs up with skin tone
            Value::string_owned("🏳️‍🌈".to_string()),      // Rainbow flag
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
    assert_eq!(rows[0][0], "👨‍👩‍👧‍👦");
    assert_eq!(rows[1][0], "👍🏽");
    assert_eq!(rows[2][0], "🏳️‍🌈");
}

/// Test that right-to-left (RTL) text is preserved.
///
/// **Validates: Requirement 33.3**
#[test]
fn test_rtl_text_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with RTL text (Arabic, Hebrew)
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("مرحبا".to_string()),      // Arabic: Hello
            Value::string_owned("שלום".to_string()),       // Hebrew: Hello
            Value::string_owned("مرحبا بك".to_string()),   // Arabic: Welcome
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
    assert_eq!(rows[0][0], "مرحبا");
    assert_eq!(rows[1][0], "שלום");
    assert_eq!(rows[2][0], "مرحبا بك");
}

/// Test that combining characters are preserved.
///
/// **Validates: Requirement 33.4**
#[test]
fn test_combining_characters_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with combining characters
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("é".to_string()),         // e + combining acute accent
            Value::string_owned("ñ".to_string()),         // n + combining tilde
            Value::string_owned("ü".to_string()),         // u + combining diaeresis
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
    assert_eq!(rows[0][0], "é");
    assert_eq!(rows[1][0], "ñ");
    assert_eq!(rows[2][0], "ü");
}

/// Test various Unicode scripts (CJK, Cyrillic, etc.).
///
/// **Validates: Requirement 33.1, 33.5**
#[test]
fn test_various_scripts_preservation() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with various scripts
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("日本語".to_string()),     // Japanese
            Value::string_owned("한국어".to_string()),     // Korean
            Value::string_owned("中文".to_string()),       // Chinese
            Value::string_owned("Русский".to_string()),   // Russian
            Value::string_owned("Ελληνικά".to_string()), // Greek
            Value::string_owned("ไทย".to_string()),       // Thai
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0][0], "日本語");
    assert_eq!(rows[1][0], "한국어");
    assert_eq!(rows[2][0], "中文");
    assert_eq!(rows[3][0], "Русский");
    assert_eq!(rows[4][0], "Ελληνικά");
    assert_eq!(rows[5][0], "ไทย");
}

/// Test mixed Unicode and ASCII content.
///
/// **Validates: Requirement 33.1**
#[test]
fn test_mixed_unicode_ascii() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with mixed content
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("Hello 世界".to_string()),
            Value::string_owned("Test 🎉 123".to_string()),
            Value::string_owned("مرحبا World".to_string()),
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
    assert_eq!(rows[0][0], "Hello 世界");
    assert_eq!(rows[1][0], "Test 🎉 123");
    assert_eq!(rows[2][0], "مرحبا World");
}

/// Test Unicode in column names (schema).
///
/// **Validates: Requirement 33.1**
#[test]
fn test_unicode_column_names() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with Unicode column names
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "名前",  // Japanese: name
        vec![
            Value::string_owned("太郎".to_string()),
            Value::string_owned("花子".to_string()),
        ],
    ));
    data.add_column(Column::new(
        "возраст",  // Russian: age
        vec![
            Value::Integer(25),
            Value::Integer(30),
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    
    // Verify schema preservation
    assert_eq!(parsed_doc.schema.len(), 2);
    assert_eq!(parsed_doc.schema[0], "名前");
    assert_eq!(parsed_doc.schema[1], "возраст");

    // Verify data preservation
    let rows = parser.expand(&parsed_doc).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "太郎");
    assert_eq!(rows[0][1], "25");
    assert_eq!(rows[1][0], "花子");
    assert_eq!(rows[1][1], "30");
}

/// Test Unicode with ALS operators (requires escaping).
///
/// **Validates: Requirement 33.1, 24.1**
#[test]
fn test_unicode_with_operators() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with Unicode and ALS operators
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("日本>語".to_string()),    // Contains >
            Value::string_owned("テスト*3".to_string()),   // Contains *
            Value::string_owned("🎉~🚀".to_string()),      // Contains ~
        ],
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Verify escaping occurred
    assert!(als_text.contains("\\>") || als_text.contains("\\*") || als_text.contains("\\~"));

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify preservation
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "日本>語");
    assert_eq!(rows[1][0], "テスト*3");
    assert_eq!(rows[2][0], "🎉~🚀");
}

/// Test byte-identical preservation for Unicode.
///
/// **Validates: Requirement 33.5**
#[test]
fn test_unicode_byte_identical() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Original strings with various Unicode
    let original_strings = vec![
        "Hello 世界 🎉",
        "مرحبا بك في العالم",
        "Привет мир",
        "👨‍👩‍👧‍👦",
        "é ñ ü",
    ];

    // Create data
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        original_strings
            .iter()
            .map(|s| Value::string_owned(s.to_string()))
            .collect(),
    ));

    // Compress to ALS
    let doc = compressor.compress(&data).unwrap();
    let als_text = als_compression::AlsSerializer::new().serialize(&doc);

    // Parse back
    let parsed_doc = parser.parse(&als_text).unwrap();
    let rows = parser.expand(&parsed_doc).unwrap();

    // Verify byte-identical preservation
    assert_eq!(rows.len(), original_strings.len());
    for (i, original) in original_strings.iter().enumerate() {
        assert_eq!(rows[i][0].as_bytes(), original.as_bytes());
    }
}

/// Test CSV round-trip with Unicode.
///
/// **Validates: Requirement 33.1, 1.1**
#[test]
fn test_csv_unicode_round_trip() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create CSV with Unicode
    let csv = "name,greeting\n太郎,こんにちは\nJohn,Hello\n花子,مرحبا";

    // Compress CSV to ALS
    let als = compressor.compress_csv(csv).unwrap();

    // Convert back to CSV
    let result_csv = parser.to_csv(&als).unwrap();

    // Parse both CSVs and compare
    let original_data = als_compression::convert::csv::parse_csv(csv).unwrap();
    let result_data = als_compression::convert::csv::parse_csv(&result_csv).unwrap();

    assert_eq!(original_data.row_count, result_data.row_count);
    assert_eq!(original_data.column_count(), result_data.column_count());

    // Verify Unicode preservation
    for row_idx in 0..original_data.row_count {
        for col_idx in 0..original_data.column_count() {
            let original_val = &original_data.columns[col_idx].values[row_idx];
            let result_val = &result_data.columns[col_idx].values[row_idx];
            assert_eq!(original_val, result_val);
        }
    }
}

/// Test JSON round-trip with Unicode.
///
/// **Validates: Requirement 33.1, 2.1**
#[test]
fn test_json_unicode_round_trip() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create JSON with Unicode
    let json = r#"[
        {"name": "太郎", "greeting": "こんにちは", "emoji": "🎉"},
        {"name": "John", "greeting": "Hello", "emoji": "👋"},
        {"name": "花子", "greeting": "مرحبا", "emoji": "🌟"}
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

    // Verify Unicode preservation
    for row_idx in 0..original_data.row_count {
        for col_idx in 0..original_data.column_count() {
            let original_val = &original_data.columns[col_idx].values[row_idx];
            let result_val = &result_data.columns[col_idx].values[row_idx];
            assert_eq!(original_val, result_val);
        }
    }
}

/// Test zero-width characters and special Unicode.
///
/// **Validates: Requirement 33.4**
#[test]
fn test_zero_width_characters() {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Create data with zero-width characters
    let mut data = TabularData::new();
    data.add_column(Column::new(
        "text",
        vec![
            Value::string_owned("hello\u{200B}world".to_string()), // Zero-width space
            Value::string_owned("test\u{200C}ing".to_string()),    // Zero-width non-joiner
            Value::string_owned("join\u{200D}ed".to_string()),     // Zero-width joiner
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
    assert_eq!(rows[0][0], "hello\u{200B}world");
    assert_eq!(rows[1][0], "test\u{200C}ing");
    assert_eq!(rows[2][0], "join\u{200D}ed");
}
