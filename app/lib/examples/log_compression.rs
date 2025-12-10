//! Example demonstrating log file parsing and compression.
//!
//! Run with: cargo run --example log_compression --release

use als_compression::convert::syslog::parse_syslog;
use als_compression::convert::syslog_optimized::parse_syslog_optimized;
use als_compression::convert::log_compress::compress_syslog;
use als_compression::{AlsCompressor, AlsSerializer};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the linux.log file
    let log_content = fs::read_to_string("datasets/linux.log")?;
    let original_size = log_content.len();
    
    println!("=== Log File Compression Analysis ===\n");
    println!("Original file size: {} bytes", original_size);
    println!("Line count: {}", log_content.lines().count());
    
    // ============ BASIC PARSER ============
    println!("\n========== BASIC PARSER ==========");
    let start = std::time::Instant::now();
    let tabular_data = parse_syslog(&log_content)?;
    let parse_time = start.elapsed();
    
    println!("Parse time: {:?}", parse_time);
    println!("Rows: {}, Columns: {}", tabular_data.row_count, tabular_data.column_count());
    
    let compressor = AlsCompressor::new();
    let als_doc = compressor.compress(&tabular_data)?;
    let serializer = AlsSerializer::new();
    let als_output = serializer.serialize(&als_doc);
    let compressed_size = als_output.len();
    
    println!("Compressed size: {} bytes", compressed_size);
    println!("Compression ratio: {:.2}x", original_size as f64 / compressed_size as f64);
    
    // ============ OPTIMIZED PARSER ============
    println!("\n========== OPTIMIZED PARSER ==========");
    let start = std::time::Instant::now();
    let tabular_data_opt = parse_syslog_optimized(&log_content)?;
    let parse_time = start.elapsed();
    
    println!("Parse time: {:?}", parse_time);
    println!("Rows: {}, Columns: {}", tabular_data_opt.row_count, tabular_data_opt.column_count());
    println!("Column names: {:?}", tabular_data_opt.column_names());
    
    // Analyze column statistics
    println!("\n--- Column Analysis ---");
    for col in &tabular_data_opt.columns {
        let unique_count = count_unique(&col.values);
        let null_count = col.values.iter().filter(|v| v.is_null()).count();
        println!(
            "  {}: {} unique, {} nulls, {:?}",
            col.name, unique_count, null_count, col.inferred_type
        );
    }
    
    // Use a compressor with lower CTX threshold to force ALS format
    let config = als_compression::CompressorConfig::new()
        .with_ctx_fallback_threshold(1.0);  // Never fall back to CTX
    let compressor_opt = AlsCompressor::with_config(config);
    
    let start = std::time::Instant::now();
    let als_doc_opt = compressor_opt.compress(&tabular_data_opt)?;
    let compress_time = start.elapsed();
    
    let als_output_opt = serializer.serialize(&als_doc_opt);
    let compressed_size_opt = als_output_opt.len();
    
    println!("\nCompression time: {:?}", compress_time);
    println!("Compressed size: {} bytes", compressed_size_opt);
    println!("Compression ratio: {:.2}x", original_size as f64 / compressed_size_opt as f64);
    println!("Space savings: {:.1}%", (1.0 - compressed_size_opt as f64 / original_size as f64) * 100.0);
    
    // Show improvement
    println!("\n========== COMPARISON ==========");
    println!("Basic parser:     {:>6} bytes ({:.2}x)", compressed_size, original_size as f64 / compressed_size as f64);
    println!("Optimized parser: {:>6} bytes ({:.2}x)", compressed_size_opt, original_size as f64 / compressed_size_opt as f64);
    println!("Improvement: {:.1}%", (1.0 - compressed_size_opt as f64 / compressed_size as f64) * 100.0);
    
    // Show a sample of the compressed output
    println!("\n--- Sample of ALS compressed output (first 500 chars) ---");
    println!("{}", &als_output_opt[..als_output_opt.len().min(500)]);
    
    // ============ CUSTOM LOG COMPRESSION ============
    println!("\n========== CUSTOM LOG COMPRESSION ==========");
    let start = std::time::Instant::now();
    let (custom_compressed, custom_ratio) = compress_syslog(&log_content);
    let custom_time = start.elapsed();
    
    println!("Compression time: {:?}", custom_time);
    println!("Compressed size: {} bytes", custom_compressed.len());
    println!("Compression ratio: {:.2}x", custom_ratio);
    println!("Space savings: {:.1}%", (1.0 - 1.0/custom_ratio) * 100.0);
    
    // Final comparison
    println!("\n========== FINAL COMPARISON ==========");
    println!("Original:         {:>6} bytes", original_size);
    println!("Basic ALS:        {:>6} bytes ({:.2}x)", compressed_size, original_size as f64 / compressed_size as f64);
    println!("Optimized ALS:    {:>6} bytes ({:.2}x)", compressed_size_opt, original_size as f64 / compressed_size_opt as f64);
    println!("Custom Log:       {:>6} bytes ({:.2}x)", custom_compressed.len(), custom_ratio);
    
    println!("\n--- Sample of custom compressed output (first 1000 chars) ---");
    println!("{}", &custom_compressed[..custom_compressed.len().min(1000)]);
    
    Ok(())
}

fn count_unique(values: &[als_compression::Value]) -> usize {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for v in values {
        seen.insert(format!("{:?}", v));
    }
    seen.len()
}
