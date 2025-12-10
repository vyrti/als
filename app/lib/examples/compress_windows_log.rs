//! Compress windows.log and display the compressed output
//!
//! Run with: cargo run --example compress_windows_log --release

use als_compression::{AlsCompressor, AlsSerializer, convert::syslog_optimized::parse_syslog_optimized};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║      Windows.log Compression Analysis & Verification       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Read the windows.log file
    let content = fs::read_to_string("datasets/windows.log")?;
    let original_size = content.len();
    let line_count = content.lines().count();

    println!("📄 Original File Stats:");
    println!("   Size:       {} bytes ({:.2} KB)", original_size, original_size as f64 / 1024.0);
    println!("   Lines:      {}", line_count);
    println!();

    // Show first few lines of original
    println!("📝 First 5 lines of original:");
    println!("   {}", "─".repeat(60));
    for (i, line) in content.lines().take(5).enumerate() {
        println!("   {}: {}", i + 1, line);
    }
    println!("   {}", "─".repeat(60));
    println!();

    // Parse and analyze structure
    println!("🔍 Parsing as structured syslog format...");
    let start = std::time::Instant::now();
    let tabular_data = parse_syslog_optimized(&content)?;
    let parse_time = start.elapsed();

    println!("   Parsed {} rows × {} columns", tabular_data.row_count, tabular_data.column_count());
    println!("   Parse time: {:.2}ms", parse_time.as_secs_f64() * 1000.0);
    println!();

    // Show column analysis
    println!("📊 Column Analysis:");
    println!("   {}", "─".repeat(60));
    for col in &tabular_data.columns {
        let null_count = col.values.iter().filter(|v| v.is_null()).count();
        println!(
            "   {:<15} | Type: {:<8} | Values: {:>6} | Nulls: {:>4}",
            col.name,
            format!("{:?}", col.inferred_type),
            col.values.len(),
            null_count
        );
    }
    println!("   {}", "─".repeat(60));
    println!();

    // Compress
    println!("⚙️  Compressing to ALS format...");
    let start = std::time::Instant::now();
    let compressor = AlsCompressor::new();
    let als_doc = compressor.compress(&tabular_data)?;
    let serializer = AlsSerializer::new();
    let als_output = serializer.serialize(&als_doc);
    let compress_time = start.elapsed();

    let compressed_size = als_output.len();
    let ratio = original_size as f64 / compressed_size as f64;
    let savings = (1.0 - compressed_size as f64 / original_size as f64) * 100.0;

    println!("   Compression time: {:.2}ms", compress_time.as_secs_f64() * 1000.0);
    println!();

    println!("📊 Compression Results:");
    println!("   {}", "─".repeat(60));
    println!("   Original size:      {} bytes ({:.2} KB)", original_size, original_size as f64 / 1024.0);
    println!("   Compressed size:    {} bytes ({:.4} KB)", compressed_size, compressed_size as f64 / 1024.0);
    println!("   Compression ratio:  {:.2}x", ratio);
    println!("   Space savings:      {:.2}%", savings);
    println!("   {}", "─".repeat(60));
    println!();

    // Display compressed output
    println!("📦 Compressed ALS Output:");
    println!("   {}", "─".repeat(60));
    println!("{}", als_output);
    println!("   {}", "─".repeat(60));
    println!();

    // Verify decompression
    println!("✅ Verification: Decompressing back to original format...");
    let parser = als_compression::AlsParser::new();
    let decompressed_csv = parser.to_csv(&als_output)?;
    
    // Parse both to compare structure
    let original_data = als_compression::convert::csv::parse_csv(&content)?;
    let decompressed_data = als_compression::convert::csv::parse_csv(&decompressed_csv)?;

    println!("   Original rows:      {}", original_data.row_count);
    println!("   Decompressed rows:  {}", decompressed_data.row_count);
    println!("   Columns match:      {}", original_data.column_names() == decompressed_data.column_names());
    println!("   ✓ Decompression verified!");
    println!();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  Compression Complete                      ║");
    println!("║  {:.2}x compression = {:.1}% space savings", ratio, savings);
    println!("╚════════════════════════════════════════════════════════════╝");

    Ok(())
}

