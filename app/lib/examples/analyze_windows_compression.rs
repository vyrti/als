//! Analyze windows.log compression in detail
//!
//! Run with: cargo run --example analyze_windows_compression --release

use als_compression::{AlsCompressor, AlsSerializer, convert::syslog_optimized::parse_syslog_optimized};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║   Windows.log Compression - Detailed Analysis              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let content = fs::read_to_string("datasets/windows.log")?;
    let original_size = content.len();
    let line_count = content.lines().count();

    println!("📊 File Info:");
    println!("   Size:  {} bytes ({:.2} KB)", original_size, original_size as f64 / 1024.0);
    println!("   Lines: {}", line_count);
    println!();

    // Parse
    println!("🔍 Parsing structure:");
    let tabular_data = parse_syslog_optimized(&content)?;
    println!("   Rows: {}", tabular_data.row_count);
    println!("   Columns: {}", tabular_data.column_count());
    println!();

    // Analyze each column
    println!("📋 Column Content Analysis:");
    println!("   {:<15} | {:<8} | {:<10} | {:<15}", "Column", "Type", "Non-Null", "Sample Values");
    println!("   {}", "─".repeat(60));
    
    for col in &tabular_data.columns {
        let non_null_count = col.values.iter().filter(|v| !v.is_null()).count();
        let first_non_null = col.values.iter()
            .find(|v| !v.is_null())
            .map(|v| format!("{:?}", v))
            .unwrap_or_default();
        
        let sample = if first_non_null.len() > 15 {
            format!("{}...", &first_non_null[..12])
        } else {
            first_non_null
        };
        
        println!("   {:<15} | {:<8} | {:<10} | {:<15}", 
            col.name,
            format!("{:?}", col.inferred_type),
            non_null_count,
            sample
        );
    }
    println!("   {}", "─".repeat(60));
    println!();

    // Compress
    println!("⚙️  Compressing...");
    let start = std::time::Instant::now();
    let compressor = AlsCompressor::new();
    let als_doc = compressor.compress(&tabular_data)?;
    let serializer = AlsSerializer::new();
    let als_output = serializer.serialize(&als_doc);
    let compress_time = start.elapsed();

    let compressed_size = als_output.len();
    let ratio = original_size as f64 / compressed_size as f64;
    let savings = (1.0 - compressed_size as f64 / original_size as f64) * 100.0;

    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  COMPRESSION RESULTS                       ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ Original:          {:>45} ║", format!("{} bytes", original_size));
    println!("║ Compressed:        {:>45} ║", format!("{} bytes", compressed_size));
    println!("║ Ratio:             {:>43.2}x ║", ratio);
    println!("║ Savings:           {:>42.2}% ║", savings);
    println!("║ Time:              {:>41.2} ms ║", compress_time.as_secs_f64() * 1000.0);
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ Note: High compression due to mostly null/empty fields     ║");
    println!("║ in parsed structure. Raw log data is mostly unstructured.  ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    println!("📝 Compressed Output Sample:");
    println!("   {}", &als_output[..als_output.len().min(200)]);

    Ok(())
}
