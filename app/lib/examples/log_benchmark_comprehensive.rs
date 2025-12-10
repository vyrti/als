//! Benchmark log compression on real log files
//!
//! Run with: cargo run --example log_benchmark --release

use als_compression::{AlsCompressor, AlsSerializer, convert::syslog_optimized::parse_syslog_optimized};
use std::fs;
use std::path::Path;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║        ALS Compression - Log File Benchmark                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Automatically discover all log files in datasets directory
    let mut logfiles = vec![];
    if let Ok(entries) = fs::read_dir("datasets") {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "log") {
                    if let Some(path_str) = path.to_str() {
                        logfiles.push(path_str.to_string());
                    }
                }
            }
        }
    }
    
    logfiles.sort();

    let mut total_original = 0;
    let mut total_compressed = 0;
    let mut total_lines = 0;

    for logfile in logfiles {
        let name = Path::new(&logfile).file_name().unwrap().to_str().unwrap();
        print!("📋 {:<30} ", name);
        
        if !Path::new(&logfile).exists() {
            println!("⚠️  File not found");
            continue;
        }

        if let Ok(content) = fs::read_to_string(&logfile) {
            let original_size = content.len();
            let line_count = content.lines().count();

            let start = Instant::now();
            
            match (|| -> Result<_, Box<dyn std::error::Error>> {
                // Parse as structured syslog format
                let tabular_data = parse_syslog_optimized(&content)?;
                
                // Compress to ALS
                let compressor = AlsCompressor::new();
                let als_doc = compressor.compress(&tabular_data)?;
                let serializer = AlsSerializer::new();
                let als_output = serializer.serialize(&als_doc);
                
                Ok((als_output, tabular_data.row_count))
            })() {
                Ok((als_output, rows)) => {
                    let elapsed = start.elapsed();
                    let compressed_size = als_output.len();
                    let ratio = original_size as f64 / compressed_size as f64;
                    let savings = (1.0 - compressed_size as f64 / original_size as f64) * 100.0;

                    total_original += original_size;
                    total_compressed += compressed_size;
                    total_lines += line_count;

                    println!("{:>10} → {:>10} ({:>6.2}x, {:>5.1}% saved, {:>6} lines, {:>6.2}ms)",
                        format_bytes(original_size),
                        format_bytes(compressed_size),
                        ratio,
                        savings,
                        rows,
                        elapsed.as_secs_f64() * 1000.0
                    );
                }
                Err(_) => {
                    println!("❌ Compression failed");
                }
            }
        }
    }

    if total_original > 0 {
        let overall_ratio = total_original as f64 / total_compressed as f64;
        let overall_savings = (1.0 - total_compressed as f64 / total_original as f64) * 100.0;
        
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║                    Overall Statistics                       ║");
        println!("╠════════════════════════════════════════════════════════════╣");
        println!("║ Total Original:      {:>42} ║", format_bytes(total_original));
        println!("║ Total Compressed:    {:>42} ║", format_bytes(total_compressed));
        println!("║ Total Lines:         {:>42} ║", total_lines);
        println!("║ Overall Ratio:       {:>38.2}x ║", overall_ratio);
        println!("║ Overall Savings:     {:>37.1}% ║", overall_savings);
        println!("╚════════════════════════════════════════════════════════════╝");
    }

    Ok(())
}

fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}
