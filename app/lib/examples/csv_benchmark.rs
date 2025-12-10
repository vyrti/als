//! Benchmark CSV compression on real datasets
//!
//! Run with: cargo run --example csv_benchmark --release

use als_compression::{AlsCompressor, AlsSerializer};
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║       ALS Compression - CSV Dataset Benchmark               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Automatically discover all CSV files in datasets directory
    let mut datasets = vec![];
    if let Ok(entries) = fs::read_dir("datasets") {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "csv") {
                    if let Some(path_str) = path.to_str() {
                        datasets.push(path_str.to_string());
                    }
                }
            }
        }
    }
    
    datasets.sort();

    let mut total_original = 0;
    let mut total_compressed = 0;

    for dataset in datasets {
        let name = Path::new(&dataset).file_name().unwrap().to_str().unwrap();
        print!("📊 {:<30} ", name);
        
        if !Path::new(&dataset).exists() {
            println!("⚠️  File not found");
            continue;
        }

        if let Ok(content) = fs::read_to_string(&dataset) {
            let original_size = content.len();
            let line_count = content.lines().count();

            if let Ok(compressor) = (|| -> Result<_, Box<dyn std::error::Error>> {
                let start = std::time::Instant::now();
                
                let compressor = AlsCompressor::new();
                let csv_data = als_compression::convert::csv::parse_csv(&content)?;
                let als_doc = compressor.compress(&csv_data)?;
                let serializer = AlsSerializer::new();
                let als_output = serializer.serialize(&als_doc);
                
                let elapsed = start.elapsed();
                let compressed_size = als_output.len();
                let ratio = original_size as f64 / compressed_size as f64;
                let savings = (1.0 - compressed_size as f64 / original_size as f64) * 100.0;

                total_original += original_size;
                total_compressed += compressed_size;

                println!("{:>10} → {:>10} ({:>6.2}x, {:>5.1}% saved, {:>6.2}ms)",
                    format_bytes(original_size),
                    format_bytes(compressed_size),
                    ratio,
                    savings,
                    elapsed.as_secs_f64() * 1000.0
                );

                Ok(())
            })() {
                let _ = compressor;
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

fn count_unique(values: &[String]) -> usize {
    use std::collections::HashSet;
    values.iter().collect::<HashSet<_>>().len()
}
