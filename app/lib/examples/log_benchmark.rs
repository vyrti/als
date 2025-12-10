//! Benchmark custom log compression on multiple log files
//!
//! Run with: cargo run --example log_benchmark --release

use als_compression::convert::log_compress::compress_syslog;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Log Compression Benchmark ===\n");
    
    let files = vec![
        ("datasets/linux.log", "Linux System Log"),
        ("datasets/openssh.log", "OpenSSH Log"),
    ];
    
    let mut total_original = 0;
    let mut total_compressed = 0;
    
    for (path, name) in &files {
        match fs::read_to_string(path) {
            Ok(content) => {
                let original_size = content.len();
                let line_count = content.lines().filter(|l| !l.trim().is_empty()).count();
                
                let start = std::time::Instant::now();
                let (compressed, ratio) = compress_syslog(&content);
                let time = start.elapsed();
                
                total_original += original_size;
                total_compressed += compressed.len();
                
                println!("--- {} ---", name);
                println!("  Lines:       {:>6}", line_count);
                println!("  Original:    {:>6} bytes", original_size);
                println!("  Compressed:  {:>6} bytes", compressed.len());
                println!("  Ratio:       {:>6.2}x", ratio);
                println!("  Savings:     {:>6.1}%", (1.0 - 1.0/ratio) * 100.0);
                println!("  Time:        {:>6.2}ms", time.as_secs_f64() * 1000.0);
                println!();
            }
            Err(e) => {
                println!("--- {} ---", name);
                println!("  Error: {}", e);
                println!();
            }
        }
    }
    
    if total_original > 0 {
        let overall_ratio = total_original as f64 / total_compressed as f64;
        println!("=== Overall Statistics ===");
        println!("  Total Original:    {:>8} bytes", total_original);
        println!("  Total Compressed:  {:>8} bytes", total_compressed);
        println!("  Overall Ratio:     {:>8.2}x", overall_ratio);
        println!("  Overall Savings:   {:>8.1}%", (1.0 - 1.0/overall_ratio) * 100.0);
    }
    
    Ok(())
}
