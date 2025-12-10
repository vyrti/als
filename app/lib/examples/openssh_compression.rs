//! Test custom log compression on openssh.log
//!
//! Run with: cargo run --example openssh_compression --release

use als_compression::convert::log_compress::compress_syslog;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the openssh.log file
    let log_content = fs::read_to_string("datasets/openssh.log")?;
    let original_size = log_content.len();
    
    println!("=== OpenSSH Log Compression Test ===\n");
    println!("Original file size: {} bytes", original_size);
    println!("Line count: {}", log_content.lines().count());
    
    // Compress using custom log compression
    println!("\n--- Compressing ---");
    let start = std::time::Instant::now();
    let (compressed, ratio) = compress_syslog(&log_content);
    let compress_time = start.elapsed();
    
    println!("Compression time: {:?}", compress_time);
    println!("Compressed size: {} bytes", compressed.len());
    println!("Compression ratio: {:.2}x", ratio);
    println!("Space savings: {:.1}%", (1.0 - 1.0/ratio) * 100.0);
    
    // Show a sample of the compressed output
    println!("\n--- Sample of compressed output (first 1000 chars) ---");
    println!("{}", &compressed[..compressed.len().min(1000)]);
    
    Ok(())
}
