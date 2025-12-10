//! Example demonstrating async compression and decompression.
//!
//! This example shows how to use the async APIs for compression and
//! decompression with the Tokio runtime.
//!
//! Run with: cargo run --example async_usage --features async

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use als_compression::{AlsCompressor, AlsParser};

    println!("=== Async ALS Compression Example ===\n");

    // Create compressor and parser
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Example 1: Async CSV compression
    println!("1. Compressing CSV asynchronously...");
    let csv = "id,name,status\n1,Alice,active\n2,Bob,active\n3,Charlie,inactive\n4,David,active\n5,Eve,active";
    let als = compressor.compress_csv_async(csv).await?;
    println!("   Original CSV: {} bytes", csv.len());
    println!("   Compressed ALS: {} bytes", als.len());
    println!("   ALS output:\n{}\n", als);

    // Example 2: Async CSV decompression
    println!("2. Decompressing ALS to CSV asynchronously...");
    let result_csv = parser.to_csv_async(&als).await?;
    println!("   Decompressed CSV:\n{}\n", result_csv);

    // Example 3: Async JSON compression
    println!("3. Compressing JSON asynchronously...");
    let json = r#"[
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]"#;
    let als_json = compressor.compress_json_async(json).await?;
    println!("   Original JSON: {} bytes", json.len());
    println!("   Compressed ALS: {} bytes", als_json.len());
    println!("   ALS output:\n{}\n", als_json);

    // Example 4: Async JSON decompression
    println!("4. Decompressing ALS to JSON asynchronously...");
    let result_json = parser.to_json_async(&als_json).await?;
    println!("   Decompressed JSON:\n{}\n", result_json);

    // Example 5: Concurrent compression
    println!("5. Compressing multiple datasets concurrently...");
    let datasets = vec![
        "id,value\n1,100\n2,200\n3,300",
        "id,value\n10,1000\n20,2000\n30,3000",
        "id,value\n100,10000\n200,20000\n300,30000",
    ];

    let mut handles = vec![];
    for (i, csv) in datasets.iter().enumerate() {
        let compressor = compressor.clone();
        let csv = csv.to_string();
        let handle = tokio::spawn(async move {
            let als = compressor.compress_csv_async(&csv).await?;
            Ok::<_, als_compression::AlsError>((i, als.len()))
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await?;
        let (i, size) = result?;
        println!("   Dataset {} compressed to {} bytes", i + 1, size);
    }

    println!("\n=== Example Complete ===");

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    eprintln!("This example requires the 'async' feature to be enabled.");
    eprintln!("Run with: cargo run --example async_usage --features async");
    std::process::exit(1);
}
