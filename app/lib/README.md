# ALS Compression Library

A high-performance Rust library for compressing structured data (CSV, JSON) using Adaptive Logic Stream (ALS) format.

## Features

- **Pattern-based compression**: Detects and encodes sequential ranges, repetitions, and alternating patterns
- **Multiple formats**: Supports CSV and JSON input/output
- **Zero-copy parsing**: Minimizes memory allocations using borrowed references
- **SIMD acceleration**: Uses AVX2, AVX-512, or NEON instructions when available
- **Parallel processing**: Leverages multiple CPU cores for large datasets
- **Cross-platform**: Works on macOS, Windows, and Linux
- **Thread-safe**: All public types implement `Send + Sync`
- **CTX fallback**: Automatically uses CTX compression when ALS provides insufficient compression

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
als-compression = "0.0.1"
```

## Quick Start

### Compression

```rust
use als_compression::AlsCompressor;

// Create a compressor with default settings
let compressor = AlsCompressor::new();

// Compress CSV data
let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
let als = compressor.compress_csv(csv)?;
println!("Compressed: {}", als);

// Compress JSON data
let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
let als = compressor.compress_json(json)?;
```

### Decompression

```rust
use als_compression::AlsParser;

// Create a parser
let parser = AlsParser::new();

// Parse ALS and convert to CSV
let als = "#id #name\n1>3|Alice Bob Charlie";
let csv = parser.to_csv(als)?;
println!("CSV: {}", csv);

// Parse ALS and convert to JSON
let json = parser.to_json(als)?;
println!("JSON: {}", json);
```

## Pattern Detection

The library automatically detects and compresses common patterns:

### Sequential Ranges

```rust
// Input CSV:
// id
// 1
// 2
// 3
// 4
// 5

// Compressed ALS:
// #id
// 1>5
```

### Repetitions

```rust
// Input CSV:
// status
// active
// active
// active

// Compressed ALS:
// #status
// active*3
```

### Alternating Patterns

```rust
// Input CSV:
// flag
// true
// false
// true
// false

// Compressed ALS:
// #flag
// true~false*4
```

### Dictionary Compression

```rust
// Input CSV:
// status
// active
// inactive
// active
// inactive
// pending

// Compressed ALS:
// $default:active|inactive|pending
// #status
// _0 _1 _0 _1 _2
```

## Configuration

Customize compression behavior:

```rust
use als_compression::{AlsCompressor, CompressorConfig};

let config = CompressorConfig::default()
    .with_ctx_fallback_threshold(1.5)  // CTX fallback threshold
    .with_min_pattern_length(4)        // Minimum pattern length
    .with_parallelism(4);              // Number of threads

let compressor = AlsCompressor::with_config(config);
```

## Streaming Large Files

Process large files without loading them entirely into memory:

```rust
use als_compression::{StreamingCompressor, StreamingParser};
use std::fs::File;
use std::io::BufReader;

// Stream compression
let file = File::open("large_data.csv")?;
let reader = BufReader::new(file);
let mut compressor = StreamingCompressor::new(reader);

for chunk in compressor.compress_chunks() {
    let als_chunk = chunk?;
    // Process chunk...
}

// Stream decompression
let file = File::open("compressed.als")?;
let reader = BufReader::new(file);
let mut parser = StreamingParser::new(reader);

for row in parser.parse_rows() {
    let values = row?;
    // Process row...
}
```

## Async Support

Integrate with async runtimes like Tokio:

```rust
use als_compression::{AlsCompressor, AlsParser};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compressor = AlsCompressor::new();
    let parser = AlsParser::new();

    // Async compression
    let csv = "id,name\n1,Alice\n2,Bob";
    let als = compressor.compress_csv_async(csv).await?;

    // Async decompression
    let csv_result = parser.to_csv_async(&als).await?;

    Ok(())
}
```

## Thread Safety

All public types are thread-safe and can be shared across threads:

```rust
use als_compression::AlsCompressor;
use std::sync::Arc;
use std::thread;

let compressor = Arc::new(AlsCompressor::new());

let handles: Vec<_> = (0..4).map(|i| {
    let compressor = Arc::clone(&compressor);
    thread::spawn(move || {
        let csv = format!("id,value\n{},{}", i, i * 10);
        compressor.compress_csv(&csv)
    })
}).collect();

for handle in handles {
    handle.join().unwrap().unwrap();
}
```

## Error Handling

The library uses Rust's `Result` type for error handling:

```rust
use als_compression::{AlsParser, AlsError};

let parser = AlsParser::new();
let result = parser.to_csv("invalid als format");

match result {
    Ok(csv) => println!("Success: {}", csv),
    Err(AlsError::AlsSyntaxError { position, message }) => {
        eprintln!("Syntax error at position {}: {}", position, message);
    }
    Err(AlsError::ColumnMismatch { schema, data }) => {
        eprintln!("Column mismatch: expected {}, got {}", schema, data);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Performance

The library is optimized for performance:

- **SIMD acceleration**: Automatically uses AVX2, AVX-512, or NEON instructions
- **Parallel processing**: Processes multiple columns concurrently on multi-core systems
- **Zero-copy parsing**: Minimizes memory allocations and copies
- **Adaptive data structures**: Automatically selects optimal data structures based on dataset size

## Features

Enable optional features in your `Cargo.toml`:

```toml
[dependencies]
als-compression = { version = "0.0.1", features = ["parallel", "simd"] }
```

Available features:
- `parallel`: Enable parallel processing with Rayon
- `simd`: Enable SIMD acceleration
- `python`: Python bindings via PyO3
- `ffi`: C FFI bindings
- `wasm`: WebAssembly support

## ALS Format

ALS (Adaptive Logic Stream) is a compression format that uses algorithmic descriptions to represent data patterns:

```
!v1                                    # Version header
$default:active|inactive|pending       # Dictionary header
#id #name #status                      # Schema
1>5|Alice*2 Bob*2 Charlie|_0 _1 _0 _1 _2  # Data streams
```

### Operators

- **Range**: `1>5` expands to `1, 2, 3, 4, 5`
- **Range with step**: `10>50:10` expands to `10, 20, 30, 40, 50`
- **Multiplier**: `value*3` expands to `value, value, value`
- **Toggle**: `A~B*4` expands to `A, B, A, B`
- **Dictionary reference**: `_0` references the first dictionary entry
- **Combined patterns**: `(1>3)*2` expands to `1, 2, 3, 1, 2, 3`

## License

See LICENSE file for details.

## Documentation

For detailed API documentation, run:

```bash
cargo doc --open
```
