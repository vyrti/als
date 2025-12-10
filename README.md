# ALS Compression Library

**Middleware for AI Prompts — Reduce LLM Token Costs**

Adaptive Logic Stream (ALS) compression library for structured data (CSV, JSON).

## Overview

ALS is a high-performance compression format designed as **middleware for AI applications** to reduce Large Language Model (LLM) token consumption and API costs. By compressing structured data through algorithmic pattern description rather than raw enumeration, ALS achieves superior compression ratios that significantly reduce the data payload sent to AI services. 

The library supports bidirectional conversion (CSV/JSON ↔ ALS), falls back to CTX compression when ALS compression ratio is insufficient, and leverages high-performance techniques including SIMD instructions, zero-copy operations, and concurrent data structures.

## Features

- **CSV & JSON Compression**: Convert CSV and JSON data to ALS format for superior compression
- **Pattern Detection**: Automatically detects and encodes patterns (ranges, repetitions, alternations)
- **CTX Fallback**: Automatically falls back to CTX compression when ALS provides insufficient compression
- **SIMD Optimization**: Leverages AVX2, AVX-512, and NEON instructions for maximum throughput
- **Parallel Processing**: Uses Rayon for multi-threaded compression and decompression
- **Zero-Copy Operations**: Minimizes memory allocations and copies using rkyv serialization
- **Thread-Safe**: Atomic operations and concurrent data structures for multi-threaded applications
- **Multiple Bindings**: Python (PyO3), C FFI, Go (CGO), WebAssembly, and Node.js support

## Use Case: AI Prompt Middleware

ALS is optimized as middleware for AI applications to minimize token costs:

- **Token Reduction**: Compress structured data by 50-90% before sending to LLMs, directly reducing token billing
- **Cost Savings**: Smaller payloads mean lower API costs for services like OpenAI, Anthropic, Google PaLM, and others
- **Decompression on Retrieval**: Decompress back to original format after receiving AI responses
- **Seamless Integration**: Works with any AI framework (LangChain, LlamaIndex, etc.) and LLM provider
- **Lossless Compression**: Maintains data integrity—compression is fully reversible with no information loss

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
als-compression = "0.0.1"
```

## Quick Start

```rust
use als_compression::AlsCompressor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compressor = AlsCompressor::new();
    
    // Compress CSV data to reduce tokens for AI APIs
    let csv_data = "col1,col2,col3\n1,a,x\n2,b,y\n3,c,z\n";
    let compressed = compressor.compress_csv(csv_data)?;
    println!("Compressed: {}", compressed);
    
    // Send compressed version to LLM instead of raw CSV
    // Decompress response when received back from AI service
    
    Ok(())
}
```

## Feature Flags

- `simd` (default): Enable SIMD optimizations
- `parallel` (default): Enable parallel processing with Rayon
- `python`: Build Python bindings with PyO3
- `ffi`: Build C FFI bindings
- `wasm`: Build WebAssembly bindings
- `async`: Enable async/await support with Tokio

## Building

### Standard Build

```bash
cargo build --release
```

### With Specific Features

```bash
# Disable SIMD
cargo build --release --no-default-features --features parallel

# Enable all features
cargo build --release --all-features

# Python bindings
cargo build --release --features python
```

## Testing

Run the test suite:

```bash
cargo test
```

Run property-based tests:

```bash
cargo test --test '*' -- --nocapture
```

Run benchmarks:

```bash
cargo bench
```

## Documentation

Build and view documentation:

```bash
cargo doc --open
```

## Performance

The library is optimized for:
- **Compression Ratio**: Superior compression for structured data with patterns
- **Throughput**: High MB/s compression and decompression rates
- **Memory Efficiency**: Minimal allocations and zero-copy deserialization
- **Latency**: Low-latency decompression with streaming support

## Architecture

The library consists of several key components:

- **Pattern Detection Engine**: Identifies sequential ranges, repetitions, and alternations
- **Dictionary Builder**: Optimizes string dictionary for compression benefit
- **SIMD Dispatcher**: Runtime CPU feature detection and SIMD implementation selection
- **Adaptive HashMap**: Automatically selects HashMap or DashMap based on dataset size
- **Streaming Support**: Process large files without loading entirely into memory

## Language Bindings

### C FFI

The library provides a C-compatible FFI for use from C and other languages with C interop:

```bash
# Build with FFI support
cd app/lib
cargo build --release --features ffi
```

See [C_FFI_README.md](app/lib/C_FFI_README.md) for detailed documentation and examples.

### Python

Python bindings are available via PyO3:

```bash
cargo build --release --features python
```

### Other Languages

- **Go**: CGO bindings (planned)
- **WebAssembly**: WASM bindings (planned)
- **Node.js**: Native addon (planned)

## Requirements

- Rust 1.70 or later
- For Python bindings: Python 3.8+
- For C FFI: GCC or Clang
- For WebAssembly: wasm-pack

## License

Licensed under the Apache-2.0. See LICENSE file for details.