# Implementation Plan

- [ ] 1. Project Setup and Core Infrastructure
  - [x] 1.1 Initialize Rust project with Cargo.toml and feature flags
    - Create `app/lib/Cargo.toml` with all dependencies (serde, csv, rkyv, dashmap, rayon, thiserror, proptest, criterion)
    - Configure feature flags: `simd`, `parallel`, `python`, `ffi`, `wasm`
    - Set up `cdylib` and `rlib` crate types
    - _Requirements: All_

  - [x] 1.2 Create error types module
    - Implement `AlsError` enum with thiserror derive
    - Include all error variants: CsvParseError, JsonParseError, AlsSyntaxError, InvalidDictRef, RangeOverflow, VersionMismatch, ColumnMismatch, IoError
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

  - [x] 1.3 Create configuration module
    - Implement `CompressorConfig` with all fields (ctx_fallback_threshold, hashmap_threshold, min_pattern_length, simd_config, parallelism)
    - Implement `ParserConfig` struct
    - Implement `SimdConfig` struct
    - Add security limits (max_range_expansion, max_dictionary_entries, max_input_size)
    - Implement `Default` trait with sensible defaults
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5_

- [-] 2. Core Data Structures
  - [x] 2.1 Implement ALS operator types
    - Create `AlsOperator` enum with Raw, Range, Multiply, Toggle, DictRef variants
    - Add rkyv derives for zero-copy serialization
    - Implement `range_safe` constructor with overflow checking
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 17.1, 32.9_

  - [ ]* 2.2 Write property test for Range operator correctness
    - **Property 4: Range Operator Correctness**
    - **Validates: Requirements 3.2, 17.1, 17.2, 17.3, 17.4**

  - [ ]* 2.3 Write property test for Multiplier operator correctness
    - **Property 5: Multiplier Operator Correctness**
    - **Validates: Requirements 3.3, 13.4**

  - [ ]* 2.4 Write property test for Toggle operator correctness
    - **Property 6: Toggle Operator Correctness**
    - **Validates: Requirements 3.5**

  - [x] 2.5 Implement ALS document structure
    - Create `AlsDocument` struct with version, dictionaries, schema, streams, format_indicator
    - Create `ColumnStream` struct
    - Create `FormatIndicator` enum (Als, Ctx)
    - _Requirements: 11.1, 22.1_

  - [x] 2.6 Implement tabular data model
    - Create `TabularData<'a>` with zero-copy support using `Cow<'a, str>`
    - Create `Column<'a>` struct with name, values, inferred_type
    - Create `Value<'a>` enum (Null, Integer, Float, String, Boolean)
    - Create `ColumnType` enum
    - _Requirements: 7.1, 7.4_

- [x] 3. Escape Sequence Handling
  - [x] 3.1 Implement escape/unescape functions
    - Create `escape_als_string()` function for all ALS operators
    - Create `unescape_als_string()` function
    - Define reserved tokens (NULL_TOKEN, EMPTY_TOKEN)
    - _Requirements: 24.1, 24.2, 24.3, 32.7_

  - [ ]* 3.2 Write property test for escape sequence preservation
    - **Property 14: Escape Sequence Preservation**
    - **Validates: Requirements 24.1, 24.2, 24.3, 24.4**

- [x] 5. ALS Parser Implementation
  - [x] 5.1 Implement ALS tokenizer
    - Create lexer for ALS format tokens
    - Handle version prefix (!v), dictionary ($), schema (#), operators
    - Support escape sequence parsing
    - _Requirements: 11.3_

  - [x] 5.2 Implement ALS parser core
    - Parse dictionary headers
    - Parse schema definitions
    - Parse column streams separated by |
    - Expand operators to values
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 11.3, 11.4, 11.5_

  - [ ]* 5.3 Write property test for dictionary reference resolution
    - **Property 7: Dictionary Reference Resolution**
    - **Validates: Requirements 3.4, 11.5**

  - [x] 5.4 Implement version detection and compatibility
    - Detect format version from input
    - Apply version-specific parsing rules
    - Return error for unknown versions
    - _Requirements: 22.2, 22.3, 22.4_

  - [ ]* 5.5 Write property test for version compatibility
    - **Property 21: Version Compatibility**
    - **Validates: Requirements 22.1, 22.2, 22.3**

- [x] 6. ALS Serializer Implementation
  - [x] 6.1 Implement ALS serializer core
    - Serialize version header
    - Serialize dictionary headers
    - Serialize schema
    - Serialize column streams with | separator
    - _Requirements: 11.1, 11.2, 22.1_

  - [x] 6.2 Implement pretty printer
    - Format ALS with visual separation
    - Add debug comments showing expanded values
    - _Requirements: 19.1, 19.2, 19.3_

  - [ ]* 6.3 Write property test for ALS format round-trip
    - **Property 3: ALS Format Round-Trip**
    - **Validates: Requirements 11.6, 19.4**

- [x] 8. Pattern Detection Engine
  - [x] 8.1 Implement pattern detector trait and infrastructure
    - Create `PatternDetector` trait
    - Create `DetectionResult` struct
    - Create `PatternType` enum
    - _Requirements: 10.5, 10.6_

  - [x] 8.2 Implement sequential range detection
    - Detect consecutive integers with step 1
    - Detect arithmetic sequences with custom step
    - Support descending sequences (negative step)
    - _Requirements: 1.2, 10.1, 17.1, 17.3, 17.4_

  - [ ]* 8.3 Write property test for sequential range detection
    - **Property 9: Sequential Range Detection**
    - **Validates: Requirements 1.2, 10.1**

  - [x] 8.4 Implement repetition detection
    - Detect consecutive identical values
    - Respect min_pattern_length configuration
    - _Requirements: 1.3, 10.2_

  - [ ]* 8.5 Write property test for repetition detection
    - **Property 10: Repetition Detection**
    - **Validates: Requirements 1.3, 10.2, 32.4**

  - [x] 8.6 Implement alternation/toggle detection
    - Detect alternating two-value patterns
    - Support toggle syntax generation
    - _Requirements: 1.4, 10.3_

  - [ ]* 8.7 Write property test for alternation detection
    - **Property 11: Alternation Detection**
    - **Validates: Requirements 1.4, 10.3**

  - [x] 8.8 Implement combined pattern detection
    - Detect repeated range patterns (1>3*2)
    - Detect repeated alternating patterns
    - _Requirements: 10.4, 13.1, 13.2, 13.3_

  - [ ]* 8.9 Write property test for combined pattern detection
    - **Property 12: Combined Pattern Detection**
    - **Validates: Requirements 10.4, 13.1, 13.2**

  - [x] 8.10 Implement pattern selection optimizer
    - Compare compression ratios of detected patterns
    - Select optimal encoding
    - Fall back to raw encoding when no benefit
    - _Requirements: 10.5, 10.6_

  - [ ]* 8.11 Write property test for pattern detection optimality
    - **Property 8: Pattern Detection Optimality**
    - **Validates: Requirements 10.5, 13.3**

- [x] 10. Dictionary Builder
  - [x] 10.1 Implement dictionary builder
    - Track string frequencies
    - Calculate compression benefit of dictionary references
    - Build optimal dictionary
    - _Requirements: 1.5, 14.1, 14.2, 14.3_

  - [x] 10.2 Implement enum/boolean detector
    - Detect columns with limited distinct values
    - Normalize boolean representations
    - Auto-create dictionaries for enum-like columns
    - _Requirements: 23.1, 23.2, 23.3, 23.4_

  - [ ]* 10.3 Write property test for dictionary benefit threshold
    - **Property 13: Dictionary Benefit Threshold**
    - **Validates: Requirements 14.1, 14.2, 14.3, 14.4**

- [x] 11. Adaptive HashMap
  - [x] 11.1 Implement AdaptiveMap
    - Create enum with Small(HashMap) and Large(DashMap) variants
    - Implement with_capacity_threshold constructor
    - Implement common map operations
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 12. Main Compressor
  - [x] 12.1 Implement AlsCompressor core
    - Create AlsCompressor struct with config and simd dispatcher
    - Implement compress() method for TabularData
    - Wire pattern detection and dictionary building
    - _Requirements: 1.1, 2.1_

  - [x] 12.2 Implement CTX fallback
    - Calculate ALS compression ratio
    - Fall back to CTX when below threshold
    - Set format indicator appropriately
    - _Requirements: 4.1, 4.2, 4.3_

  - [ ]* 12.3 Write property test for CTX fallback correctness
    - **Property 18: CTX Fallback Correctness**
    - **Validates: Requirements 4.2, 4.3, 4.4**

  - [x] 12.4 Implement compression statistics
    - Create CompressionStats with atomic counters
    - Track input/output bytes, patterns used
    - Report per-column effectiveness
    - _Requirements: 21.1, 21.2, 21.3, 21.4_

- [x] 14. CSV Conversion
  - [x] 14.1 Implement CSV parser
    - Parse CSV to TabularData
    - Infer column types
    - Handle edge cases (empty, single row/column)
    - _Requirements: 1.1, 32.1, 32.2, 32.3_

  - [x] 14.2 Implement CSV writer
    - Convert TabularData to CSV string
    - _Requirements: 3.1_

  - [x] 14.3 Wire CSV compression pipeline
    - Implement compress_csv() method
    - Implement to_csv() method on parser
    - _Requirements: 1.1, 3.1_

  - [ ]* 14.4 Write property test for CSV round-trip
    - **Property 1: CSV Round-Trip Equivalence**
    - **Validates: Requirements 1.1, 3.1, 3.6, 11.6**

- [x] 15. JSON Conversion
  - [x] 15.1 Implement JSON parser
    - Parse JSON array of objects to TabularData
    - Handle nested objects with dot-notation flattening
    - Handle null values
    - _Requirements: 2.1, 2.2, 2.4, 2.5_

  - [x] 15.2 Implement JSON writer
    - Convert TabularData to JSON array
    - Reconstruct nested objects from dot-notation
    - _Requirements: 3.1_

  - [x] 15.3 Wire JSON compression pipeline
    - Implement compress_json() method
    - Implement to_json() method on parser
    - _Requirements: 2.1, 3.1_

  - [ ]* 15.4 Write property test for JSON round-trip
    - **Property 2: JSON Round-Trip Equivalence**
    - **Validates: Requirements 2.1, 3.1, 3.6, 11.6**

  - [ ]* 15.5 Write property test for null value handling
    - **Property 22: Null Value Handling**
    - **Validates: Requirements 2.5**

  - [ ]* 15.6 Write property test for nested JSON flattening
    - **Property 23: Nested JSON Flattening**
    - **Validates: Requirements 2.4**

- [-] 17. Unicode and Special Character Support
  - [x] 17.1 Implement Unicode handling
    - Ensure UTF-8 preservation throughout pipeline
    - Handle emoji, RTL text, combining characters
    - _Requirements: 33.1, 33.2, 33.3, 33.4_

  - [ ]* 17.2 Write property test for Unicode preservation
    - **Property 15: Unicode Preservation**
    - **Validates: Requirements 33.1, 33.2, 33.3, 33.4, 33.5**

  - [ ]* 17.3 Write property test for whitespace preservation
    - **Property 16: Whitespace Preservation**
    - **Validates: Requirements 32.8**

  - [x] 17.4 Implement floating point handling
    - Preserve precision in round-trip
    - Detect floating point patterns
    - _Requirements: 18.1, 18.2, 18.3_

  - [ ]* 17.5 Write property test for floating point precision
    - **Property 17: Floating Point Precision**
    - **Validates: Requirements 18.1, 18.2, 18.3, 18.4**

- [x] 18. SIMD Implementation
  - [x] 18.1 Implement SIMD dispatcher
    - Create SimdDispatcher with runtime CPU detection
    - Create CpuFeatures struct
    - _Requirements: 6.5_

  - [x] 18.2 Implement scalar fallback
    - Create scalar implementations for all SIMD operations
    - expand_range, find_runs
    - _Requirements: 6.4_

  - [x] 18.3 Implement AVX2 operations
    - Range expansion with AVX2
    - Pattern detection with AVX2
    - _Requirements: 6.1_

  - [x] 18.4 Implement AVX-512 operations
    - Range expansion with AVX-512
    - Pattern detection with AVX-512
    - _Requirements: 6.2_

  - [x] 18.5 Implement NEON operations (ARM64)
    - Range expansion with NEON
    - Pattern detection with NEON
    - _Requirements: 6.3_

- [x] 19. Parallel Processing
  - [x] 19.1 Implement parallel compression
    - Use Rayon for column-parallel compression
    - Implement size threshold for parallel vs sequential
    - _Requirements: 20.1, 20.2, 20.3, 20.4_

  - [x] 19.2 Implement parallel decompression
    - Use Rayon for column-parallel expansion
    - Maintain correct column alignment during zip
    - _Requirements: 25.1, 25.2, 25.3, 25.4_

  - [ ]* 19.3 Write property test for parallel processing equivalence
    - **Property 19: Parallel Processing Equivalence**
    - **Validates: Requirements 20.1, 20.2, 25.1, 25.2, 31.1, 31.2**

- [x] 20. Thread Safety and Atomics
  - [x] 20.1 Implement thread-safe statistics
    - Use atomic operations for all counters
    - Ensure lock-free reads where possible
    - _Requirements: 8.1, 8.2_

  - [x] 20.2 Ensure API thread safety
    - Verify Send + Sync bounds on public types
    - Document thread safety guarantees
    - _Requirements: 8.3, 8.4_

  - [ ]* 20.3 Write property test for concurrent access correctness
    - **Property 20: Concurrent Access Correctness**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4, 5.4**

- [x] 22. Streaming Support
  - [x] 22.1 Implement StreamingCompressor
    - Process input in chunks
    - Yield ALS fragments
    - _Requirements: 15.1, 15.2, 15.3_

  - [x] 22.2 Implement StreamingParser
    - Parse ALS in streaming fashion
    - Yield rows incrementally
    - _Requirements: 15.4_

- [x] 23. Async Support
  - [x] 23.1 Implement async compression
    - Create compress_csv_async, compress_json_async
    - Integrate with Tokio runtime
    - _Requirements: 31.1, 31.3_

  - [x] 23.2 Implement async decompression
    - Create async parser methods
    - Support cancellation
    - _Requirements: 31.2, 31.4, 31.5_

- [x] 24. Public API Finalization
  - [x] 24.1 Create lib.rs with public exports
    - Export AlsCompressor, AlsParser
    - Export configuration types
    - Export error types
    - Export streaming types
    - _Requirements: All_

  - [x] 24.2 Implement AlsParser public methods
    - Implement to_csv() method
    - Implement to_json() method
    - Implement parse() method returning TabularData
    - _Requirements: 3.1, 4.4_

  - [x] 24.3 Add documentation
    - Document all public APIs
    - Add usage examples
    - _Requirements: All_

- [ ] 26. Benchmark Suite
  - [ ] 26.1 Implement compression benchmarks
    - Benchmark CSV compression at various sizes (100, 1000, 10000, 100000 rows)
    - Benchmark JSON compression
    - Measure throughput (MB/s)
    - _Requirements: 9.1, 9.2, 9.4, 35.1, 35.5_

  - [ ] 26.2 Implement decompression benchmarks
    - Benchmark ALS parsing
    - Measure latency percentiles (p50, p95, p99)
    - _Requirements: 9.2, 35.2_

  - [ ] 26.6 Implement memory benchmarks
    - Measure memory allocation counts
    - Measure peak memory usage
    - _Requirements: 35.6_

- [ ] 27. Edge Case Tests
  - [ ] 27.1 Implement edge case test suite
    - Test empty input
    - Test single row/column
    - Test integer boundaries
    - Test overflow scenarios
    - Test all unique values (no dictionary)
    - Test empty strings
    - Test invalid dictionary references
    - _Requirements: 32.1, 32.2, 32.3, 32.5, 32.6, 32.7, 32.9, 32.10, 34.7_

- [ ] 28. Fuzz Testing
  - [ ] 28.1 Set up fuzz testing infrastructure
    - Configure cargo-fuzz or similar
    - Create fuzz targets for ALS parser
    - Create fuzz targets for CSV parser
    - Create fuzz targets for JSON parser
    - _Requirements: 34.6_

- [ ] 29. Integration Tests
  - [ ] 29.1 Implement integration test suite
    - Test full CSV→ALS→CSV pipeline
    - Test full JSON→ALS→JSON pipeline
    - Test CTX fallback scenarios
    - Test with real-world sample data
    - _Requirements: 34.3, 34.4_

  - [ ] 29.2 Implement cross-platform tests
    - Test line ending normalization (CRLF/LF handling)
    - Test path separator handling
    - Verify identical behavior across platforms
    - _Requirements: 36.4, 36.5, 36.7_

- [x] 30. Python Bindings (PyO3)
  - [x] 30.1 Implement Python bindings
    - Create PyAlsCompressor class
    - Create PyAlsParser class
    - Implement compress_csv, compress_json methods
    - _Requirements: 26.1, 26.2, 26.3, 26.4_

  - [ ] 30.2 Add DataFrame support
    - Implement compress_dataframe method
    - Support numpy array conversion
    - _Requirements: 26.5, 26.6_

- [x] 31. C FFI Bindings
  - [x] 31.1 Implement C FFI
    - Create C-compatible functions
    - Generate header file
    - Implement memory management functions
    - _Requirements: 27.1, 27.2, 27.3, 27.4, 27.5_

- [ ] 32. Go Bindings
  - [ ] 32.1 Implement Go bindings
    - Create CGO wrapper
    - Implement Go package
    - _Requirements: 28.1, 28.2, 28.3, 28.4, 28.5_

- [ ] 33. WebAssembly Support
  - [ ] 33.1 Implement WASM bindings
    - Create wasm-bindgen exports
    - Implement JavaScript-friendly API
    - Enable WASM SIMD when available
    - _Requirements: 29.1, 29.2, 29.3, 29.4, 29.5_

- [ ] 34. Node.js Bindings
  - [ ] 34.1 Implement Node.js native addon
    - Create N-API bindings
    - Support Buffer objects
    - Implement stream interfaces
    - _Requirements: 30.1, 30.2, 30.3, 30.4, 30.5_

- [-] 35. CLI Application Setup
  - [x] 35.1 Create CLI project structure
    - Create `app/cli` directory
    - Create `app/cli/Cargo.toml` with clap dependency and workspace configuration
    - Add CLI to workspace members in root Cargo.toml
    - Create `app/cli/src/main.rs` with basic structure
    - _Requirements: All (CLI interface to library)_

  - [x] 35.2 Implement command-line argument parsing
    - Define CLI structure with clap derive macros
    - Add subcommands: compress, decompress, info
    - Add global options: --verbose, --quiet, --config
    - Add format options: --format (csv/json/als)
    - Add input/output options: --input, --output (support stdin/stdout)
    - _Requirements: 1.1, 2.1, 3.1_

  - [x] 35.3 Implement compress subcommand
    - Parse compress command arguments
    - Read input from file or stdin
    - Detect input format (CSV/JSON) or use --format flag
    - Call library compression functions
    - Write output to file or stdout
    - Handle errors and display user-friendly messages
    - _Requirements: 1.1, 2.1_

  - [x] 35.4 Implement decompress subcommand
    - Parse decompress command arguments
    - Read ALS input from file or stdin
    - Specify output format (CSV/JSON) via --format flag
    - Call library decompression functions
    - Write output to file or stdout
    - Handle errors and display user-friendly messages
    - _Requirements: 3.1, 4.4_

  - [x] 35.5 Implement info subcommand
    - Parse info command arguments
    - Read ALS input from file or stdin
    - Display compression statistics (ratio, patterns used, etc.)
    - Display document metadata (version, columns, rows)
    - Show dictionary contents if present
    - _Requirements: 21.1, 21.2, 21.3, 21.4, 22.1_

  - [ ] 35.6 Add configuration file support
    - Support loading config from file (TOML/JSON)
    - Allow overriding config with CLI flags
    - Implement --config flag to specify config file path
    - Map config file options to CompressorConfig
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 16.5_

  - [x] 35.7 Add progress reporting and logging
    - Implement progress bar for large files
    - Add --verbose flag for detailed logging
    - Add --quiet flag to suppress non-error output
    - Use appropriate logging levels (info, warn, error)
    - Display timing information for operations
    - _Requirements: 9.1, 9.2_

  - [ ] 35.8 Add batch processing support
    - Support multiple input files
    - Add --recursive flag for directory processing
    - Add --pattern flag for glob pattern matching
    - Preserve directory structure in output
    - Display summary statistics for batch operations
    - _Requirements: 15.1, 15.2_

  - [ ]* 35.9 Write integration tests for CLI
    - Test compress command with CSV input
    - Test compress command with JSON input
    - Test decompress command with ALS input
    - Test info command output
    - Test stdin/stdout piping
    - Test error handling and exit codes
    - Test batch processing
    - _Requirements: 34.3, 34.4_

  - [ ] 35.10 Add CLI documentation
    - Write comprehensive help text for all commands
    - Add examples to help output
    - Create README.md in app/cli with usage examples
    - Document all flags and options
    - Add man page generation (optional)
    - _Requirements: All_