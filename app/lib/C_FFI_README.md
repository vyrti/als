# ALS Compression Library - C FFI

This document describes how to use the ALS compression library from C and other languages with C FFI support.

## Building

To build the library with C FFI support:

```bash
cd app/lib
cargo build --release --features ffi
```

This will produce a shared library in `target/release/`:
- Linux: `libals_compression.so`
- macOS: `libals_compression.dylib`
- Windows: `als_compression.dll`

## Header File

The C header file is located at `include/als.h`. Include this file in your C code:

```c
#include "als.h"
```

## API Overview

### Compressor Functions

- `AlsCompressor* als_compressor_new(void)` - Create a new compressor with default configuration
- `AlsCompressor* als_compressor_new_with_config(double, size_t, size_t)` - Create with custom config
- `void als_compressor_free(AlsCompressor*)` - Free a compressor
- `char* als_compress_csv(const AlsCompressor*, const char*, size_t)` - Compress CSV to ALS
- `char* als_compress_json(const AlsCompressor*, const char*, size_t)` - Compress JSON to ALS

### Parser Functions

- `AlsParser* als_parser_new(void)` - Create a new parser
- `void als_parser_free(AlsParser*)` - Free a parser
- `char* als_to_csv(const AlsParser*, const char*, size_t)` - Parse ALS to CSV
- `char* als_to_json(const AlsParser*, const char*, size_t)` - Parse ALS to JSON

### Utility Functions

- `void als_string_free(char*)` - Free a string returned by the library
- `int als_get_last_error(char*, size_t)` - Get the last error message

## Memory Management

**Important:** All strings returned by the library are allocated on the heap and must be freed using `als_string_free()`. Failure to free strings will result in memory leaks.

```c
char* als = als_compress_csv(compressor, csv, strlen(csv));
if (als) {
    // Use the string...
    als_string_free(als);  // Don't forget to free!
}
```

## Error Handling

Functions return `NULL` on error. Use `als_get_last_error()` to retrieve the error message:

```c
char* als = als_compress_csv(compressor, csv, strlen(csv));
if (!als) {
    char error[256];
    als_get_last_error(error, sizeof(error));
    fprintf(stderr, "Error: %s\n", error);
}
```

## Example Usage

### Basic CSV Compression

```c
#include "als.h"
#include <stdio.h>
#include <string.h>

int main() {
    // Create compressor
    AlsCompressor* compressor = als_compressor_new();
    if (!compressor) {
        fprintf(stderr, "Failed to create compressor\n");
        return 1;
    }

    // Compress CSV
    const char* csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
    char* als = als_compress_csv(compressor, csv, strlen(csv));
    if (!als) {
        char error[256];
        als_get_last_error(error, sizeof(error));
        fprintf(stderr, "Compression failed: %s\n", error);
        als_compressor_free(compressor);
        return 1;
    }

    printf("Compressed: %s\n", als);

    // Clean up
    als_string_free(als);
    als_compressor_free(compressor);
    return 0;
}
```

### Round-Trip Conversion

```c
#include "als.h"
#include <stdio.h>
#include <string.h>

int main() {
    AlsCompressor* compressor = als_compressor_new();
    AlsParser* parser = als_parser_new();

    const char* csv = "id,value\n1,100\n2,200\n3,300";
    
    // Compress
    char* als = als_compress_csv(compressor, csv, strlen(csv));
    if (!als) {
        fprintf(stderr, "Compression failed\n");
        goto cleanup;
    }
    
    // Decompress
    char* csv_result = als_to_csv(parser, als, strlen(als));
    if (!csv_result) {
        fprintf(stderr, "Decompression failed\n");
        als_string_free(als);
        goto cleanup;
    }
    
    printf("Original:\n%s\n\n", csv);
    printf("Compressed:\n%s\n\n", als);
    printf("Decompressed:\n%s\n", csv_result);
    
    als_string_free(csv_result);
    als_string_free(als);

cleanup:
    als_parser_free(parser);
    als_compressor_free(compressor);
    return 0;
}
```

### Custom Configuration

```c
#include "als.h"

int main() {
    // Create compressor with custom settings
    AlsCompressor* compressor = als_compressor_new_with_config(
        1.5,  // ctx_fallback_threshold - fall back to CTX if ratio < 1.5
        4,    // min_pattern_length - minimum pattern length to detect
        2     // parallelism - use 2 threads (0 = auto)
    );
    
    // Use the compressor...
    
    als_compressor_free(compressor);
    return 0;
}
```

## Compiling Your Code

### Linux

```bash
gcc -o myapp myapp.c \
    -I path/to/als-compression/include \
    -L path/to/als-compression/target/release \
    -lals_compression \
    -lpthread -ldl -lm

# Run with library path
LD_LIBRARY_PATH=path/to/als-compression/target/release ./myapp
```

### macOS

```bash
gcc -o myapp myapp.c \
    -I path/to/als-compression/include \
    -L path/to/als-compression/target/release \
    -lals_compression \
    -lpthread -ldl -lm

# Run with library path
DYLD_LIBRARY_PATH=path/to/als-compression/target/release ./myapp
```

### Windows (MSVC)

```cmd
cl myapp.c /I path\to\als-compression\include ^
    /link /LIBPATH:path\to\als-compression\target\release als_compression.lib

REM Run (ensure als_compression.dll is in PATH or same directory)
myapp.exe
```

## Thread Safety

The compressor and parser objects are thread-safe and can be shared across threads. However, you must ensure proper synchronization when accessing them from multiple threads using your platform's threading primitives (e.g., pthread mutexes on POSIX systems).

```c
#include <pthread.h>

AlsCompressor* compressor;  // Shared compressor
pthread_mutex_t mutex;

void* worker_thread(void* arg) {
    const char* csv = (const char*)arg;
    
    pthread_mutex_lock(&mutex);
    char* als = als_compress_csv(compressor, csv, strlen(csv));
    pthread_mutex_unlock(&mutex);
    
    if (als) {
        // Process result...
        als_string_free(als);
    }
    
    return NULL;
}
```

## Platform-Specific Notes

### Linux
- Link with `-lpthread -ldl -lm`
- Set `LD_LIBRARY_PATH` to include the library directory

### macOS
- Link with `-lpthread -ldl -lm`
- Set `DYLD_LIBRARY_PATH` to include the library directory
- On Apple Silicon, the library uses NEON SIMD instructions

### Windows
- Link with `als_compression.lib`
- Ensure `als_compression.dll` is in PATH or the same directory as your executable
- On x86_64, the library uses AVX2/AVX-512 SIMD instructions when available

## Example Program

A complete example is provided in `examples/c_example.c`. To build and run:

```bash
# Build the library
cargo build --release --features ffi

# Compile the example (Linux/macOS)
gcc -o c_example examples/c_example.c \
    -I include \
    -L target/release \
    -lals_compression \
    -lpthread -ldl -lm

# Run (Linux)
LD_LIBRARY_PATH=target/release ./c_example

# Run (macOS)
DYLD_LIBRARY_PATH=target/release ./c_example
```

## Troubleshooting

### Library Not Found

If you get "library not found" errors at runtime:
- **Linux**: Set `LD_LIBRARY_PATH` to include the directory containing `libals_compression.so`
- **macOS**: Set `DYLD_LIBRARY_PATH` to include the directory containing `libals_compression.dylib`
- **Windows**: Ensure `als_compression.dll` is in your PATH or the same directory as your executable

### Linking Errors

If you get linking errors:
- Ensure you're linking with the required system libraries: `-lpthread -ldl -lm` (Linux/macOS)
- Check that the library path (`-L`) is correct
- Verify the library was built with the `ffi` feature enabled

### Segmentation Faults

If you experience crashes:
- Ensure you're not using freed pointers
- Check that all strings are properly null-terminated
- Verify you're not passing NULL pointers to functions that don't accept them
- Make sure you're freeing all returned strings with `als_string_free()`

## API Reference

See `include/als.h` for complete API documentation with detailed parameter descriptions and safety requirements.
