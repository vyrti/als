/**
 * @file c_example.c
 * @brief Example usage of the ALS C FFI
 *
 * This example demonstrates how to use the ALS compression library from C.
 *
 * To compile and run:
 *   1. Build the library with FFI feature:
 *      cargo build --release --features ffi
 *
 *   2. Compile this example (Linux/macOS):
 *      gcc -o c_example examples/c_example.c \
 *          -I include \
 *          -L target/release \
 *          -lals_compression \
 *          -lpthread -ldl -lm
 *
 *   3. Run (Linux):
 *      LD_LIBRARY_PATH=target/release ./c_example
 *
 *   4. Run (macOS):
 *      DYLD_LIBRARY_PATH=target/release ./c_example
 */

#include "als.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void print_error(const char* context) {
    char error[512];
    int error_len = als_get_last_error(error, sizeof(error));
    if (error_len > 0) {
        fprintf(stderr, "%s: %s\n", context, error);
    } else {
        fprintf(stderr, "%s: Unknown error\n", context);
    }
}

int main(void) {
    printf("ALS Compression Library - C FFI Example\n");
    printf("========================================\n\n");

    // Create compressor with default configuration
    printf("Creating compressor...\n");
    AlsCompressor* compressor = als_compressor_new();
    if (!compressor) {
        print_error("Failed to create compressor");
        return 1;
    }
    printf("Compressor created successfully\n\n");

    // Example 1: Compress CSV
    printf("Example 1: CSV Compression\n");
    printf("--------------------------\n");
    const char* csv = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n4,David,28\n5,Eve,32";
    printf("Original CSV (%zu bytes):\n%s\n\n", strlen(csv), csv);

    char* als = als_compress_csv(compressor, csv, strlen(csv));
    if (!als) {
        print_error("CSV compression failed");
        als_compressor_free(compressor);
        return 1;
    }
    printf("Compressed ALS (%zu bytes):\n%s\n\n", strlen(als), als);

    // Create parser
    printf("Creating parser...\n");
    AlsParser* parser = als_parser_new();
    if (!parser) {
        print_error("Failed to create parser");
        als_string_free(als);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Parser created successfully\n\n");

    // Decompress back to CSV
    printf("Decompressing back to CSV...\n");
    char* csv_result = als_to_csv(parser, als, strlen(als));
    if (!csv_result) {
        print_error("CSV decompression failed");
        als_string_free(als);
        als_parser_free(parser);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Decompressed CSV:\n%s\n\n", csv_result);

    // Calculate compression ratio
    double ratio = (double)strlen(csv) / (double)strlen(als);
    printf("Compression ratio: %.2fx\n\n", ratio);

    // Clean up
    als_string_free(csv_result);
    als_string_free(als);

    // Example 2: Compress JSON
    printf("Example 2: JSON Compression\n");
    printf("---------------------------\n");
    const char* json = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"},{\"id\":3,\"name\":\"Charlie\"}]";
    printf("Original JSON (%zu bytes):\n%s\n\n", strlen(json), json);

    als = als_compress_json(compressor, json, strlen(json));
    if (!als) {
        print_error("JSON compression failed");
        als_parser_free(parser);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Compressed ALS (%zu bytes):\n%s\n\n", strlen(als), als);

    // Decompress back to JSON
    printf("Decompressing back to JSON...\n");
    char* json_result = als_to_json(parser, als, strlen(als));
    if (!json_result) {
        print_error("JSON decompression failed");
        als_string_free(als);
        als_parser_free(parser);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Decompressed JSON:\n%s\n\n", json_result);

    // Clean up
    als_string_free(json_result);
    als_string_free(als);

    // Example 3: Custom configuration
    printf("Example 3: Custom Configuration\n");
    printf("-------------------------------\n");
    printf("Creating compressor with custom config...\n");
    AlsCompressor* custom_compressor = als_compressor_new_with_config(
        1.5,  // ctx_fallback_threshold
        4,    // min_pattern_length
        2     // parallelism
    );
    if (!custom_compressor) {
        print_error("Failed to create custom compressor");
        als_parser_free(parser);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Custom compressor created successfully\n\n");

    const char* csv2 = "value\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10";
    printf("Original CSV:\n%s\n\n", csv2);

    als = als_compress_csv(custom_compressor, csv2, strlen(csv2));
    if (!als) {
        print_error("CSV compression with custom config failed");
        als_compressor_free(custom_compressor);
        als_parser_free(parser);
        als_compressor_free(compressor);
        return 1;
    }
    printf("Compressed ALS:\n%s\n\n", als);

    als_string_free(als);
    als_compressor_free(custom_compressor);

    // Clean up
    als_parser_free(parser);
    als_compressor_free(compressor);

    printf("All examples completed successfully!\n");
    return 0;
}
