/**
 * @file als.h
 * @brief C API for the ALS (Adaptive Logic Stream) compression library
 *
 * This header provides a C-compatible interface for using the ALS compression
 * library from C and other languages with C FFI support.
 *
 * # Memory Management
 *
 * All strings returned by this API are allocated on the heap and must be freed
 * by calling als_string_free(). Failure to free strings will result in memory leaks.
 *
 * # Error Handling
 *
 * Functions return NULL pointers on error. Use als_get_last_error() to retrieve
 * the error message.
 *
 * # Thread Safety
 *
 * The compressor and parser objects are thread-safe and can be shared across threads.
 * However, you must ensure proper synchronization when accessing them from multiple threads.
 *
 * # Example
 *
 * @code
 * #include "als.h"
 * #include <stdio.h>
 * #include <stdlib.h>
 * #include <string.h>
 *
 * int main() {
 *     // Create compressor
 *     AlsCompressor* compressor = als_compressor_new();
 *     if (!compressor) {
 *         fprintf(stderr, "Failed to create compressor\n");
 *         return 1;
 *     }
 *
 *     // Compress CSV
 *     const char* csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
 *     char* als = als_compress_csv(compressor, csv, strlen(csv));
 *     if (!als) {
 *         char error[256];
 *         als_get_last_error(error, sizeof(error));
 *         fprintf(stderr, "Compression failed: %s\n", error);
 *         als_compressor_free(compressor);
 *         return 1;
 *     }
 *
 *     printf("Compressed: %s\n", als);
 *
 *     // Parse back to CSV
 *     AlsParser* parser = als_parser_new();
 *     char* csv_result = als_to_csv(parser, als, strlen(als));
 *     if (csv_result) {
 *         printf("Decompressed: %s\n", csv_result);
 *         als_string_free(csv_result);
 *     }
 *
 *     // Clean up
 *     als_string_free(als);
 *     als_compressor_free(compressor);
 *     als_parser_free(parser);
 *     return 0;
 * }
 * @endcode
 */

#ifndef ALS_H
#define ALS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

/**
 * @brief Opaque handle to an ALS compressor
 *
 * This type is opaque and should only be manipulated through the provided API functions.
 */
typedef struct AlsCompressorHandle AlsCompressor;

/**
 * @brief Opaque handle to an ALS parser
 *
 * This type is opaque and should only be manipulated through the provided API functions.
 */
typedef struct AlsParserHandle AlsParser;

/**
 * @brief Create a new ALS compressor with default configuration
 *
 * The compressor must be freed with als_compressor_free() when no longer needed.
 *
 * @return Pointer to the compressor, or NULL on failure
 */
AlsCompressor* als_compressor_new(void);

/**
 * @brief Create a new ALS compressor with custom configuration
 *
 * The compressor must be freed with als_compressor_free() when no longer needed.
 *
 * @param ctx_fallback_threshold Minimum compression ratio before falling back to CTX (e.g., 1.2)
 * @param min_pattern_length Minimum pattern length to consider (e.g., 3)
 * @param parallelism Number of threads for parallel processing (0 = auto)
 * @return Pointer to the compressor, or NULL on failure
 */
AlsCompressor* als_compressor_new_with_config(
    double ctx_fallback_threshold,
    size_t min_pattern_length,
    size_t parallelism
);

/**
 * @brief Free an ALS compressor
 *
 * After calling this function, the compressor pointer must not be used again.
 *
 * @param compressor Pointer to the compressor to free (must not be NULL)
 */
void als_compressor_free(AlsCompressor* compressor);

/**
 * @brief Compress CSV data to ALS format
 *
 * The returned string must be freed with als_string_free().
 *
 * @param compressor Pointer to an ALS compressor (must not be NULL)
 * @param input Pointer to CSV data (must not be NULL, must be valid UTF-8)
 * @param len Length of the input data in bytes (excluding null terminator)
 * @return Pointer to the compressed ALS string (null-terminated), or NULL on failure
 */
char* als_compress_csv(
    const AlsCompressor* compressor,
    const char* input,
    size_t len
);

/**
 * @brief Compress JSON data to ALS format
 *
 * The returned string must be freed with als_string_free().
 *
 * @param compressor Pointer to an ALS compressor (must not be NULL)
 * @param input Pointer to JSON data (must not be NULL, must be valid UTF-8)
 * @param len Length of the input data in bytes (excluding null terminator)
 * @return Pointer to the compressed ALS string (null-terminated), or NULL on failure
 */
char* als_compress_json(
    const AlsCompressor* compressor,
    const char* input,
    size_t len
);

/**
 * @brief Create a new ALS parser
 *
 * The parser must be freed with als_parser_free() when no longer needed.
 *
 * @return Pointer to the parser, or NULL on failure
 */
AlsParser* als_parser_new(void);

/**
 * @brief Free an ALS parser
 *
 * After calling this function, the parser pointer must not be used again.
 *
 * @param parser Pointer to the parser to free (must not be NULL)
 */
void als_parser_free(AlsParser* parser);

/**
 * @brief Parse ALS data and convert to CSV format
 *
 * The returned string must be freed with als_string_free().
 *
 * @param parser Pointer to an ALS parser (must not be NULL)
 * @param input Pointer to ALS data (must not be NULL, must be valid UTF-8)
 * @param len Length of the input data in bytes (excluding null terminator)
 * @return Pointer to the CSV string (null-terminated), or NULL on failure
 */
char* als_to_csv(
    const AlsParser* parser,
    const char* input,
    size_t len
);

/**
 * @brief Parse ALS data and convert to JSON format
 *
 * The returned string must be freed with als_string_free().
 *
 * @param parser Pointer to an ALS parser (must not be NULL)
 * @param input Pointer to ALS data (must not be NULL, must be valid UTF-8)
 * @param len Length of the input data in bytes (excluding null terminator)
 * @return Pointer to the JSON string (null-terminated), or NULL on failure
 */
char* als_to_json(
    const AlsParser* parser,
    const char* input,
    size_t len
);

/**
 * @brief Free a string returned by the ALS library
 *
 * After calling this function, the string pointer must not be used again.
 *
 * @param s Pointer to the string to free (must not be NULL)
 */
void als_string_free(char* s);

/**
 * @brief Get the last error message
 *
 * If no error occurred, the buffer is not modified and 0 is returned.
 * If the buffer is too small, the error message is truncated.
 *
 * @param buffer Pointer to a buffer to receive the error message (must not be NULL)
 * @param buffer_len Size of the buffer in bytes
 * @return 0 if no error occurred, or the length of the error message (including null terminator)
 */
int als_get_last_error(char* buffer, size_t buffer_len);

#ifdef __cplusplus
}
#endif

#endif /* ALS_H */
