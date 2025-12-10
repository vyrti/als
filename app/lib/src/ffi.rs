//! C FFI bindings for the ALS compression library.
//!
//! This module provides a C-compatible API for using the ALS compression library
//! from C and other languages with C FFI support.
//!
//! # Memory Management
//!
//! All strings returned by this API are allocated on the heap and must be freed
//! by calling `als_string_free()`. Failure to free strings will result in memory leaks.
//!
//! # Error Handling
//!
//! Functions return null pointers on error. Use `als_get_last_error()` to retrieve
//! the error message.
//!
//! # Thread Safety
//!
//! The compressor and parser objects are thread-safe and can be shared across threads.
//! However, you must ensure proper synchronization when accessing them from multiple threads.
//!
//! # Example (C)
//!
//! ```c
//! #include "als.h"
//! #include <stdio.h>
//! #include <stdlib.h>
//!
//! int main() {
//!     // Create compressor
//!     AlsCompressor* compressor = als_compressor_new();
//!     if (!compressor) {
//!         fprintf(stderr, "Failed to create compressor\n");
//!         return 1;
//!     }
//!
//!     // Compress CSV
//!     const char* csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
//!     char* als = als_compress_csv(compressor, csv, strlen(csv));
//!     if (!als) {
//!         char error[256];
//!         als_get_last_error(error, sizeof(error));
//!         fprintf(stderr, "Compression failed: %s\n", error);
//!         als_compressor_free(compressor);
//!         return 1;
//!     }
//!
//!     printf("Compressed: %s\n", als);
//!
//!     // Clean up
//!     als_string_free(als);
//!     als_compressor_free(compressor);
//!     return 0;
//! }
//! ```

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Mutex;

use crate::compress::AlsCompressor;
use crate::als::AlsParser;
use crate::config::CompressorConfig;

/// Thread-local storage for the last error message.
///
/// This allows error messages to be retrieved after a function returns null.
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

/// Set the last error message.
fn set_last_error(error: String) {
    if let Ok(mut last_error) = LAST_ERROR.lock() {
        *last_error = Some(error);
    }
}

/// Clear the last error message.
fn clear_last_error() {
    if let Ok(mut last_error) = LAST_ERROR.lock() {
        *last_error = None;
    }
}

/// Opaque handle to an ALS compressor.
///
/// This type is opaque to C code and should only be manipulated through
/// the provided API functions.
#[repr(C)]
pub struct AlsCompressorHandle {
    _private: [u8; 0],
}

/// Opaque handle to an ALS parser.
///
/// This type is opaque to C code and should only be manipulated through
/// the provided API functions.
#[repr(C)]
pub struct AlsParserHandle {
    _private: [u8; 0],
}

/// Create a new ALS compressor with default configuration.
///
/// Returns a pointer to the compressor, or null on failure.
/// The compressor must be freed with `als_compressor_free()`.
///
/// # Safety
///
/// The returned pointer must be freed exactly once using `als_compressor_free()`.
#[no_mangle]
pub extern "C" fn als_compressor_new() -> *mut AlsCompressorHandle {
    clear_last_error();
    
    let result = catch_unwind(|| {
        let compressor = Box::new(AlsCompressor::new());
        Box::into_raw(compressor) as *mut AlsCompressorHandle
    });
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic creating compressor: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Create a new ALS compressor with custom configuration.
///
/// # Arguments
///
/// * `ctx_fallback_threshold` - Minimum compression ratio before falling back to CTX (e.g., 1.2)
/// * `min_pattern_length` - Minimum pattern length to consider (e.g., 3)
/// * `parallelism` - Number of threads for parallel processing (0 = auto)
///
/// Returns a pointer to the compressor, or null on failure.
/// The compressor must be freed with `als_compressor_free()`.
///
/// # Safety
///
/// The returned pointer must be freed exactly once using `als_compressor_free()`.
#[no_mangle]
pub extern "C" fn als_compressor_new_with_config(
    ctx_fallback_threshold: f64,
    min_pattern_length: usize,
    parallelism: usize,
) -> *mut AlsCompressorHandle {
    clear_last_error();
    
    let result = catch_unwind(|| {
        let config = CompressorConfig::default()
            .with_ctx_fallback_threshold(ctx_fallback_threshold)
            .with_min_pattern_length(min_pattern_length)
            .with_parallelism(parallelism);
        
        let compressor = Box::new(AlsCompressor::with_config(config));
        Box::into_raw(compressor) as *mut AlsCompressorHandle
    });
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic creating compressor: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Free an ALS compressor.
///
/// # Safety
///
/// * `compressor` must be a valid pointer returned by `als_compressor_new()` or
///   `als_compressor_new_with_config()`
/// * `compressor` must not be null
/// * `compressor` must not have been previously freed
/// * After calling this function, `compressor` must not be used again
#[no_mangle]
pub unsafe extern "C" fn als_compressor_free(compressor: *mut AlsCompressorHandle) {
    if !compressor.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(Box::from_raw(compressor as *mut AlsCompressor));
        }));
    }
}

/// Compress CSV data to ALS format.
///
/// # Arguments
///
/// * `compressor` - Pointer to an ALS compressor
/// * `input` - Pointer to CSV data (null-terminated string)
/// * `len` - Length of the input data (excluding null terminator)
///
/// Returns a pointer to the compressed ALS string (null-terminated), or null on failure.
/// The returned string must be freed with `als_string_free()`.
///
/// # Safety
///
/// * `compressor` must be a valid pointer returned by `als_compressor_new()`
/// * `input` must be a valid pointer to a buffer of at least `len` bytes
/// * `input` must contain valid UTF-8 data
#[no_mangle]
pub unsafe extern "C" fn als_compress_csv(
    compressor: *const AlsCompressorHandle,
    input: *const c_char,
    len: usize,
) -> *mut c_char {
    clear_last_error();
    
    if compressor.is_null() {
        set_last_error("Compressor pointer is null".to_string());
        return ptr::null_mut();
    }
    
    if input.is_null() {
        set_last_error("Input pointer is null".to_string());
        return ptr::null_mut();
    }
    
    let result = catch_unwind(AssertUnwindSafe(|| {
        let compressor = &*(compressor as *const AlsCompressor);
        let input_slice = std::slice::from_raw_parts(input as *const u8, len);
        
        let input_str = match std::str::from_utf8(input_slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 input: {}", e));
                return ptr::null_mut();
            }
        };
        
        match compressor.compress_csv(input_str) {
            Ok(als) => {
                match CString::new(als) {
                    Ok(c_str) => c_str.into_raw(),
                    Err(e) => {
                        set_last_error(format!("Failed to create C string: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(format!("Compression failed: {}", e));
                ptr::null_mut()
            }
        }
    }));
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic during compression: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Compress JSON data to ALS format.
///
/// # Arguments
///
/// * `compressor` - Pointer to an ALS compressor
/// * `input` - Pointer to JSON data (null-terminated string)
/// * `len` - Length of the input data (excluding null terminator)
///
/// Returns a pointer to the compressed ALS string (null-terminated), or null on failure.
/// The returned string must be freed with `als_string_free()`.
///
/// # Safety
///
/// * `compressor` must be a valid pointer returned by `als_compressor_new()`
/// * `input` must be a valid pointer to a buffer of at least `len` bytes
/// * `input` must contain valid UTF-8 data
#[no_mangle]
pub unsafe extern "C" fn als_compress_json(
    compressor: *const AlsCompressorHandle,
    input: *const c_char,
    len: usize,
) -> *mut c_char {
    clear_last_error();
    
    if compressor.is_null() {
        set_last_error("Compressor pointer is null".to_string());
        return ptr::null_mut();
    }
    
    if input.is_null() {
        set_last_error("Input pointer is null".to_string());
        return ptr::null_mut();
    }
    
    let result = catch_unwind(AssertUnwindSafe(|| {
        let compressor = &*(compressor as *const AlsCompressor);
        let input_slice = std::slice::from_raw_parts(input as *const u8, len);
        
        let input_str = match std::str::from_utf8(input_slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 input: {}", e));
                return ptr::null_mut();
            }
        };
        
        match compressor.compress_json(input_str) {
            Ok(als) => {
                match CString::new(als) {
                    Ok(c_str) => c_str.into_raw(),
                    Err(e) => {
                        set_last_error(format!("Failed to create C string: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(format!("Compression failed: {}", e));
                ptr::null_mut()
            }
        }
    }));
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic during compression: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Create a new ALS parser.
///
/// Returns a pointer to the parser, or null on failure.
/// The parser must be freed with `als_parser_free()`.
///
/// # Safety
///
/// The returned pointer must be freed exactly once using `als_parser_free()`.
#[no_mangle]
pub extern "C" fn als_parser_new() -> *mut AlsParserHandle {
    clear_last_error();
    
    let result = catch_unwind(|| {
        let parser = Box::new(AlsParser::new());
        Box::into_raw(parser) as *mut AlsParserHandle
    });
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic creating parser: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Free an ALS parser.
///
/// # Safety
///
/// * `parser` must be a valid pointer returned by `als_parser_new()`
/// * `parser` must not be null
/// * `parser` must not have been previously freed
/// * After calling this function, `parser` must not be used again
#[no_mangle]
pub unsafe extern "C" fn als_parser_free(parser: *mut AlsParserHandle) {
    if !parser.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(Box::from_raw(parser as *mut AlsParser));
        }));
    }
}

/// Parse ALS data and convert to CSV format.
///
/// # Arguments
///
/// * `parser` - Pointer to an ALS parser
/// * `input` - Pointer to ALS data (null-terminated string)
/// * `len` - Length of the input data (excluding null terminator)
///
/// Returns a pointer to the CSV string (null-terminated), or null on failure.
/// The returned string must be freed with `als_string_free()`.
///
/// # Safety
///
/// * `parser` must be a valid pointer returned by `als_parser_new()`
/// * `input` must be a valid pointer to a buffer of at least `len` bytes
/// * `input` must contain valid UTF-8 data
#[no_mangle]
pub unsafe extern "C" fn als_to_csv(
    parser: *const AlsParserHandle,
    input: *const c_char,
    len: usize,
) -> *mut c_char {
    clear_last_error();
    
    if parser.is_null() {
        set_last_error("Parser pointer is null".to_string());
        return ptr::null_mut();
    }
    
    if input.is_null() {
        set_last_error("Input pointer is null".to_string());
        return ptr::null_mut();
    }
    
    let result = catch_unwind(AssertUnwindSafe(|| {
        let parser = &*(parser as *const AlsParser);
        let input_slice = std::slice::from_raw_parts(input as *const u8, len);
        
        let input_str = match std::str::from_utf8(input_slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 input: {}", e));
                return ptr::null_mut();
            }
        };
        
        match parser.to_csv(input_str) {
            Ok(csv) => {
                match CString::new(csv) {
                    Ok(c_str) => c_str.into_raw(),
                    Err(e) => {
                        set_last_error(format!("Failed to create C string: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(format!("Parsing failed: {}", e));
                ptr::null_mut()
            }
        }
    }));
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic during parsing: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Parse ALS data and convert to JSON format.
///
/// # Arguments
///
/// * `parser` - Pointer to an ALS parser
/// * `input` - Pointer to ALS data (null-terminated string)
/// * `len` - Length of the input data (excluding null terminator)
///
/// Returns a pointer to the JSON string (null-terminated), or null on failure.
/// The returned string must be freed with `als_string_free()`.
///
/// # Safety
///
/// * `parser` must be a valid pointer returned by `als_parser_new()`
/// * `input` must be a valid pointer to a buffer of at least `len` bytes
/// * `input` must contain valid UTF-8 data
#[no_mangle]
pub unsafe extern "C" fn als_to_json(
    parser: *const AlsParserHandle,
    input: *const c_char,
    len: usize,
) -> *mut c_char {
    clear_last_error();
    
    if parser.is_null() {
        set_last_error("Parser pointer is null".to_string());
        return ptr::null_mut();
    }
    
    if input.is_null() {
        set_last_error("Input pointer is null".to_string());
        return ptr::null_mut();
    }
    
    let result = catch_unwind(AssertUnwindSafe(|| {
        let parser = &*(parser as *const AlsParser);
        let input_slice = std::slice::from_raw_parts(input as *const u8, len);
        
        let input_str = match std::str::from_utf8(input_slice) {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("Invalid UTF-8 input: {}", e));
                return ptr::null_mut();
            }
        };
        
        match parser.to_json(input_str) {
            Ok(json) => {
                match CString::new(json) {
                    Ok(c_str) => c_str.into_raw(),
                    Err(e) => {
                        set_last_error(format!("Failed to create C string: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(format!("Parsing failed: {}", e));
                ptr::null_mut()
            }
        }
    }));
    
    match result {
        Ok(ptr) => ptr,
        Err(e) => {
            set_last_error(format!("Panic during parsing: {:?}", e));
            ptr::null_mut()
        }
    }
}

/// Free a string returned by the ALS library.
///
/// # Safety
///
/// * `s` must be a valid pointer returned by one of the ALS functions
/// * `s` must not be null
/// * `s` must not have been previously freed
/// * After calling this function, `s` must not be used again
#[no_mangle]
pub unsafe extern "C" fn als_string_free(s: *mut c_char) {
    if !s.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(CString::from_raw(s));
        }));
    }
}

/// Get the last error message.
///
/// # Arguments
///
/// * `buffer` - Pointer to a buffer to receive the error message
/// * `buffer_len` - Size of the buffer in bytes
///
/// Returns 0 if no error occurred, or the length of the error message (including null terminator).
/// If the buffer is too small, the error message is truncated.
///
/// # Safety
///
/// * `buffer` must be a valid pointer to a buffer of at least `buffer_len` bytes
/// * `buffer` must not be null
#[no_mangle]
pub unsafe extern "C" fn als_get_last_error(buffer: *mut c_char, buffer_len: usize) -> c_int {
    if buffer.is_null() || buffer_len == 0 {
        return 0;
    }
    
    let result = catch_unwind(AssertUnwindSafe(|| {
        let last_error = LAST_ERROR.lock().ok()?;
        let error_msg = last_error.as_ref()?;
        
        let error_bytes = error_msg.as_bytes();
        let copy_len = std::cmp::min(error_bytes.len(), buffer_len - 1);
        
        std::ptr::copy_nonoverlapping(
            error_bytes.as_ptr(),
            buffer as *mut u8,
            copy_len,
        );
        
        // Null terminate
        *buffer.add(copy_len) = 0;
        
        Some((error_bytes.len() + 1) as c_int)
    }));
    
    match result {
        Ok(Some(len)) => len,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    
    #[test]
    fn test_compressor_lifecycle() {
        unsafe {
            let compressor = als_compressor_new();
            assert!(!compressor.is_null());
            als_compressor_free(compressor);
        }
    }
    
    #[test]
    fn test_parser_lifecycle() {
        unsafe {
            let parser = als_parser_new();
            assert!(!parser.is_null());
            als_parser_free(parser);
        }
    }
    
    #[test]
    fn test_compress_csv() {
        unsafe {
            let compressor = als_compressor_new();
            assert!(!compressor.is_null());
            
            let csv = "id,name\n1,Alice\n2,Bob\n3,Charlie";
            let csv_cstr = CString::new(csv).unwrap();
            
            let als = als_compress_csv(compressor, csv_cstr.as_ptr(), csv.len());
            assert!(!als.is_null());
            
            let als_str = CStr::from_ptr(als).to_str().unwrap();
            assert!(als_str.contains("#id"));
            assert!(als_str.contains("#name"));
            
            als_string_free(als);
            als_compressor_free(compressor);
        }
    }
    
    #[test]
    fn test_compress_json() {
        unsafe {
            let compressor = als_compressor_new();
            assert!(!compressor.is_null());
            
            let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
            let json_cstr = CString::new(json).unwrap();
            
            let als = als_compress_json(compressor, json_cstr.as_ptr(), json.len());
            assert!(!als.is_null());
            
            let als_str = CStr::from_ptr(als).to_str().unwrap();
            assert!(als_str.contains("#id"));
            assert!(als_str.contains("#name"));
            
            als_string_free(als);
            als_compressor_free(compressor);
        }
    }
    
    #[test]
    fn test_round_trip_csv() {
        unsafe {
            let compressor = als_compressor_new();
            let parser = als_parser_new();
            assert!(!compressor.is_null());
            assert!(!parser.is_null());
            
            let csv = "id,name\n1,Alice\n2,Bob";
            let csv_cstr = CString::new(csv).unwrap();
            
            let als = als_compress_csv(compressor, csv_cstr.as_ptr(), csv.len());
            assert!(!als.is_null());
            
            let als_len = CStr::from_ptr(als).to_bytes().len();
            let csv_result = als_to_csv(parser, als, als_len);
            assert!(!csv_result.is_null());
            
            let csv_result_str = CStr::from_ptr(csv_result).to_str().unwrap();
            assert!(csv_result_str.contains("Alice"));
            assert!(csv_result_str.contains("Bob"));
            
            als_string_free(als);
            als_string_free(csv_result);
            als_compressor_free(compressor);
            als_parser_free(parser);
        }
    }
    
    #[test]
    fn test_error_handling() {
        unsafe {
            let compressor = als_compressor_new();
            assert!(!compressor.is_null());
            
            // Invalid CSV
            let invalid_csv = "id,name\n1,Alice,Extra";
            let csv_cstr = CString::new(invalid_csv).unwrap();
            
            let als = als_compress_csv(compressor, csv_cstr.as_ptr(), invalid_csv.len());
            
            if als.is_null() {
                let mut error_buf = vec![0u8; 256];
                let error_len = als_get_last_error(error_buf.as_mut_ptr() as *mut c_char, 256);
                assert!(error_len > 0);
                
                let error_str = CStr::from_ptr(error_buf.as_ptr() as *const c_char)
                    .to_str()
                    .unwrap();
                assert!(!error_str.is_empty());
            }
            
            als_compressor_free(compressor);
        }
    }
    
    #[test]
    fn test_null_pointer_handling() {
        unsafe {
            // Null compressor
            let csv = "id,name\n1,Alice";
            let csv_cstr = CString::new(csv).unwrap();
            let als = als_compress_csv(ptr::null(), csv_cstr.as_ptr(), csv.len());
            assert!(als.is_null());
            
            // Null input
            let compressor = als_compressor_new();
            let als = als_compress_csv(compressor, ptr::null(), 0);
            assert!(als.is_null());
            als_compressor_free(compressor);
        }
    }
    
    #[test]
    fn test_custom_config() {
        unsafe {
            let compressor = als_compressor_new_with_config(1.5, 4, 2);
            assert!(!compressor.is_null());
            
            let csv = "id\n1\n2\n3\n4\n5";
            let csv_cstr = CString::new(csv).unwrap();
            
            let als = als_compress_csv(compressor, csv_cstr.as_ptr(), csv.len());
            assert!(!als.is_null());
            
            als_string_free(als);
            als_compressor_free(compressor);
        }
    }
}
