//! Escape sequence handling for ALS format.
//!
//! This module provides functions for escaping and unescaping strings
//! that contain ALS operator characters. When data values contain characters
//! that have special meaning in ALS syntax (like `>`, `*`, `~`, etc.),
//! they must be escaped to preserve the original data.
//!
//! # Reserved Tokens
//!
//! - `\0` - Represents a null value
//! - `\e` - Represents an empty string
//!
//! # Escape Sequences
//!
//! | Character | Escape | Description |
//! |-----------|--------|-------------|
//! | `>` | `\>` | Range operator |
//! | `*` | `\*` | Multiplier operator |
//! | `~` | `\~` | Toggle operator |
//! | `|` | `\|` | Column separator |
//! | `_` | `\_` | Dictionary reference prefix |
//! | `#` | `\#` | Schema prefix |
//! | `$` | `\$` | Dictionary header prefix |
//! | `:` | `\:` | Step separator in ranges |
//! | `\` | `\\` | Escape character itself |
//! | newline | `\n` | Line break |
//! | tab | `\t` | Tab character |
//! | carriage return | `\r` | Carriage return |
//! | space | `\ ` | Preserved space (in delimiter contexts) |

use crate::error::{AlsError, Result};

/// Reserved token representing a null value in ALS format.
///
/// When a data value is null (e.g., from JSON null or CSV empty field
/// that should be interpreted as null), it is encoded as this token.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::NULL_TOKEN;
/// assert_eq!(NULL_TOKEN, "\\0");
/// ```
pub const NULL_TOKEN: &str = "\\0";

/// Reserved token representing an empty string in ALS format.
///
/// When a data value is an empty string (as opposed to null), it is
/// encoded as this token to distinguish it from null values.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::EMPTY_TOKEN;
/// assert_eq!(EMPTY_TOKEN, "\\e");
/// ```
pub const EMPTY_TOKEN: &str = "\\e";

/// Escape a string for use in ALS format.
///
/// This function escapes all characters that have special meaning in ALS
/// syntax, ensuring that the original string can be safely embedded in
/// ALS output and later recovered exactly.
///
/// # Arguments
///
/// * `s` - The string to escape
///
/// # Returns
///
/// A new string with all special characters escaped.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::escape_als_string;
///
/// // Escape range operator
/// assert_eq!(escape_als_string("1>2"), "1\\>2");
///
/// // Escape multiple operators
/// assert_eq!(escape_als_string("a*b~c"), "a\\*b\\~c");
///
/// // Escape newlines and tabs
/// assert_eq!(escape_als_string("line1\nline2"), "line1\\nline2");
/// ```
pub fn escape_als_string(s: &str) -> String {
    // Pre-allocate with some extra capacity for escape sequences
    let mut result = String::with_capacity(s.len() + s.len() / 4);
    
    for c in s.chars() {
        match c {
            '>' => result.push_str("\\>"),
            '*' => result.push_str("\\*"),
            '~' => result.push_str("\\~"),
            '|' => result.push_str("\\|"),
            '_' => result.push_str("\\_"),
            '#' => result.push_str("\\#"),
            '$' => result.push_str("\\$"),
            ':' => result.push_str("\\:"),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\t' => result.push_str("\\t"),
            '\r' => result.push_str("\\r"),
            ' ' => result.push_str("\\ "),
            _ => result.push(c),
        }
    }
    
    result
}

/// Unescape an ALS-escaped string back to its original form.
///
/// This function reverses the escaping performed by `escape_als_string`,
/// converting escape sequences back to their original characters.
///
/// # Arguments
///
/// * `s` - The escaped string to unescape
///
/// # Returns
///
/// A `Result` containing the unescaped string, or an error if the input
/// contains invalid escape sequences.
///
/// # Errors
///
/// Returns `AlsError::AlsSyntaxError` if:
/// - An escape sequence is incomplete (trailing backslash)
/// - An unknown escape sequence is encountered
///
/// # Example
///
/// ```
/// use als_compression::als::escape::unescape_als_string;
///
/// // Unescape range operator
/// assert_eq!(unescape_als_string("1\\>2").unwrap(), "1>2");
///
/// // Unescape multiple operators
/// assert_eq!(unescape_als_string("a\\*b\\~c").unwrap(), "a*b~c");
///
/// // Unescape newlines and tabs
/// assert_eq!(unescape_als_string("line1\\nline2").unwrap(), "line1\nline2");
/// ```
pub fn unescape_als_string(s: &str) -> Result<String> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    let mut position = 0;
    
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('>') => result.push('>'),
                Some('*') => result.push('*'),
                Some('~') => result.push('~'),
                Some('|') => result.push('|'),
                Some('_') => result.push('_'),
                Some('#') => result.push('#'),
                Some('$') => result.push('$'),
                Some(':') => result.push(':'),
                Some('\\') => result.push('\\'),
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some(' ') => result.push(' '),
                Some('0') => {
                    // This is the NULL_TOKEN - return special marker
                    // The caller should handle this case specially
                    // For now, we'll just skip it as it represents null
                    // Note: This only applies when the entire string is \0
                }
                Some('e') => {
                    // This is the EMPTY_TOKEN - represents empty string
                    // The caller should handle this case specially
                    // Note: This only applies when the entire string is \e
                }
                Some(other) => {
                    return Err(AlsError::AlsSyntaxError {
                        position,
                        message: format!("Unknown escape sequence: \\{}", other),
                    });
                }
                None => {
                    return Err(AlsError::AlsSyntaxError {
                        position,
                        message: "Incomplete escape sequence at end of string".to_string(),
                    });
                }
            }
            position += 2; // Escape sequences are 2 characters
        } else {
            result.push(c);
            position += c.len_utf8();
        }
    }
    
    Ok(result)
}

/// Check if a string is the null token.
///
/// # Arguments
///
/// * `s` - The string to check
///
/// # Returns
///
/// `true` if the string equals `NULL_TOKEN`, `false` otherwise.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::{is_null_token, NULL_TOKEN};
///
/// assert!(is_null_token(NULL_TOKEN));
/// assert!(!is_null_token("hello"));
/// ```
#[inline]
pub fn is_null_token(s: &str) -> bool {
    s == NULL_TOKEN
}

/// Check if a string is the empty token.
///
/// # Arguments
///
/// * `s` - The string to check
///
/// # Returns
///
/// `true` if the string equals `EMPTY_TOKEN`, `false` otherwise.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::{is_empty_token, EMPTY_TOKEN};
///
/// assert!(is_empty_token(EMPTY_TOKEN));
/// assert!(!is_empty_token("hello"));
/// ```
#[inline]
pub fn is_empty_token(s: &str) -> bool {
    s == EMPTY_TOKEN
}

/// Encode a value for ALS format, handling null and empty strings specially.
///
/// This function provides a higher-level interface that:
/// - Returns `NULL_TOKEN` for `None` values
/// - Returns `EMPTY_TOKEN` for empty strings
/// - Escapes all other strings using `escape_als_string`
///
/// # Arguments
///
/// * `value` - An optional string value to encode
///
/// # Returns
///
/// The encoded string suitable for ALS format.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::{encode_als_value, NULL_TOKEN, EMPTY_TOKEN};
///
/// assert_eq!(encode_als_value(None), NULL_TOKEN);
/// assert_eq!(encode_als_value(Some("")), EMPTY_TOKEN);
/// assert_eq!(encode_als_value(Some("hello")), "hello");
/// assert_eq!(encode_als_value(Some("a>b")), "a\\>b");
/// ```
pub fn encode_als_value(value: Option<&str>) -> String {
    match value {
        None => NULL_TOKEN.to_string(),
        Some("") => EMPTY_TOKEN.to_string(),
        Some(s) => escape_als_string(s),
    }
}

/// Decode an ALS-encoded value, handling null and empty tokens specially.
///
/// This function provides a higher-level interface that:
/// - Returns `None` for `NULL_TOKEN`
/// - Returns `Some("")` for `EMPTY_TOKEN`
/// - Unescapes all other strings using `unescape_als_string`
///
/// # Arguments
///
/// * `s` - The ALS-encoded string to decode
///
/// # Returns
///
/// A `Result` containing `None` for null values, or `Some(String)` for
/// actual string values.
///
/// # Errors
///
/// Returns `AlsError::AlsSyntaxError` if the string contains invalid
/// escape sequences.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::{decode_als_value, NULL_TOKEN, EMPTY_TOKEN};
///
/// assert_eq!(decode_als_value(NULL_TOKEN).unwrap(), None);
/// assert_eq!(decode_als_value(EMPTY_TOKEN).unwrap(), Some("".to_string()));
/// assert_eq!(decode_als_value("hello").unwrap(), Some("hello".to_string()));
/// assert_eq!(decode_als_value("a\\>b").unwrap(), Some("a>b".to_string()));
/// ```
pub fn decode_als_value(s: &str) -> Result<Option<String>> {
    if s == NULL_TOKEN {
        Ok(None)
    } else if s == EMPTY_TOKEN {
        Ok(Some(String::new()))
    } else {
        unescape_als_string(s).map(Some)
    }
}

/// Check if a string needs escaping for ALS format.
///
/// This is useful for optimization - if a string doesn't contain any
/// special characters, it can be used directly without allocation.
///
/// # Arguments
///
/// * `s` - The string to check
///
/// # Returns
///
/// `true` if the string contains characters that need escaping.
///
/// # Example
///
/// ```
/// use als_compression::als::escape::needs_escaping;
///
/// assert!(!needs_escaping("hello"));
/// assert!(needs_escaping("a>b"));
/// assert!(needs_escaping("line1\nline2"));
/// ```
pub fn needs_escaping(s: &str) -> bool {
    s.chars().any(|c| matches!(c, 
        '>' | '*' | '~' | '|' | '_' | '#' | '$' | ':' | '\\' | '\n' | '\t' | '\r' | ' '
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== escape_als_string tests ====================

    #[test]
    fn test_escape_range_operator() {
        assert_eq!(escape_als_string("1>2"), "1\\>2");
        assert_eq!(escape_als_string(">"), "\\>");
        assert_eq!(escape_als_string("a>b>c"), "a\\>b\\>c");
    }

    #[test]
    fn test_escape_multiplier_operator() {
        assert_eq!(escape_als_string("a*3"), "a\\*3");
        assert_eq!(escape_als_string("*"), "\\*");
    }

    #[test]
    fn test_escape_toggle_operator() {
        assert_eq!(escape_als_string("a~b"), "a\\~b");
        assert_eq!(escape_als_string("~"), "\\~");
    }

    #[test]
    fn test_escape_column_separator() {
        assert_eq!(escape_als_string("a|b"), "a\\|b");
        assert_eq!(escape_als_string("|"), "\\|");
    }

    #[test]
    fn test_escape_dict_ref_prefix() {
        assert_eq!(escape_als_string("_0"), "\\_0");
        assert_eq!(escape_als_string("_"), "\\_");
    }

    #[test]
    fn test_escape_schema_prefix() {
        assert_eq!(escape_als_string("#col"), "\\#col");
        assert_eq!(escape_als_string("#"), "\\#");
    }

    #[test]
    fn test_escape_dict_header_prefix() {
        assert_eq!(escape_als_string("$key"), "\\$key");
        assert_eq!(escape_als_string("$"), "\\$");
    }

    #[test]
    fn test_escape_backslash() {
        assert_eq!(escape_als_string("a\\b"), "a\\\\b");
        assert_eq!(escape_als_string("\\"), "\\\\");
    }

    #[test]
    fn test_escape_newline() {
        assert_eq!(escape_als_string("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_als_string("\n"), "\\n");
    }

    #[test]
    fn test_escape_tab() {
        assert_eq!(escape_als_string("col1\tcol2"), "col1\\tcol2");
        assert_eq!(escape_als_string("\t"), "\\t");
    }

    #[test]
    fn test_escape_carriage_return() {
        assert_eq!(escape_als_string("line1\rline2"), "line1\\rline2");
        assert_eq!(escape_als_string("\r"), "\\r");
    }

    #[test]
    fn test_escape_space() {
        assert_eq!(escape_als_string("hello world"), "hello\\ world");
        assert_eq!(escape_als_string(" "), "\\ ");
    }

    #[test]
    fn test_escape_multiple_operators() {
        assert_eq!(escape_als_string("a>b*c~d"), "a\\>b\\*c\\~d");
    }

    #[test]
    fn test_escape_no_special_chars() {
        assert_eq!(escape_als_string("hello"), "hello");
        assert_eq!(escape_als_string("12345"), "12345");
        assert_eq!(escape_als_string(""), "");
    }

    #[test]
    fn test_escape_unicode() {
        // Unicode characters should pass through unchanged
        assert_eq!(escape_als_string("héllo"), "héllo");
        assert_eq!(escape_als_string("日本語"), "日本語");
        assert_eq!(escape_als_string("🎉"), "🎉");
    }

    #[test]
    fn test_escape_mixed_unicode_and_operators() {
        assert_eq!(escape_als_string("日本>語"), "日本\\>語");
        assert_eq!(escape_als_string("🎉*3"), "🎉\\*3");
    }

    // ==================== unescape_als_string tests ====================

    #[test]
    fn test_unescape_range_operator() {
        assert_eq!(unescape_als_string("1\\>2").unwrap(), "1>2");
        assert_eq!(unescape_als_string("\\>").unwrap(), ">");
    }

    #[test]
    fn test_unescape_multiplier_operator() {
        assert_eq!(unescape_als_string("a\\*3").unwrap(), "a*3");
        assert_eq!(unescape_als_string("\\*").unwrap(), "*");
    }

    #[test]
    fn test_unescape_toggle_operator() {
        assert_eq!(unescape_als_string("a\\~b").unwrap(), "a~b");
        assert_eq!(unescape_als_string("\\~").unwrap(), "~");
    }

    #[test]
    fn test_unescape_column_separator() {
        assert_eq!(unescape_als_string("a\\|b").unwrap(), "a|b");
        assert_eq!(unescape_als_string("\\|").unwrap(), "|");
    }

    #[test]
    fn test_unescape_dict_ref_prefix() {
        assert_eq!(unescape_als_string("\\_0").unwrap(), "_0");
        assert_eq!(unescape_als_string("\\_").unwrap(), "_");
    }

    #[test]
    fn test_unescape_schema_prefix() {
        assert_eq!(unescape_als_string("\\#col").unwrap(), "#col");
        assert_eq!(unescape_als_string("\\#").unwrap(), "#");
    }

    #[test]
    fn test_unescape_dict_header_prefix() {
        assert_eq!(unescape_als_string("\\$key").unwrap(), "$key");
        assert_eq!(unescape_als_string("\\$").unwrap(), "$");
    }

    #[test]
    fn test_unescape_backslash() {
        assert_eq!(unescape_als_string("a\\\\b").unwrap(), "a\\b");
        assert_eq!(unescape_als_string("\\\\").unwrap(), "\\");
    }

    #[test]
    fn test_unescape_newline() {
        assert_eq!(unescape_als_string("line1\\nline2").unwrap(), "line1\nline2");
        assert_eq!(unescape_als_string("\\n").unwrap(), "\n");
    }

    #[test]
    fn test_unescape_tab() {
        assert_eq!(unescape_als_string("col1\\tcol2").unwrap(), "col1\tcol2");
        assert_eq!(unescape_als_string("\\t").unwrap(), "\t");
    }

    #[test]
    fn test_unescape_carriage_return() {
        assert_eq!(unescape_als_string("line1\\rline2").unwrap(), "line1\rline2");
        assert_eq!(unescape_als_string("\\r").unwrap(), "\r");
    }

    #[test]
    fn test_unescape_space() {
        assert_eq!(unescape_als_string("hello\\ world").unwrap(), "hello world");
        assert_eq!(unescape_als_string("\\ ").unwrap(), " ");
    }

    #[test]
    fn test_unescape_multiple_operators() {
        assert_eq!(unescape_als_string("a\\>b\\*c\\~d").unwrap(), "a>b*c~d");
    }

    #[test]
    fn test_unescape_no_escapes() {
        assert_eq!(unescape_als_string("hello").unwrap(), "hello");
        assert_eq!(unescape_als_string("12345").unwrap(), "12345");
        assert_eq!(unescape_als_string("").unwrap(), "");
    }

    #[test]
    fn test_unescape_unicode() {
        assert_eq!(unescape_als_string("héllo").unwrap(), "héllo");
        assert_eq!(unescape_als_string("日本語").unwrap(), "日本語");
        assert_eq!(unescape_als_string("🎉").unwrap(), "🎉");
    }

    #[test]
    fn test_unescape_invalid_escape_sequence() {
        let result = unescape_als_string("\\x");
        assert!(result.is_err());
        if let Err(AlsError::AlsSyntaxError { message, .. }) = result {
            assert!(message.contains("Unknown escape sequence"));
        }
    }

    #[test]
    fn test_unescape_incomplete_escape() {
        let result = unescape_als_string("hello\\");
        assert!(result.is_err());
        if let Err(AlsError::AlsSyntaxError { message, .. }) = result {
            assert!(message.contains("Incomplete escape sequence"));
        }
    }

    // ==================== Round-trip tests ====================

    #[test]
    fn test_roundtrip_simple() {
        let original = "hello";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_roundtrip_with_operators() {
        let original = "a>b*c~d|e_f#g$h";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_roundtrip_with_whitespace() {
        let original = "line1\nline2\tline3\rline4 line5";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_roundtrip_with_backslashes() {
        let original = "path\\to\\file";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_roundtrip_unicode() {
        let original = "日本語>テスト*🎉";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    #[test]
    fn test_roundtrip_empty() {
        let original = "";
        let escaped = escape_als_string(original);
        let unescaped = unescape_als_string(&escaped).unwrap();
        assert_eq!(original, unescaped);
    }

    // ==================== Token tests ====================

    #[test]
    fn test_null_token() {
        assert_eq!(NULL_TOKEN, "\\0");
        assert!(is_null_token(NULL_TOKEN));
        assert!(!is_null_token("\\0extra"));
        assert!(!is_null_token("hello"));
    }

    #[test]
    fn test_empty_token() {
        assert_eq!(EMPTY_TOKEN, "\\e");
        assert!(is_empty_token(EMPTY_TOKEN));
        assert!(!is_empty_token("\\eextra"));
        assert!(!is_empty_token("hello"));
    }

    // ==================== encode/decode tests ====================

    #[test]
    fn test_encode_null() {
        assert_eq!(encode_als_value(None), NULL_TOKEN);
    }

    #[test]
    fn test_encode_empty() {
        assert_eq!(encode_als_value(Some("")), EMPTY_TOKEN);
    }

    #[test]
    fn test_encode_normal() {
        assert_eq!(encode_als_value(Some("hello")), "hello");
    }

    #[test]
    fn test_encode_with_operators() {
        assert_eq!(encode_als_value(Some("a>b")), "a\\>b");
    }

    #[test]
    fn test_decode_null() {
        assert_eq!(decode_als_value(NULL_TOKEN).unwrap(), None);
    }

    #[test]
    fn test_decode_empty() {
        assert_eq!(decode_als_value(EMPTY_TOKEN).unwrap(), Some("".to_string()));
    }

    #[test]
    fn test_decode_normal() {
        assert_eq!(decode_als_value("hello").unwrap(), Some("hello".to_string()));
    }

    #[test]
    fn test_decode_with_operators() {
        assert_eq!(decode_als_value("a\\>b").unwrap(), Some("a>b".to_string()));
    }

    // ==================== needs_escaping tests ====================

    #[test]
    fn test_needs_escaping_true() {
        assert!(needs_escaping("a>b"));
        assert!(needs_escaping("a*b"));
        assert!(needs_escaping("a~b"));
        assert!(needs_escaping("a|b"));
        assert!(needs_escaping("_0"));
        assert!(needs_escaping("#col"));
        assert!(needs_escaping("$key"));
        assert!(needs_escaping("a\\b"));
        assert!(needs_escaping("a\nb"));
        assert!(needs_escaping("a\tb"));
        assert!(needs_escaping("a\rb"));
        assert!(needs_escaping("a b"));
    }

    #[test]
    fn test_needs_escaping_false() {
        assert!(!needs_escaping("hello"));
        assert!(!needs_escaping("12345"));
        assert!(!needs_escaping(""));
        assert!(!needs_escaping("日本語"));
        assert!(!needs_escaping("🎉"));
    }
}
