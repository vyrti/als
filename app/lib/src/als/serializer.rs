//! ALS format serializer.
//!
//! This module provides the serializer for converting `AlsDocument` structures
//! into ALS format text. It handles version headers, dictionaries, schema,
//! and column streams with proper escaping.

use super::document::{AlsDocument, ColumnStream, FormatIndicator};
use super::escape::escape_als_string;
use super::operator::AlsOperator;

/// ALS format serializer.
///
/// Converts `AlsDocument` structures into ALS format text strings.
/// The serializer handles:
/// - Version headers (`!v1` or `!ctx`)
/// - Dictionary headers (`$name:val1|val2`)
/// - Schema definitions (`#col1 #col2`)
/// - Column streams with operators separated by `|`
pub struct AlsSerializer;

impl AlsSerializer {
    /// Create a new serializer.
    pub fn new() -> Self {
        Self
    }

    /// Serialize an `AlsDocument` to ALS format string.
    ///
    /// # Arguments
    ///
    /// * `doc` - The document to serialize
    ///
    /// # Returns
    ///
    /// A string containing the ALS format representation.
    ///
    /// # Example
    ///
    /// ```
    /// use als_compression::als::{AlsDocument, AlsSerializer, ColumnStream, AlsOperator};
    ///
    /// let mut doc = AlsDocument::with_schema(vec!["id", "name"]);
    /// doc.add_stream(ColumnStream::from_operators(vec![AlsOperator::range(1, 3)]));
    /// doc.add_stream(ColumnStream::from_operators(vec![
    ///     AlsOperator::raw("alice"),
    ///     AlsOperator::raw("bob"),
    ///     AlsOperator::raw("charlie"),
    /// ]));
    ///
    /// let serializer = AlsSerializer::new();
    /// let als_text = serializer.serialize(&doc);
    /// ```
    pub fn serialize(&self, doc: &AlsDocument) -> String {
        let mut output = String::new();

        // Serialize version header
        self.serialize_version(&mut output, doc);

        // Serialize dictionaries
        self.serialize_dictionaries(&mut output, doc);

        // Serialize schema
        self.serialize_schema(&mut output, doc);

        // Serialize column streams
        self.serialize_streams(&mut output, doc);

        output
    }

    /// Serialize the version header.
    fn serialize_version(&self, output: &mut String, doc: &AlsDocument) {
        match doc.format_indicator {
            FormatIndicator::Als => {
                output.push_str(&format!("!v{}\n", doc.version));
            }
            FormatIndicator::Ctx => {
                output.push_str("!ctx\n");
            }
        }
    }

    /// Serialize dictionary headers.
    fn serialize_dictionaries(&self, output: &mut String, doc: &AlsDocument) {
        // Sort dictionary names for deterministic output
        let mut dict_names: Vec<_> = doc.dictionaries.keys().collect();
        dict_names.sort();

        for name in dict_names {
            if let Some(values) = doc.dictionaries.get(name) {
                output.push('$');
                output.push_str(name);
                output.push(':');

                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        output.push('|');
                    }
                    // Escape special characters in dictionary values
                    output.push_str(&escape_dict_value(value));
                }
                output.push('\n');
            }
        }
    }

    /// Serialize the schema.
    fn serialize_schema(&self, output: &mut String, doc: &AlsDocument) {
        for (i, col_name) in doc.schema.iter().enumerate() {
            if i > 0 {
                output.push(' ');
            }
            output.push('#');
            output.push_str(&escape_schema_name(col_name));
        }
        if !doc.schema.is_empty() {
            output.push('\n');
        }
    }

    /// Serialize column streams.
    pub fn serialize_streams(&self, output: &mut String, doc: &AlsDocument) {
        for (i, stream) in doc.streams.iter().enumerate() {
            if i > 0 {
                output.push('|');
            }
            self.serialize_stream(output, stream);
        }
    }

    /// Serialize a single column stream.
    fn serialize_stream(&self, output: &mut String, stream: &ColumnStream) {
        for (i, op) in stream.operators.iter().enumerate() {
            if i > 0 {
                output.push(' ');
            }
            self.serialize_operator(output, op);
        }
    }

    /// Serialize a single operator.
    pub fn serialize_operator(&self, output: &mut String, op: &AlsOperator) {
        match op {
            AlsOperator::Raw(value) => {
                output.push_str(&escape_als_string(value));
            }
            AlsOperator::Range { start, end, step } => {
                output.push_str(&start.to_string());
                output.push('>');
                output.push_str(&end.to_string());
                // Only include step if it's not the default
                let default_step = if *end >= *start { 1 } else { -1 };
                if *step != default_step {
                    output.push(':');
                    output.push_str(&step.to_string());
                }
            }
            AlsOperator::Multiply { value, count } => {
                // Check if inner value needs parentheses
                let needs_parens = matches!(value.as_ref(), 
                    AlsOperator::Range { .. } | 
                    AlsOperator::Toggle { .. } |
                    AlsOperator::Multiply { .. }
                );
                
                if needs_parens {
                    output.push('(');
                    self.serialize_operator(output, value);
                    output.push(')');
                } else {
                    self.serialize_operator(output, value);
                }
                output.push('*');
                output.push_str(&count.to_string());
            }
            AlsOperator::Toggle { values, count } => {
                for (i, val) in values.iter().enumerate() {
                    if i > 0 {
                        output.push('~');
                    }
                    output.push_str(&escape_als_string(val));
                }
                output.push('*');
                output.push_str(&count.to_string());
            }
            AlsOperator::DictRef(index) => {
                output.push('_');
                output.push_str(&index.to_string());
            }
        }
    }
}

impl Default for AlsSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// ALS pretty printer for human-readable output.
///
/// Produces formatted ALS output with visual separation and optional
/// debug comments showing expanded values. This is useful for debugging
/// and inspecting compressed output.
pub struct AlsPrettyPrinter {
    /// Whether to include debug comments showing expanded values
    show_expanded: bool,
    /// Indentation string (default: 2 spaces)
    indent: String,
}

impl AlsPrettyPrinter {
    /// Create a new pretty printer with default settings.
    pub fn new() -> Self {
        Self {
            show_expanded: false,
            indent: "  ".to_string(),
        }
    }

    /// Enable or disable debug comments showing expanded values.
    ///
    /// When enabled, each operator will have a comment showing what
    /// values it expands to.
    pub fn with_expanded_comments(mut self, show: bool) -> Self {
        self.show_expanded = show;
        self
    }

    /// Set the indentation string.
    pub fn with_indent(mut self, indent: &str) -> Self {
        self.indent = indent.to_string();
        self
    }

    /// Pretty print an `AlsDocument` to a formatted string.
    ///
    /// # Arguments
    ///
    /// * `doc` - The document to pretty print
    ///
    /// # Returns
    ///
    /// A formatted string with visual separation and optional debug comments.
    pub fn format(&self, doc: &AlsDocument) -> String {
        let mut output = String::new();

        // Header section
        output.push_str("# ALS Document\n");
        output.push_str("# =============\n\n");

        // Version
        self.format_version(&mut output, doc);
        output.push('\n');

        // Dictionaries
        if !doc.dictionaries.is_empty() {
            output.push_str("# Dictionaries\n");
            output.push_str("# ------------\n");
            self.format_dictionaries(&mut output, doc);
            output.push('\n');
        }

        // Schema
        if !doc.schema.is_empty() {
            output.push_str("# Schema\n");
            output.push_str("# ------\n");
            self.format_schema(&mut output, doc);
            output.push('\n');
        }

        // Streams
        if !doc.streams.is_empty() {
            output.push_str("# Data Streams\n");
            output.push_str("# ------------\n");
            self.format_streams(&mut output, doc);
        }

        output
    }

    /// Format the version header.
    fn format_version(&self, output: &mut String, doc: &AlsDocument) {
        match doc.format_indicator {
            FormatIndicator::Als => {
                output.push_str(&format!("!v{}  # ALS format version {}\n", doc.version, doc.version));
            }
            FormatIndicator::Ctx => {
                output.push_str("!ctx  # CTX fallback format\n");
            }
        }
    }

    /// Format dictionary headers.
    fn format_dictionaries(&self, output: &mut String, doc: &AlsDocument) {
        let mut dict_names: Vec<_> = doc.dictionaries.keys().collect();
        dict_names.sort();

        for name in dict_names {
            if let Some(values) = doc.dictionaries.get(name) {
                output.push('$');
                output.push_str(name);
                output.push(':');

                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        output.push('|');
                    }
                    output.push_str(&escape_dict_value(value));
                }

                // Add comment showing indices
                output.push_str("  # indices: ");
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        output.push_str(", ");
                    }
                    output.push_str(&format!("_{}={}", i, value));
                }
                output.push('\n');
            }
        }
    }

    /// Format the schema.
    fn format_schema(&self, output: &mut String, doc: &AlsDocument) {
        for (i, col_name) in doc.schema.iter().enumerate() {
            if i > 0 {
                output.push(' ');
            }
            output.push('#');
            output.push_str(&escape_schema_name(col_name));
        }
        output.push_str(&format!("  # {} columns\n", doc.schema.len()));
    }

    /// Format column streams.
    fn format_streams(&self, output: &mut String, doc: &AlsDocument) {
        let default_dict = doc.default_dictionary();

        for (col_idx, stream) in doc.streams.iter().enumerate() {
            if col_idx > 0 {
                output.push_str("\n|  # column separator\n\n");
            }

            // Column header comment
            let col_name = doc.schema.get(col_idx).map(|s| s.as_str()).unwrap_or("?");
            output.push_str(&format!("# Column {}: {}\n", col_idx, col_name));

            self.format_stream(output, stream, default_dict.map(|v| v.as_slice()));
        }
    }

    /// Format a single column stream.
    fn format_stream(&self, output: &mut String, stream: &ColumnStream, dictionary: Option<&[String]>) {
        for (i, op) in stream.operators.iter().enumerate() {
            if i > 0 {
                output.push(' ');
            }
            self.format_operator(output, op, dictionary);
        }
        output.push('\n');
    }

    /// Format a single operator with optional expanded comment.
    fn format_operator(&self, output: &mut String, op: &AlsOperator, dictionary: Option<&[String]>) {
        // Serialize the operator
        let serializer = AlsSerializer::new();
        let mut op_str = String::new();
        serializer.serialize_operator(&mut op_str, op);
        output.push_str(&op_str);

        // Add expanded comment if enabled
        if self.show_expanded {
            if let Ok(expanded) = op.expand(dictionary) {
                let preview = if expanded.len() <= 5 {
                    expanded.join(", ")
                } else {
                    format!(
                        "{}, ..., {} ({} values)",
                        expanded[..2].join(", "),
                        expanded.last().unwrap(),
                        expanded.len()
                    )
                };
                output.push_str(&format!("  /* {} */", preview));
            }
        }
    }
}

impl Default for AlsPrettyPrinter {
    fn default() -> Self {
        Self::new()
    }
}

/// Escape a dictionary value for serialization.
///
/// Dictionary values are separated by `|` and terminated by newline,
/// so we need to escape those characters plus the standard ALS operators.
fn escape_dict_value(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + s.len() / 4);
    
    for c in s.chars() {
        match c {
            '|' => result.push_str("\\|"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\\' => result.push_str("\\\\"),
            _ => result.push(c),
        }
    }
    
    result
}

/// Escape a schema column name for serialization.
///
/// Schema names are separated by spaces, so we need to escape spaces
/// and other special characters.
fn escape_schema_name(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + s.len() / 4);
    
    for c in s.chars() {
        match c {
            ' ' => result.push_str("\\ "),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\\' => result.push_str("\\\\"),
            '#' => result.push_str("\\#"),
            _ => result.push(c),
        }
    }
    
    result
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::als::AlsDocument;

    // ==================== AlsSerializer tests ====================

    #[test]
    fn test_serialize_empty_document() {
        let doc = AlsDocument::new();
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert_eq!(result, "!v1\n");
    }

    #[test]
    fn test_serialize_version_als() {
        let doc = AlsDocument::new();
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.starts_with("!v1\n"));
    }

    #[test]
    fn test_serialize_version_ctx() {
        let mut doc = AlsDocument::new();
        doc.set_ctx_format();
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.starts_with("!ctx\n"));
    }

    #[test]
    fn test_serialize_dictionary() {
        let mut doc = AlsDocument::new();
        doc.add_dictionary("default", vec!["apple".to_string(), "banana".to_string(), "cherry".to_string()]);
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("$default:apple|banana|cherry\n"));
    }

    #[test]
    fn test_serialize_multiple_dictionaries() {
        let mut doc = AlsDocument::new();
        doc.add_dictionary("colors", vec!["red".to_string(), "green".to_string()]);
        doc.add_dictionary("sizes", vec!["small".to_string(), "large".to_string()]);
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        // Dictionaries should be sorted alphabetically
        assert!(result.contains("$colors:red|green\n"));
        assert!(result.contains("$sizes:small|large\n"));
    }

    #[test]
    fn test_serialize_schema() {
        let doc = AlsDocument::with_schema(vec!["id", "name", "age"]);
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("#id #name #age\n"));
    }

    #[test]
    fn test_serialize_raw_values() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("hello"),
            AlsOperator::raw("world"),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("hello world"));
    }

    #[test]
    fn test_serialize_range() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 5),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("1>5"));
    }

    #[test]
    fn test_serialize_range_with_step() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range_with_step(10, 50, 10),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("10>50:10"));
    }

    #[test]
    fn test_serialize_descending_range() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range_with_step(5, 1, -1),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        // Default step for descending is -1, so no step should be shown
        assert!(result.contains("5>1"));
        assert!(!result.contains("5>1:"));
    }

    #[test]
    fn test_serialize_descending_range_with_custom_step() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range_with_step(50, 10, -10),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("50>10:-10"));
    }

    #[test]
    fn test_serialize_multiply() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::multiply(AlsOperator::raw("hello"), 3),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("hello*3"));
    }

    #[test]
    fn test_serialize_multiply_range() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::multiply(AlsOperator::range(1, 3), 2),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("(1>3)*2"));
    }

    #[test]
    fn test_serialize_toggle() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::toggle("T", "F", 4),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("T~F*4"));
    }

    #[test]
    fn test_serialize_toggle_multi() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::toggle_multi(vec!["A", "B", "C"], 6),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("A~B~C*6"));
    }

    #[test]
    fn test_serialize_dict_ref() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_dictionary("default", vec!["apple".to_string(), "banana".to_string()]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::dict_ref(0),
            AlsOperator::dict_ref(1),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("_0 _1"));
    }

    #[test]
    fn test_serialize_multiple_columns() {
        let mut doc = AlsDocument::with_schema(vec!["id", "name"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("alice"),
            AlsOperator::raw("bob"),
            AlsOperator::raw("charlie"),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("1>3|alice bob charlie"));
    }

    #[test]
    fn test_serialize_escaped_values() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("a>b"),
            AlsOperator::raw("c*d"),
            AlsOperator::raw("e~f"),
        ]));
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        assert!(result.contains("a\\>b"));
        assert!(result.contains("c\\*d"));
        assert!(result.contains("e\\~f"));
    }

    #[test]
    fn test_serialize_complete_document() {
        let mut doc = AlsDocument::with_schema(vec!["id", "name", "status"]);
        doc.add_dictionary("default", vec!["active".to_string(), "inactive".to_string()]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("alice"),
            AlsOperator::raw("bob"),
            AlsOperator::raw("charlie"),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::dict_ref(0),
            AlsOperator::dict_ref(1),
            AlsOperator::dict_ref(0),
        ]));
        
        let serializer = AlsSerializer::new();
        let result = serializer.serialize(&doc);
        
        assert!(result.starts_with("!v1\n"));
        assert!(result.contains("$default:active|inactive\n"));
        assert!(result.contains("#id #name #status\n"));
        assert!(result.contains("1>3|alice bob charlie|_0 _1 _0"));
    }

    #[test]
    fn test_escape_dict_value() {
        assert_eq!(escape_dict_value("hello"), "hello");
        assert_eq!(escape_dict_value("a|b"), "a\\|b");
        assert_eq!(escape_dict_value("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_dict_value("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_escape_schema_name() {
        assert_eq!(escape_schema_name("column"), "column");
        assert_eq!(escape_schema_name("my column"), "my\\ column");
        assert_eq!(escape_schema_name("col\ttab"), "col\\ttab");
        assert_eq!(escape_schema_name("a#b"), "a\\#b");
    }

    #[test]
    fn test_serializer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsSerializer>();
    }

    // ==================== AlsPrettyPrinter tests ====================

    #[test]
    fn test_pretty_print_empty_document() {
        let doc = AlsDocument::new();
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("# ALS Document"));
        assert!(result.contains("!v1"));
    }

    #[test]
    fn test_pretty_print_version_als() {
        let doc = AlsDocument::new();
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("!v1  # ALS format version 1"));
    }

    #[test]
    fn test_pretty_print_version_ctx() {
        let mut doc = AlsDocument::new();
        doc.set_ctx_format();
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("!ctx  # CTX fallback format"));
    }

    #[test]
    fn test_pretty_print_dictionary() {
        let mut doc = AlsDocument::new();
        doc.add_dictionary("default", vec!["apple".to_string(), "banana".to_string()]);
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("# Dictionaries"));
        assert!(result.contains("$default:apple|banana"));
        assert!(result.contains("_0=apple"));
        assert!(result.contains("_1=banana"));
    }

    #[test]
    fn test_pretty_print_schema() {
        let doc = AlsDocument::with_schema(vec!["id", "name", "age"]);
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("# Schema"));
        assert!(result.contains("#id #name #age"));
        assert!(result.contains("# 3 columns"));
    }

    #[test]
    fn test_pretty_print_streams() {
        let mut doc = AlsDocument::with_schema(vec!["id", "name"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("alice"),
            AlsOperator::raw("bob"),
            AlsOperator::raw("charlie"),
        ]));
        
        let printer = AlsPrettyPrinter::new();
        let result = printer.format(&doc);
        
        assert!(result.contains("# Data Streams"));
        assert!(result.contains("# Column 0: id"));
        assert!(result.contains("# Column 1: name"));
        assert!(result.contains("1>3"));
        assert!(result.contains("|  # column separator"));
    }

    #[test]
    fn test_pretty_print_with_expanded_comments() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        assert!(result.contains("1>3"));
        assert!(result.contains("/* 1, 2, 3 */"));
    }

    #[test]
    fn test_pretty_print_expanded_long_sequence() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 10),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        // Long sequences should be truncated
        assert!(result.contains("1, 2, ..., 10"));
        assert!(result.contains("10 values"));
    }

    #[test]
    fn test_pretty_print_multiply() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::multiply(AlsOperator::raw("hello"), 3),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        assert!(result.contains("hello*3"));
        assert!(result.contains("/* hello, hello, hello */"));
    }

    #[test]
    fn test_pretty_print_toggle() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::toggle("T", "F", 4),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        assert!(result.contains("T~F*4"));
        assert!(result.contains("/* T, F, T, F */"));
    }

    #[test]
    fn test_pretty_print_dict_ref() {
        let mut doc = AlsDocument::with_schema(vec!["col"]);
        doc.add_dictionary("default", vec!["apple".to_string(), "banana".to_string()]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::dict_ref(0),
            AlsOperator::dict_ref(1),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        assert!(result.contains("_0"));
        assert!(result.contains("/* apple */"));
        assert!(result.contains("_1"));
        assert!(result.contains("/* banana */"));
    }

    #[test]
    fn test_pretty_printer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsPrettyPrinter>();
    }

    #[test]
    fn test_pretty_print_complete_document() {
        let mut doc = AlsDocument::with_schema(vec!["id", "name", "status"]);
        doc.add_dictionary("default", vec!["active".to_string(), "inactive".to_string()]);
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::raw("alice"),
            AlsOperator::raw("bob"),
            AlsOperator::raw("charlie"),
        ]));
        doc.add_stream(ColumnStream::from_operators(vec![
            AlsOperator::dict_ref(0),
            AlsOperator::dict_ref(1),
            AlsOperator::dict_ref(0),
        ]));
        
        let printer = AlsPrettyPrinter::new().with_expanded_comments(true);
        let result = printer.format(&doc);
        
        // Check all sections are present
        assert!(result.contains("# ALS Document"));
        assert!(result.contains("!v1"));
        assert!(result.contains("# Dictionaries"));
        assert!(result.contains("$default:active|inactive"));
        assert!(result.contains("# Schema"));
        assert!(result.contains("#id #name #status"));
        assert!(result.contains("# Data Streams"));
        assert!(result.contains("# Column 0: id"));
        assert!(result.contains("# Column 1: name"));
        assert!(result.contains("# Column 2: status"));
    }
}
