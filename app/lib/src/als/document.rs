//! ALS document structure.
//!
//! This module defines the `AlsDocument` struct which represents a complete
//! ALS compressed document, including dictionaries, schema, and column streams.

use std::collections::HashMap;

use super::AlsOperator;

/// Represents a complete ALS document.
///
/// An ALS document consists of:
/// - A version indicator
/// - Optional dictionaries for string deduplication
/// - A schema defining column names
/// - Column streams containing compressed data
/// - A format indicator (ALS or CTX fallback)
///
/// # Thread Safety
///
/// `AlsDocument` is `Send + Sync`, meaning it can be safely shared across
/// threads. The document is typically created by compression operations and
/// then shared for serialization or further processing.
///
/// Note that while the document can be shared, concurrent mutation requires
/// external synchronization. For read-only access, no synchronization is needed.
#[derive(Debug, Clone, PartialEq)]
pub struct AlsDocument {
    /// ALS format version (currently 1).
    pub version: u8,

    /// Dictionaries for string deduplication.
    ///
    /// Keys are dictionary names, values are the dictionary entries.
    /// Dictionary references in operators use indices into these vectors.
    pub dictionaries: HashMap<String, Vec<String>>,

    /// Column schema defining the names of each column.
    ///
    /// The order of names corresponds to the order of streams.
    pub schema: Vec<String>,

    /// Column streams containing compressed data.
    ///
    /// Each stream corresponds to a column in the schema.
    pub streams: Vec<ColumnStream>,

    /// Format indicator distinguishing ALS from CTX fallback.
    pub format_indicator: FormatIndicator,
}

impl AlsDocument {
    /// Current ALS format version.
    pub const CURRENT_VERSION: u8 = 1;

    /// Create a new empty ALS document.
    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            dictionaries: HashMap::new(),
            schema: Vec::new(),
            streams: Vec::new(),
            format_indicator: FormatIndicator::Als,
        }
    }

    /// Create a new ALS document with the given schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - Column names for the document
    pub fn with_schema<S: Into<String>>(schema: Vec<S>) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            dictionaries: HashMap::new(),
            schema: schema.into_iter().map(|s| s.into()).collect(),
            streams: Vec::new(),
            format_indicator: FormatIndicator::Als,
        }
    }

    /// Add a dictionary to the document.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the dictionary
    /// * `entries` - Dictionary entries
    pub fn add_dictionary<S: Into<String>>(&mut self, name: S, entries: Vec<String>) {
        self.dictionaries.insert(name.into(), entries);
    }

    /// Add a column stream to the document.
    ///
    /// # Arguments
    ///
    /// * `stream` - The column stream to add
    pub fn add_stream(&mut self, stream: ColumnStream) {
        self.streams.push(stream);
    }

    /// Get the number of columns in the document.
    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    /// Get the number of rows in the document.
    ///
    /// This is calculated by expanding the first column stream.
    /// Returns 0 if there are no streams.
    pub fn row_count(&self) -> usize {
        self.streams
            .first()
            .map(|s| s.expanded_count())
            .unwrap_or(0)
    }

    /// Check if the document uses CTX fallback format.
    pub fn is_ctx(&self) -> bool {
        self.format_indicator == FormatIndicator::Ctx
    }

    /// Check if the document uses ALS format.
    pub fn is_als(&self) -> bool {
        self.format_indicator == FormatIndicator::Als
    }

    /// Set the format indicator to CTX.
    pub fn set_ctx_format(&mut self) {
        self.format_indicator = FormatIndicator::Ctx;
    }

    /// Set the format indicator to ALS.
    pub fn set_als_format(&mut self) {
        self.format_indicator = FormatIndicator::Als;
    }

    /// Get the default dictionary entries (if any).
    ///
    /// The default dictionary is used for `_i` references without
    /// a dictionary name prefix.
    pub fn default_dictionary(&self) -> Option<&Vec<String>> {
        self.dictionaries.get("default")
    }

    /// Validate the document structure.
    ///
    /// Checks that:
    /// - Schema and streams have the same length
    /// - All streams have the same expanded count
    ///
    /// # Returns
    ///
    /// `true` if the document is valid, `false` otherwise.
    pub fn is_valid(&self) -> bool {
        // Schema and streams must match
        if self.schema.len() != self.streams.len() {
            return false;
        }

        // All streams must have the same expanded count
        if let Some(first) = self.streams.first() {
            let expected_count = first.expanded_count();
            for stream in &self.streams[1..] {
                if stream.expanded_count() != expected_count {
                    return false;
                }
            }
        }

        true
    }
}

impl Default for AlsDocument {
    fn default() -> Self {
        Self::new()
    }
}

/// A single column's compressed representation.
///
/// Contains a sequence of operators that, when expanded, produce
/// the column's values.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStream {
    /// Operators that produce this column's values when expanded.
    pub operators: Vec<AlsOperator>,
}

impl ColumnStream {
    /// Create a new empty column stream.
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    /// Create a column stream from a vector of operators.
    pub fn from_operators(operators: Vec<AlsOperator>) -> Self {
        Self { operators }
    }

    /// Add an operator to the stream.
    pub fn push(&mut self, operator: AlsOperator) {
        self.operators.push(operator);
    }

    /// Get the number of operators in the stream.
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Calculate the total number of values this stream will produce when expanded.
    pub fn expanded_count(&self) -> usize {
        self.operators.iter().map(|op| op.expanded_count()).sum()
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Expand all operators and return the values.
    ///
    /// # Arguments
    ///
    /// * `dictionary` - Optional dictionary for resolving DictRef operators
    ///
    /// # Errors
    ///
    /// Returns an error if any DictRef references an invalid index.
    pub fn expand(&self, dictionary: Option<&[String]>) -> crate::error::Result<Vec<String>> {
        let mut result = Vec::with_capacity(self.expanded_count());
        for op in &self.operators {
            result.extend(op.expand(dictionary)?);
        }
        Ok(result)
    }
}

impl Default for ColumnStream {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<AlsOperator> for ColumnStream {
    fn from_iter<I: IntoIterator<Item = AlsOperator>>(iter: I) -> Self {
        Self {
            operators: iter.into_iter().collect(),
        }
    }
}

/// Format indicator for ALS documents.
///
/// Distinguishes between full ALS compression and CTX fallback format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum FormatIndicator {
    /// Full ALS compression with pattern operators.
    #[default]
    Als,
    /// CTX fallback format (columnar text without compression operators).
    Ctx,
}

impl FormatIndicator {
    /// Get the version prefix string for this format.
    pub fn version_prefix(&self) -> &'static str {
        match self {
            FormatIndicator::Als => "!v",
            FormatIndicator::Ctx => "!ctx",
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_als_document_new() {
        let doc = AlsDocument::new();
        assert_eq!(doc.version, AlsDocument::CURRENT_VERSION);
        assert!(doc.dictionaries.is_empty());
        assert!(doc.schema.is_empty());
        assert!(doc.streams.is_empty());
        assert_eq!(doc.format_indicator, FormatIndicator::Als);
    }

    #[test]
    fn test_als_document_with_schema() {
        let doc = AlsDocument::with_schema(vec!["id", "name", "value"]);
        assert_eq!(doc.schema, vec!["id", "name", "value"]);
        assert_eq!(doc.column_count(), 3);
    }

    #[test]
    fn test_als_document_add_dictionary() {
        let mut doc = AlsDocument::new();
        doc.add_dictionary("colors", vec!["red".to_string(), "green".to_string(), "blue".to_string()]);
        
        assert!(doc.dictionaries.contains_key("colors"));
        assert_eq!(doc.dictionaries["colors"].len(), 3);
    }

    #[test]
    fn test_als_document_add_stream() {
        let mut doc = AlsDocument::with_schema(vec!["col1"]);
        let stream = ColumnStream::from_operators(vec![
            AlsOperator::range(1, 5),
        ]);
        doc.add_stream(stream);
        
        assert_eq!(doc.streams.len(), 1);
        assert_eq!(doc.row_count(), 5);
    }

    #[test]
    fn test_als_document_format_indicator() {
        let mut doc = AlsDocument::new();
        assert!(doc.is_als());
        assert!(!doc.is_ctx());
        
        doc.set_ctx_format();
        assert!(doc.is_ctx());
        assert!(!doc.is_als());
        
        doc.set_als_format();
        assert!(doc.is_als());
    }

    #[test]
    fn test_als_document_is_valid() {
        let mut doc = AlsDocument::with_schema(vec!["col1", "col2"]);
        
        // Empty streams - valid (schema and streams both have 0 streams)
        doc.schema.clear();
        assert!(doc.is_valid());
        
        // Mismatched schema and streams
        doc.schema = vec!["col1".to_string(), "col2".to_string()];
        doc.streams = vec![ColumnStream::from_operators(vec![AlsOperator::range(1, 5)])];
        assert!(!doc.is_valid());
        
        // Matching schema and streams
        doc.streams.push(ColumnStream::from_operators(vec![AlsOperator::range(1, 5)]));
        assert!(doc.is_valid());
        
        // Mismatched row counts
        doc.streams[1] = ColumnStream::from_operators(vec![AlsOperator::range(1, 3)]);
        assert!(!doc.is_valid());
    }

    #[test]
    fn test_als_document_default_dictionary() {
        let mut doc = AlsDocument::new();
        assert!(doc.default_dictionary().is_none());
        
        doc.add_dictionary("default", vec!["a".to_string(), "b".to_string()]);
        assert!(doc.default_dictionary().is_some());
        assert_eq!(doc.default_dictionary().unwrap().len(), 2);
    }

    #[test]
    fn test_column_stream_new() {
        let stream = ColumnStream::new();
        assert!(stream.is_empty());
        assert_eq!(stream.operator_count(), 0);
        assert_eq!(stream.expanded_count(), 0);
    }

    #[test]
    fn test_column_stream_from_operators() {
        let stream = ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
            AlsOperator::raw("x"),
        ]);
        assert_eq!(stream.operator_count(), 2);
        assert_eq!(stream.expanded_count(), 4); // 3 from range + 1 from raw
    }

    #[test]
    fn test_column_stream_push() {
        let mut stream = ColumnStream::new();
        stream.push(AlsOperator::raw("a"));
        stream.push(AlsOperator::raw("b"));
        
        assert_eq!(stream.operator_count(), 2);
        assert_eq!(stream.expanded_count(), 2);
    }

    #[test]
    fn test_column_stream_expand() {
        let stream = ColumnStream::from_operators(vec![
            AlsOperator::range(1, 3),
            AlsOperator::multiply(AlsOperator::raw("x"), 2),
        ]);
        
        let values = stream.expand(None).unwrap();
        assert_eq!(values, vec!["1", "2", "3", "x", "x"]);
    }

    #[test]
    fn test_column_stream_expand_with_dict() {
        let dict = vec!["apple".to_string(), "banana".to_string()];
        let stream = ColumnStream::from_operators(vec![
            AlsOperator::dict_ref(0),
            AlsOperator::dict_ref(1),
        ]);
        
        let values = stream.expand(Some(&dict)).unwrap();
        assert_eq!(values, vec!["apple", "banana"]);
    }

    #[test]
    fn test_column_stream_from_iter() {
        let ops = vec![AlsOperator::raw("a"), AlsOperator::raw("b")];
        let stream: ColumnStream = ops.into_iter().collect();
        
        assert_eq!(stream.operator_count(), 2);
    }

    #[test]
    fn test_format_indicator_version_prefix() {
        assert_eq!(FormatIndicator::Als.version_prefix(), "!v");
        assert_eq!(FormatIndicator::Ctx.version_prefix(), "!ctx");
    }

    #[test]
    fn test_format_indicator_default() {
        assert_eq!(FormatIndicator::default(), FormatIndicator::Als);
    }

    #[test]
    fn test_document_row_count_empty() {
        let doc = AlsDocument::new();
        assert_eq!(doc.row_count(), 0);
    }

    #[test]
    fn test_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlsDocument>();
        assert_send_sync::<ColumnStream>();
        assert_send_sync::<FormatIndicator>();
    }
}
