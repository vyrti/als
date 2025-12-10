//! Dictionary builder for ALS compression.
//!
//! This module provides the `DictionaryBuilder` which tracks string frequencies
//! and builds optimal dictionaries for ALS compression. It also includes the
//! `EnumDetector` for detecting columns with limited distinct values.

use std::collections::HashMap;

use crate::config::CompressorConfig;

/// Entry in the dictionary with frequency and compression benefit information.
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryEntry {
    /// The string value.
    pub value: String,
    /// Number of occurrences in the data.
    pub frequency: usize,
    /// Estimated bytes saved by using dictionary reference.
    pub bytes_saved: i64,
}

impl DictionaryEntry {
    /// Create a new dictionary entry.
    pub fn new(value: String, frequency: usize) -> Self {
        let bytes_saved = Self::calculate_bytes_saved(&value, frequency, 0);
        Self {
            value,
            frequency,
            bytes_saved,
        }
    }

    /// Create a new dictionary entry with a specific dictionary index.
    pub fn with_index(value: String, frequency: usize, index: usize) -> Self {
        let bytes_saved = Self::calculate_bytes_saved(&value, frequency, index);
        Self {
            value,
            frequency,
            bytes_saved,
        }
    }

    /// Calculate bytes saved by using dictionary reference.
    ///
    /// Dictionary reference format: `_i` where i is the index.
    /// Bytes saved = (value_len * frequency) - (ref_len * frequency) - value_len_in_header
    fn calculate_bytes_saved(value: &str, frequency: usize, index: usize) -> i64 {
        let value_len = value.len() as i64;
        let ref_len = Self::reference_length(index) as i64;
        
        // Original cost: value appears `frequency` times
        let original_cost = value_len * frequency as i64;
        
        // Dictionary cost: value in header once + reference `frequency` times
        // Header format: $default:val1|val2|... (we count just the value + separator)
        let header_cost = value_len + 1; // value + separator (| or :)
        let reference_cost = ref_len * frequency as i64;
        let dictionary_cost = header_cost + reference_cost;
        
        original_cost - dictionary_cost
    }

    /// Calculate the length of a dictionary reference string.
    fn reference_length(index: usize) -> usize {
        // Format: _i (underscore + digits)
        1 + if index == 0 {
            1
        } else {
            (index as f64).log10().floor() as usize + 1
        }
    }

    /// Check if this entry provides compression benefit.
    pub fn provides_benefit(&self) -> bool {
        self.bytes_saved > 0
    }
}

/// Builder for creating optimal dictionaries.
///
/// Tracks string frequencies and calculates compression benefit to determine
/// which strings should be included in the dictionary.
#[derive(Debug, Clone)]
pub struct DictionaryBuilder {
    /// String frequencies.
    frequencies: HashMap<String, usize>,
    /// Maximum dictionary entries allowed.
    max_entries: usize,
}

impl DictionaryBuilder {
    /// Create a new dictionary builder with default configuration.
    pub fn new() -> Self {
        Self {
            frequencies: HashMap::new(),
            max_entries: 65_536,
        }
    }

    /// Create a new dictionary builder with the given configuration.
    pub fn with_config(config: &CompressorConfig) -> Self {
        Self {
            frequencies: HashMap::new(),
            max_entries: config.max_dictionary_entries,
        }
    }

    /// Create a new dictionary builder with a specific max entries limit.
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            frequencies: HashMap::new(),
            max_entries,
        }
    }

    /// Add a value to track.
    pub fn add(&mut self, value: &str) {
        *self.frequencies.entry(value.to_string()).or_insert(0) += 1;
    }

    /// Add multiple values to track.
    pub fn add_all<'a, I>(&mut self, values: I)
    where
        I: IntoIterator<Item = &'a str>,
    {
        for value in values {
            self.add(value);
        }
    }

    /// Add values from a column (as string representations).
    pub fn add_column_values<'a, I>(&mut self, values: I)
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.add_all(values);
    }

    /// Get the frequency of a value.
    pub fn frequency(&self, value: &str) -> usize {
        self.frequencies.get(value).copied().unwrap_or(0)
    }

    /// Get the number of distinct values tracked.
    pub fn distinct_count(&self) -> usize {
        self.frequencies.len()
    }

    /// Check if the builder has any values.
    pub fn is_empty(&self) -> bool {
        self.frequencies.is_empty()
    }

    /// Clear all tracked values.
    pub fn clear(&mut self) {
        self.frequencies.clear();
    }

    /// Build the optimal dictionary.
    ///
    /// Returns a vector of strings that should be included in the dictionary,
    /// ordered by compression benefit (highest benefit first).
    pub fn build(&self) -> Vec<String> {
        let entries = self.build_entries();
        entries.into_iter().map(|e| e.value).collect()
    }

    /// Build dictionary entries with full metadata.
    ///
    /// Returns entries sorted by compression benefit (highest first).
    pub fn build_entries(&self) -> Vec<DictionaryEntry> {
        // Filter to values that appear more than once
        let mut candidates: Vec<_> = self
            .frequencies
            .iter()
            .filter(|(_, &freq)| freq > 1)
            .collect();

        // Sort by frequency (descending) to assign lower indices to more frequent values
        candidates.sort_by(|a, b| b.1.cmp(a.1));

        // Calculate bytes saved for each candidate with their assigned index
        let mut entries: Vec<DictionaryEntry> = candidates
            .iter()
            .enumerate()
            .map(|(index, (value, &frequency))| {
                DictionaryEntry::with_index(value.to_string(), frequency, index)
            })
            .filter(|e| e.provides_benefit())
            .collect();

        // Re-sort by bytes saved (descending) for final ordering
        entries.sort_by(|a, b| b.bytes_saved.cmp(&a.bytes_saved));

        // Limit to max entries
        entries.truncate(self.max_entries);

        entries
    }

    /// Check if building a dictionary would provide compression benefit.
    pub fn has_benefit(&self) -> bool {
        self.frequencies.values().any(|&freq| freq > 1)
            && self.build_entries().iter().any(|e| e.provides_benefit())
    }

    /// Calculate the total bytes saved by using the optimal dictionary.
    pub fn total_bytes_saved(&self) -> i64 {
        self.build_entries().iter().map(|e| e.bytes_saved).sum()
    }

    /// Get all tracked values with their frequencies.
    pub fn frequencies(&self) -> &HashMap<String, usize> {
        &self.frequencies
    }
}

impl Default for DictionaryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Detector for enum-like and boolean columns.
///
/// Identifies columns with limited distinct values that can benefit from
/// dictionary optimization or toggle encoding.
#[derive(Debug, Clone)]
pub struct EnumDetector {
    /// Maximum distinct values to consider for enum detection.
    max_distinct_values: usize,
}

impl EnumDetector {
    /// Default maximum distinct values for enum detection.
    pub const DEFAULT_MAX_DISTINCT: usize = 16;

    /// Create a new enum detector with default settings.
    pub fn new() -> Self {
        Self {
            max_distinct_values: Self::DEFAULT_MAX_DISTINCT,
        }
    }

    /// Create a new enum detector with a custom max distinct values limit.
    pub fn with_max_distinct(max_distinct_values: usize) -> Self {
        Self {
            max_distinct_values,
        }
    }

    /// Detect if a column is boolean-like (exactly 2 distinct values).
    ///
    /// Returns the two values if the column is boolean-like, with the
    /// "true" value first if boolean normalization is possible.
    pub fn is_boolean_column(&self, values: &[&str]) -> Option<(String, String)> {
        let distinct = self.get_distinct_values(values);
        
        if distinct.len() != 2 {
            return None;
        }

        let mut vals: Vec<_> = distinct.into_iter().collect();
        
        // Try to normalize boolean values
        if let (Some(b1), Some(b2)) = (
            Self::normalize_boolean(&vals[0]),
            Self::normalize_boolean(&vals[1]),
        ) {
            // Ensure true value comes first
            if b1 {
                Some((vals[0].clone(), vals[1].clone()))
            } else if b2 {
                Some((vals[1].clone(), vals[0].clone()))
            } else {
                // Both normalize to false - shouldn't happen with 2 distinct values
                Some((vals[0].clone(), vals[1].clone()))
            }
        } else {
            // Not boolean-like, but still 2 distinct values
            vals.sort(); // Consistent ordering
            Some((vals[0].clone(), vals[1].clone()))
        }
    }

    /// Detect if a column is enum-like (few distinct values).
    ///
    /// Returns the distinct values if the column qualifies as enum-like.
    pub fn is_enum_column(&self, values: &[&str]) -> Option<Vec<String>> {
        let distinct = self.get_distinct_values(values);
        
        if distinct.len() <= self.max_distinct_values && distinct.len() > 1 {
            let mut vals: Vec<_> = distinct.into_iter().collect();
            vals.sort(); // Consistent ordering
            Some(vals)
        } else {
            None
        }
    }

    /// Get the distinct values in a column.
    pub fn get_distinct_values(&self, values: &[&str]) -> Vec<String> {
        let mut seen = HashMap::new();
        for &value in values {
            seen.entry(value.to_string()).or_insert(());
        }
        seen.into_keys().collect()
    }

    /// Count distinct values in a column.
    pub fn count_distinct(&self, values: &[&str]) -> usize {
        let mut seen = HashMap::new();
        for &value in values {
            seen.entry(value).or_insert(());
        }
        seen.len()
    }

    /// Normalize a boolean representation to a bool value.
    ///
    /// Recognizes various boolean representations:
    /// - true/false
    /// - 1/0
    /// - yes/no
    /// - y/n
    /// - t/f
    pub fn normalize_boolean(value: &str) -> Option<bool> {
        match value.to_lowercase().as_str() {
            "true" | "1" | "yes" | "y" | "t" => Some(true),
            "false" | "0" | "no" | "n" | "f" => Some(false),
            _ => None,
        }
    }

    /// Check if a value is a recognized boolean representation.
    pub fn is_boolean_value(value: &str) -> bool {
        Self::normalize_boolean(value).is_some()
    }

    /// Check if all values in a column are boolean representations.
    pub fn all_boolean_values(&self, values: &[&str]) -> bool {
        values.iter().all(|v| Self::is_boolean_value(v))
    }

    /// Build a dictionary for an enum-like column.
    ///
    /// Returns a dictionary optimized for the column's distinct values,
    /// or None if the column doesn't qualify as enum-like.
    pub fn build_enum_dictionary(&self, values: &[&str]) -> Option<Vec<String>> {
        self.is_enum_column(values)
    }

    /// Get the maximum distinct values setting.
    pub fn max_distinct_values(&self) -> usize {
        self.max_distinct_values
    }
}

impl Default for EnumDetector {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // DictionaryEntry tests

    #[test]
    fn test_dictionary_entry_new() {
        let entry = DictionaryEntry::new("hello".to_string(), 5);
        assert_eq!(entry.value, "hello");
        assert_eq!(entry.frequency, 5);
    }

    #[test]
    fn test_dictionary_entry_with_index() {
        let entry = DictionaryEntry::with_index("hello".to_string(), 5, 0);
        assert_eq!(entry.value, "hello");
        assert_eq!(entry.frequency, 5);
        // With index 0, reference is "_0" (2 chars)
        // Original: 5 * 5 = 25 bytes
        // Dictionary: 5 (header) + 1 (separator) + 5 * 2 (refs) = 16 bytes
        // Saved: 25 - 16 = 9 bytes
        assert!(entry.bytes_saved > 0);
    }

    #[test]
    fn test_dictionary_entry_provides_benefit() {
        // Long string appearing many times - should provide benefit
        let entry = DictionaryEntry::with_index("long_string_value".to_string(), 10, 0);
        assert!(entry.provides_benefit());

        // Short string appearing few times - may not provide benefit
        let entry = DictionaryEntry::with_index("a".to_string(), 2, 0);
        // "a" appears 2 times: original = 2 bytes
        // Dictionary: 1 (value) + 1 (sep) + 2*2 (refs) = 6 bytes
        // This doesn't provide benefit
        assert!(!entry.provides_benefit());
    }

    #[test]
    fn test_dictionary_entry_reference_length() {
        // Index 0: "_0" = 2 chars
        assert_eq!(DictionaryEntry::reference_length(0), 2);
        // Index 9: "_9" = 2 chars
        assert_eq!(DictionaryEntry::reference_length(9), 2);
        // Index 10: "_10" = 3 chars
        assert_eq!(DictionaryEntry::reference_length(10), 3);
        // Index 99: "_99" = 3 chars
        assert_eq!(DictionaryEntry::reference_length(99), 3);
        // Index 100: "_100" = 4 chars
        assert_eq!(DictionaryEntry::reference_length(100), 4);
    }

    // DictionaryBuilder tests

    #[test]
    fn test_dictionary_builder_new() {
        let builder = DictionaryBuilder::new();
        assert!(builder.is_empty());
        assert_eq!(builder.distinct_count(), 0);
    }

    #[test]
    fn test_dictionary_builder_add() {
        let mut builder = DictionaryBuilder::new();
        builder.add("hello");
        builder.add("world");
        builder.add("hello");

        assert_eq!(builder.distinct_count(), 2);
        assert_eq!(builder.frequency("hello"), 2);
        assert_eq!(builder.frequency("world"), 1);
        assert_eq!(builder.frequency("unknown"), 0);
    }

    #[test]
    fn test_dictionary_builder_add_all() {
        let mut builder = DictionaryBuilder::new();
        builder.add_all(["a", "b", "a", "c", "a", "b"].iter().copied());

        assert_eq!(builder.distinct_count(), 3);
        assert_eq!(builder.frequency("a"), 3);
        assert_eq!(builder.frequency("b"), 2);
        assert_eq!(builder.frequency("c"), 1);
    }

    #[test]
    fn test_dictionary_builder_clear() {
        let mut builder = DictionaryBuilder::new();
        builder.add("hello");
        builder.add("world");
        
        assert!(!builder.is_empty());
        builder.clear();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_dictionary_builder_build_empty() {
        let builder = DictionaryBuilder::new();
        let dict = builder.build();
        assert!(dict.is_empty());
    }

    #[test]
    fn test_dictionary_builder_build_no_repeats() {
        let mut builder = DictionaryBuilder::new();
        builder.add("a");
        builder.add("b");
        builder.add("c");

        // No value appears more than once, so no dictionary benefit
        let dict = builder.build();
        assert!(dict.is_empty());
    }

    #[test]
    fn test_dictionary_builder_build_with_repeats() {
        let mut builder = DictionaryBuilder::new();
        // Add a long string many times to ensure benefit
        for _ in 0..10 {
            builder.add("long_repeated_value");
        }
        builder.add("single_occurrence");

        let dict = builder.build();
        // Should include the repeated value
        assert!(dict.contains(&"long_repeated_value".to_string()));
        // Should not include single occurrence
        assert!(!dict.contains(&"single_occurrence".to_string()));
    }

    #[test]
    fn test_dictionary_builder_build_entries() {
        let mut builder = DictionaryBuilder::new();
        for _ in 0..20 {
            builder.add("very_long_string_value");
        }
        for _ in 0..10 {
            builder.add("another_long_value");
        }

        let entries = builder.build_entries();
        assert!(!entries.is_empty());
        
        // All entries should provide benefit
        for entry in &entries {
            assert!(entry.provides_benefit());
        }
    }

    #[test]
    fn test_dictionary_builder_has_benefit() {
        let mut builder = DictionaryBuilder::new();
        assert!(!builder.has_benefit());

        // Add values that don't provide benefit
        builder.add("a");
        builder.add("b");
        assert!(!builder.has_benefit());

        // Add values that do provide benefit
        for _ in 0..10 {
            builder.add("long_repeated_value");
        }
        assert!(builder.has_benefit());
    }

    #[test]
    fn test_dictionary_builder_total_bytes_saved() {
        let mut builder = DictionaryBuilder::new();
        
        // Empty builder saves nothing
        assert_eq!(builder.total_bytes_saved(), 0);

        // Add beneficial entries
        for _ in 0..20 {
            builder.add("long_string_value");
        }
        
        assert!(builder.total_bytes_saved() > 0);
    }

    #[test]
    fn test_dictionary_builder_max_entries() {
        let mut builder = DictionaryBuilder::with_max_entries(2);
        
        // Add many different repeated values
        for i in 0..10 {
            let value = format!("long_value_{:03}", i);
            for _ in 0..20 {
                builder.add(&value);
            }
        }

        let dict = builder.build();
        // Should be limited to max_entries
        assert!(dict.len() <= 2);
    }

    #[test]
    fn test_dictionary_builder_with_config() {
        let config = CompressorConfig::new().with_max_dictionary_entries(100);
        let builder = DictionaryBuilder::with_config(&config);
        assert_eq!(builder.max_entries, 100);
    }

    // EnumDetector tests

    #[test]
    fn test_enum_detector_new() {
        let detector = EnumDetector::new();
        assert_eq!(detector.max_distinct_values(), EnumDetector::DEFAULT_MAX_DISTINCT);
    }

    #[test]
    fn test_enum_detector_with_max_distinct() {
        let detector = EnumDetector::with_max_distinct(5);
        assert_eq!(detector.max_distinct_values(), 5);
    }

    #[test]
    fn test_enum_detector_is_boolean_column_true_false() {
        let detector = EnumDetector::new();
        let values = vec!["true", "false", "true", "false"];
        
        let result = detector.is_boolean_column(&values);
        assert!(result.is_some());
        
        let (val1, val2) = result.unwrap();
        // True value should come first
        assert_eq!(val1, "true");
        assert_eq!(val2, "false");
    }

    #[test]
    fn test_enum_detector_is_boolean_column_yes_no() {
        let detector = EnumDetector::new();
        let values = vec!["yes", "no", "yes", "no"];
        
        let result = detector.is_boolean_column(&values);
        assert!(result.is_some());
        
        let (val1, val2) = result.unwrap();
        // True value should come first
        assert_eq!(val1, "yes");
        assert_eq!(val2, "no");
    }

    #[test]
    fn test_enum_detector_is_boolean_column_one_zero() {
        let detector = EnumDetector::new();
        let values = vec!["1", "0", "1", "0"];
        
        let result = detector.is_boolean_column(&values);
        assert!(result.is_some());
        
        let (val1, val2) = result.unwrap();
        // True value should come first
        assert_eq!(val1, "1");
        assert_eq!(val2, "0");
    }

    #[test]
    fn test_enum_detector_is_boolean_column_non_boolean() {
        let detector = EnumDetector::new();
        let values = vec!["apple", "banana", "apple", "banana"];
        
        let result = detector.is_boolean_column(&values);
        assert!(result.is_some());
        
        // Non-boolean values should be sorted alphabetically
        let (val1, val2) = result.unwrap();
        assert_eq!(val1, "apple");
        assert_eq!(val2, "banana");
    }

    #[test]
    fn test_enum_detector_is_boolean_column_not_two_values() {
        let detector = EnumDetector::new();
        
        // Single value
        let values = vec!["true", "true", "true"];
        assert!(detector.is_boolean_column(&values).is_none());
        
        // Three values
        let values = vec!["a", "b", "c"];
        assert!(detector.is_boolean_column(&values).is_none());
    }

    #[test]
    fn test_enum_detector_is_enum_column() {
        let detector = EnumDetector::new();
        let values = vec!["red", "green", "blue", "red", "green"];
        
        let result = detector.is_enum_column(&values);
        assert!(result.is_some());
        
        let distinct = result.unwrap();
        assert_eq!(distinct.len(), 3);
        assert!(distinct.contains(&"red".to_string()));
        assert!(distinct.contains(&"green".to_string()));
        assert!(distinct.contains(&"blue".to_string()));
    }

    #[test]
    fn test_enum_detector_is_enum_column_too_many_distinct() {
        let detector = EnumDetector::with_max_distinct(3);
        let values = vec!["a", "b", "c", "d", "e"];
        
        // 5 distinct values exceeds max of 3
        assert!(detector.is_enum_column(&values).is_none());
    }

    #[test]
    fn test_enum_detector_is_enum_column_single_value() {
        let detector = EnumDetector::new();
        let values = vec!["same", "same", "same"];
        
        // Single distinct value doesn't qualify as enum
        assert!(detector.is_enum_column(&values).is_none());
    }

    #[test]
    fn test_enum_detector_get_distinct_values() {
        let detector = EnumDetector::new();
        let values = vec!["a", "b", "a", "c", "b", "a"];
        
        let distinct = detector.get_distinct_values(&values);
        assert_eq!(distinct.len(), 3);
    }

    #[test]
    fn test_enum_detector_count_distinct() {
        let detector = EnumDetector::new();
        let values = vec!["a", "b", "a", "c", "b", "a"];
        
        assert_eq!(detector.count_distinct(&values), 3);
    }

    #[test]
    fn test_enum_detector_normalize_boolean() {
        // True values
        assert_eq!(EnumDetector::normalize_boolean("true"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("TRUE"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("True"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("1"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("yes"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("YES"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("y"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("Y"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("t"), Some(true));
        assert_eq!(EnumDetector::normalize_boolean("T"), Some(true));

        // False values
        assert_eq!(EnumDetector::normalize_boolean("false"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("FALSE"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("False"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("0"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("no"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("NO"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("n"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("N"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("f"), Some(false));
        assert_eq!(EnumDetector::normalize_boolean("F"), Some(false));

        // Non-boolean values
        assert_eq!(EnumDetector::normalize_boolean("maybe"), None);
        assert_eq!(EnumDetector::normalize_boolean("2"), None);
        assert_eq!(EnumDetector::normalize_boolean(""), None);
    }

    #[test]
    fn test_enum_detector_is_boolean_value() {
        assert!(EnumDetector::is_boolean_value("true"));
        assert!(EnumDetector::is_boolean_value("false"));
        assert!(EnumDetector::is_boolean_value("1"));
        assert!(EnumDetector::is_boolean_value("0"));
        assert!(!EnumDetector::is_boolean_value("maybe"));
        assert!(!EnumDetector::is_boolean_value("hello"));
    }

    #[test]
    fn test_enum_detector_all_boolean_values() {
        let detector = EnumDetector::new();
        
        let values = vec!["true", "false", "true", "false"];
        assert!(detector.all_boolean_values(&values));
        
        let values = vec!["1", "0", "1", "0"];
        assert!(detector.all_boolean_values(&values));
        
        let values = vec!["yes", "no", "maybe"];
        assert!(!detector.all_boolean_values(&values));
    }

    #[test]
    fn test_enum_detector_build_enum_dictionary() {
        let detector = EnumDetector::new();
        let values = vec!["red", "green", "blue", "red", "green"];
        
        let dict = detector.build_enum_dictionary(&values);
        assert!(dict.is_some());
        
        let dict = dict.unwrap();
        assert_eq!(dict.len(), 3);
    }

    #[test]
    fn test_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DictionaryBuilder>();
        assert_send_sync::<DictionaryEntry>();
        assert_send_sync::<EnumDetector>();
    }
}
