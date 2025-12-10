//! Alternation/toggle pattern detection.
//!
//! This module detects alternating patterns that can be encoded using
//! toggle syntax (e.g., `T~F*n`).

use super::detector::{DetectionResult, PatternDetector};

/// Detector for alternating/toggle patterns.
///
/// Detects alternating sequences of two or more values that can be
/// compressed using the toggle operator (e.g., "T", "F", "T", "F" → `T~F*4`).
#[derive(Debug, Clone)]
pub struct ToggleDetector {
    min_pattern_length: usize,
}

impl ToggleDetector {
    /// Create a new toggle detector with the given minimum pattern length.
    pub fn new(min_pattern_length: usize) -> Self {
        Self { min_pattern_length }
    }

    /// Detect the alternating pattern in values.
    ///
    /// Returns the distinct values in order if an alternating pattern is found.
    fn detect_alternation<'a>(&self, values: &[&'a str]) -> Option<Vec<&'a str>> {
        if values.len() < 2 {
            return None;
        }

        // Find the cycle length by looking for when the pattern repeats
        // Start with assuming 2 values (most common case)
        for cycle_len in 2..=values.len().min(8) {
            if self.is_valid_cycle(values, cycle_len) {
                let cycle: Vec<&str> = values[..cycle_len].to_vec();
                // Ensure we have at least 2 distinct values
                let mut distinct = cycle.clone();
                distinct.sort();
                distinct.dedup();
                if distinct.len() >= 2 {
                    return Some(cycle);
                }
            }
        }

        None
    }

    /// Check if values follow a repeating cycle of the given length.
    fn is_valid_cycle(&self, values: &[&str], cycle_len: usize) -> bool {
        if cycle_len == 0 || values.len() < cycle_len {
            return false;
        }

        // Check that all values match the cycle pattern
        for (i, &value) in values.iter().enumerate() {
            if value != values[i % cycle_len] {
                return false;
            }
        }

        true
    }

    /// Calculate the original string length of the values.
    fn calculate_original_length(values: &[&str]) -> usize {
        let value_len: usize = values.iter().map(|v| v.len()).sum();
        let separator_len = values.len().saturating_sub(1);
        value_len + separator_len
    }
}

impl PatternDetector for ToggleDetector {
    fn detect(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < self.min_pattern_length {
            return None;
        }

        // Detect alternating pattern
        let cycle = self.detect_alternation(values)?;
        
        // Convert to owned strings for the result
        let cycle_strings: Vec<String> = cycle.iter().map(|s| s.to_string()).collect();
        
        let count = values.len();
        let original_len = Self::calculate_original_length(values);
        let result = DetectionResult::toggle(cycle_strings, count, original_len);

        // Only return if there's compression benefit
        if result.compression_ratio > 1.0 {
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_toggle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["T", "F", "T", "F", "T", "F"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Toggle { values: toggle_values, count } = result.operator {
            assert_eq!(count, 6);
            assert_eq!(toggle_values, vec!["T", "F"]);
        } else {
            panic!("Expected Toggle operator");
        }
    }

    #[test]
    fn test_boolean_toggle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["true", "false", "true", "false"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Toggle { values: toggle_values, count } = result.operator {
            assert_eq!(count, 4);
            assert_eq!(toggle_values, vec!["true", "false"]);
        } else {
            panic!("Expected Toggle operator");
        }
    }

    #[test]
    fn test_three_value_cycle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["A", "B", "C", "A", "B", "C"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Toggle { values: toggle_values, count } = result.operator {
            assert_eq!(count, 6);
            assert_eq!(toggle_values, vec!["A", "B", "C"]);
        } else {
            panic!("Expected Toggle operator");
        }
    }

    #[test]
    fn test_no_pattern_all_same() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["X", "X", "X", "X"];
        // All same values don't form a toggle pattern (need 2+ distinct)
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_irregular() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["A", "B", "A", "C"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_too_short() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["T", "F"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_numeric_toggle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["0", "1", "0", "1", "0", "1"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Toggle { values: toggle_values, count } = result.operator {
            assert_eq!(count, 6);
            assert_eq!(toggle_values, vec!["0", "1"]);
        } else {
            panic!("Expected Toggle operator");
        }
    }

    #[test]
    fn test_odd_count_toggle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec!["A", "B", "A", "B", "A"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Toggle { values: toggle_values, count } = result.operator {
            assert_eq!(count, 5);
            assert_eq!(toggle_values, vec!["A", "B"]);
        } else {
            panic!("Expected Toggle operator");
        }
    }

    #[test]
    fn test_long_strings_toggle() {
        let detector = ToggleDetector::new(3);
        let values: Vec<&str> = vec![
            "active", "inactive", "active", "inactive", "active", "inactive"
        ];
        let result = detector.detect(&values).unwrap();
        assert!(result.compression_ratio > 1.0);
    }

    #[test]
    fn test_is_valid_cycle() {
        let detector = ToggleDetector::new(2);
        
        // Valid 2-cycle
        let values = vec!["A", "B", "A", "B"];
        assert!(detector.is_valid_cycle(&values, 2));
        
        // Valid 3-cycle
        let values = vec!["A", "B", "C", "A", "B", "C"];
        assert!(detector.is_valid_cycle(&values, 3));
        
        // Invalid cycle
        let values = vec!["A", "B", "A", "C"];
        assert!(!detector.is_valid_cycle(&values, 2));
    }

    #[test]
    fn test_partial_cycle() {
        let detector = ToggleDetector::new(3);
        // Pattern that doesn't complete the last cycle
        let values: Vec<&str> = vec!["A", "B", "C", "A", "B"];
        let result = detector.detect(&values);
        // Should still detect the pattern
        assert!(result.is_some());
    }
}
