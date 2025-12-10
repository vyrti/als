//! Repetition pattern detection.
//!
//! This module detects consecutive identical values that can be encoded
//! using multiplier syntax (e.g., `val*n`).

use super::detector::{DetectionResult, PatternDetector};

/// Detector for repetition patterns.
///
/// Detects consecutive identical values that can be compressed using
/// the multiplier operator (e.g., "hello", "hello", "hello" → `hello*3`).
#[derive(Debug, Clone)]
pub struct RepeatDetector {
    min_pattern_length: usize,
}

impl RepeatDetector {
    /// Create a new repeat detector with the given minimum pattern length.
    pub fn new(min_pattern_length: usize) -> Self {
        Self { min_pattern_length }
    }

    /// Calculate the original string length of the values.
    fn calculate_original_length(values: &[&str]) -> usize {
        let value_len: usize = values.iter().map(|v| v.len()).sum();
        let separator_len = values.len().saturating_sub(1);
        value_len + separator_len
    }
}

impl PatternDetector for RepeatDetector {
    fn detect(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < self.min_pattern_length {
            return None;
        }

        // Check if all values are identical
        let first = values.first()?;
        if !values.iter().all(|v| v == first) {
            return None;
        }

        let count = values.len();
        let original_len = Self::calculate_original_length(values);
        let result = DetectionResult::repeat(first, count, original_len);

        // Only return if there's compression benefit
        if result.compression_ratio > 1.0 {
            Some(result)
        } else {
            None
        }
    }
}

/// Detector that finds runs of repeated values within a larger sequence.
///
/// Unlike `RepeatDetector` which requires all values to be identical,
/// this detector finds the longest run of consecutive identical values.
#[derive(Debug, Clone)]
pub struct RunDetector {
    min_run_length: usize,
}

impl RunDetector {
    /// Create a new run detector with the given minimum run length.
    pub fn new(min_run_length: usize) -> Self {
        Self { min_run_length }
    }

    /// Find all runs of consecutive identical values.
    ///
    /// Returns a vector of (start_index, value, count) tuples.
    pub fn find_runs<'a>(&self, values: &[&'a str]) -> Vec<(usize, &'a str, usize)> {
        if values.is_empty() {
            return Vec::new();
        }

        let mut runs = Vec::new();
        let mut run_start = 0;
        let mut run_value = values[0];
        let mut run_count = 1;

        for (i, &value) in values.iter().enumerate().skip(1) {
            if value == run_value {
                run_count += 1;
            } else {
                if run_count >= self.min_run_length {
                    runs.push((run_start, run_value, run_count));
                }
                run_start = i;
                run_value = value;
                run_count = 1;
            }
        }

        // Don't forget the last run
        if run_count >= self.min_run_length {
            runs.push((run_start, run_value, run_count));
        }

        runs
    }

    /// Find the longest run of consecutive identical values.
    ///
    /// Returns (start_index, value, count) or None if no run meets the minimum length.
    pub fn find_longest_run<'a>(&self, values: &[&'a str]) -> Option<(usize, &'a str, usize)> {
        self.find_runs(values).into_iter().max_by_key(|&(_, _, count)| count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_identical() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["hello", "hello", "hello", "hello", "hello"];
        let result = detector.detect(&values).unwrap();
        
        assert!(result.compression_ratio > 1.0);
        if let crate::als::AlsOperator::Multiply { value, count } = result.operator {
            assert_eq!(count, 5);
            if let crate::als::AlsOperator::Raw(s) = *value {
                assert_eq!(s, "hello");
            } else {
                panic!("Expected Raw operator inside Multiply");
            }
        } else {
            panic!("Expected Multiply operator");
        }
    }

    #[test]
    fn test_no_pattern_different_values() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["a", "b", "c", "d"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_too_short() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["x", "x"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_empty_string_repeat() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["", "", "", ""];
        let result = detector.detect(&values);
        // Empty strings might not provide compression benefit
        // depending on the calculation
        if let Some(r) = result {
            assert!(r.compression_ratio > 1.0);
        }
    }

    #[test]
    fn test_single_char_repeat() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["x", "x", "x", "x", "x"];
        let result = detector.detect(&values).unwrap();
        assert!(result.compression_ratio > 1.0);
    }

    #[test]
    fn test_long_string_repeat() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec![
            "this is a long string",
            "this is a long string",
            "this is a long string",
        ];
        let result = detector.detect(&values).unwrap();
        assert!(result.compression_ratio > 1.0);
    }

    #[test]
    fn test_numeric_string_repeat() {
        let detector = RepeatDetector::new(3);
        let values: Vec<&str> = vec!["42", "42", "42", "42"];
        let result = detector.detect(&values).unwrap();
        assert!(result.compression_ratio > 1.0);
    }

    // RunDetector tests

    #[test]
    fn test_find_runs_single_run() {
        let detector = RunDetector::new(3);
        let values: Vec<&str> = vec!["a", "a", "a", "a"];
        let runs = detector.find_runs(&values);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0], (0, "a", 4));
    }

    #[test]
    fn test_find_runs_multiple_runs() {
        let detector = RunDetector::new(2);
        let values: Vec<&str> = vec!["a", "a", "b", "b", "b", "c", "c"];
        let runs = detector.find_runs(&values);
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0], (0, "a", 2));
        assert_eq!(runs[1], (2, "b", 3));
        assert_eq!(runs[2], (5, "c", 2));
    }

    #[test]
    fn test_find_runs_no_runs() {
        let detector = RunDetector::new(3);
        let values: Vec<&str> = vec!["a", "b", "c", "d"];
        let runs = detector.find_runs(&values);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_find_runs_empty() {
        let detector = RunDetector::new(2);
        let values: Vec<&str> = vec![];
        let runs = detector.find_runs(&values);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_find_longest_run() {
        let detector = RunDetector::new(2);
        let values: Vec<&str> = vec!["a", "a", "b", "b", "b", "b", "c", "c"];
        let longest = detector.find_longest_run(&values).unwrap();
        assert_eq!(longest, (2, "b", 4));
    }

    #[test]
    fn test_find_longest_run_none() {
        let detector = RunDetector::new(5);
        let values: Vec<&str> = vec!["a", "a", "b", "b"];
        assert!(detector.find_longest_run(&values).is_none());
    }
}
