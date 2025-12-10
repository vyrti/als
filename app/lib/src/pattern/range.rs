//! Sequential and arithmetic range pattern detection.
//!
//! This module detects integer sequences that can be encoded using
//! range syntax (e.g., `1>5` or `10>50:10`).

use super::detector::{DetectionResult, PatternDetector};

/// Detector for sequential and arithmetic range patterns.
///
/// Detects:
/// - Sequential integers with step 1 (e.g., 1, 2, 3, 4, 5 → `1>5`)
/// - Arithmetic sequences with custom step (e.g., 10, 20, 30 → `10>30:10`)
/// - Descending sequences (e.g., 5, 4, 3, 2, 1 → `5>1`)
#[derive(Debug, Clone)]
pub struct RangeDetector {
    min_pattern_length: usize,
}

impl RangeDetector {
    /// Create a new range detector with the given minimum pattern length.
    pub fn new(min_pattern_length: usize) -> Self {
        Self { min_pattern_length }
    }

    /// Try to parse a string as an integer.
    fn parse_integer(s: &str) -> Option<i64> {
        s.trim().parse::<i64>().ok()
    }

    /// Detect a range pattern in the values.
    ///
    /// Returns the start, end, and step if a valid range is detected.
    fn detect_range(&self, values: &[i64]) -> Option<(i64, i64, i64)> {
        if values.len() < 2 {
            return None;
        }

        let start = values[0];
        let step = values[1] - values[0];

        // Step of 0 means all values are the same - not a range pattern
        if step == 0 {
            return None;
        }

        // Verify all values follow the arithmetic sequence
        for (i, &value) in values.iter().enumerate() {
            let expected = start.checked_add((i as i64).checked_mul(step)?)?;
            if value != expected {
                return None;
            }
        }

        let end = *values.last()?;
        Some((start, end, step))
    }

    /// Calculate the original string length of the values.
    fn calculate_original_length(values: &[&str]) -> usize {
        // Sum of all value lengths plus separators (spaces)
        let value_len: usize = values.iter().map(|v| v.len()).sum();
        let separator_len = values.len().saturating_sub(1);
        value_len + separator_len
    }
}

impl PatternDetector for RangeDetector {
    fn detect(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < self.min_pattern_length {
            return None;
        }

        // Try to parse all values as integers
        let integers: Option<Vec<i64>> = values.iter().map(|s| Self::parse_integer(s)).collect();
        let integers = integers?;

        // Detect range pattern
        let (start, end, step) = self.detect_range(&integers)?;

        // Calculate compression benefit
        let original_len = Self::calculate_original_length(values);
        let result = DetectionResult::range(start, end, step, original_len);

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
    use crate::pattern::PatternType;

    #[test]
    fn test_sequential_ascending() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "3", "4", "5"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::Sequential);
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 1);
            assert_eq!(end, 5);
            assert_eq!(step, 1);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_sequential_descending() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["5", "4", "3", "2", "1"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::Sequential);
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 5);
            assert_eq!(end, 1);
            assert_eq!(step, -1);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_arithmetic_sequence() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["10", "20", "30", "40", "50"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::Arithmetic);
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 10);
            assert_eq!(end, 50);
            assert_eq!(step, 10);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_arithmetic_descending() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["50", "40", "30", "20", "10"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::Arithmetic);
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 50);
            assert_eq!(end, 10);
            assert_eq!(step, -10);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_negative_numbers() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["-5", "-4", "-3", "-2", "-1"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, -5);
            assert_eq!(end, -1);
            assert_eq!(step, 1);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_no_pattern_non_integers() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["a", "b", "c"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_irregular() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "4", "5"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_too_short() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["1", "2"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_all_same() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["5", "5", "5", "5"];
        // All same values should not be detected as range (step = 0)
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_single_value() {
        let detector = RangeDetector::new(1);
        let values: Vec<&str> = vec!["42"];
        // Single value cannot form a range
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_two_values() {
        let detector = RangeDetector::new(2);
        let values: Vec<&str> = vec!["1", "2"];
        let result = detector.detect(&values);
        // Two values may not provide compression benefit (1>2 vs "1 2")
        // The result depends on compression ratio calculation
        // For very short sequences, raw encoding may be better
        if let Some(r) = result {
            assert!(r.compression_ratio > 1.0);
        }
    }

    #[test]
    fn test_whitespace_handling() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec![" 1 ", "2", " 3"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 1);
            assert_eq!(end, 3);
            assert_eq!(step, 1);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_large_step() {
        let detector = RangeDetector::new(3);
        let values: Vec<&str> = vec!["0", "100", "200", "300"];
        let result = detector.detect(&values).unwrap();
        
        if let crate::als::AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 0);
            assert_eq!(end, 300);
            assert_eq!(step, 100);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_compression_ratio() {
        let detector = RangeDetector::new(3);
        // Long sequence should have good compression
        let values: Vec<&str> = vec!["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
        let result = detector.detect(&values).unwrap();
        assert!(result.compression_ratio > 1.0);
    }
}
