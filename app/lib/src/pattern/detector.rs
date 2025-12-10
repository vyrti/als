//! Pattern detector trait and common types.
//!
//! This module defines the `PatternDetector` trait and associated types
//! used by all pattern detection implementations.

use crate::als::AlsOperator;

/// Trait for pattern detection algorithms.
///
/// Implementors analyze a slice of string values and attempt to detect
/// a compressible pattern. If a pattern is found, they return a
/// `DetectionResult` with the corresponding `AlsOperator`.
pub trait PatternDetector: Send + Sync {
    /// Analyze values and return the best compression operator if a pattern is found.
    ///
    /// Returns `None` if no beneficial pattern is detected.
    fn detect(&self, values: &[&str]) -> Option<DetectionResult>;
}

/// Result of pattern detection.
///
/// Contains the detected operator, compression ratio, and pattern type.
#[derive(Debug, Clone)]
pub struct DetectionResult {
    /// The ALS operator that encodes the detected pattern.
    pub operator: AlsOperator,
    /// Compression ratio (original_size / compressed_size).
    ///
    /// Higher values indicate better compression.
    /// A ratio of 1.0 means no compression benefit.
    pub compression_ratio: f64,
    /// The type of pattern detected.
    pub pattern_type: PatternType,
}

impl DetectionResult {
    /// Create a new detection result.
    pub fn new(operator: AlsOperator, compression_ratio: f64, pattern_type: PatternType) -> Self {
        Self {
            operator,
            compression_ratio,
            pattern_type,
        }
    }

    /// Create a raw (no compression) result for empty input.
    pub fn raw_empty() -> Self {
        Self {
            operator: AlsOperator::Raw(String::new()),
            compression_ratio: 1.0,
            pattern_type: PatternType::Raw,
        }
    }

    /// Create a raw (no compression) result from values.
    ///
    /// This is used as a fallback when no beneficial pattern is detected.
    pub fn raw_from_values(values: &[&str]) -> Self {
        // For raw encoding, we need to create multiple Raw operators
        // but for simplicity in the result, we represent it as a single
        // operator with the first value (the actual encoding will handle
        // multiple values separately)
        let operator = if values.is_empty() {
            AlsOperator::Raw(String::new())
        } else if values.len() == 1 {
            AlsOperator::Raw(values[0].to_string())
        } else {
            // For multiple raw values, we use a placeholder
            // The actual compression will handle each value individually
            AlsOperator::Raw(values.join(" "))
        };

        Self {
            operator,
            compression_ratio: 1.0,
            pattern_type: PatternType::Raw,
        }
    }

    /// Create a range detection result.
    pub fn range(start: i64, end: i64, step: i64, original_len: usize) -> Self {
        let operator = AlsOperator::Range { start, end, step };
        let compressed_len = Self::estimate_range_length(start, end, step);
        let original_size = original_len as f64;
        let compression_ratio = if compressed_len > 0.0 {
            original_size / compressed_len
        } else {
            1.0
        };

        Self {
            operator,
            compression_ratio,
            pattern_type: if step == 1 || step == -1 {
                PatternType::Sequential
            } else {
                PatternType::Arithmetic
            },
        }
    }

    /// Create a repeat detection result.
    pub fn repeat(value: &str, count: usize, _original_len: usize) -> Self {
        let operator = AlsOperator::Multiply {
            value: Box::new(AlsOperator::Raw(value.to_string())),
            count,
        };
        
        // Estimate compressed size: value + "*" + count_digits
        let compressed_len = value.len() as f64 + 1.0 + Self::digit_count(count) as f64;
        let original_size = (value.len() * count) as f64 + (count - 1) as f64; // values + separators
        let compression_ratio = if compressed_len > 0.0 {
            original_size / compressed_len
        } else {
            1.0
        };

        Self {
            operator,
            compression_ratio,
            pattern_type: PatternType::Repeat,
        }
    }

    /// Create a toggle detection result.
    pub fn toggle(values: Vec<String>, count: usize, original_len: usize) -> Self {
        let operator = AlsOperator::Toggle {
            values: values.clone(),
            count,
        };

        // Estimate compressed size: val1~val2*count
        let values_len: usize = values.iter().map(|v| v.len()).sum();
        let separators = values.len().saturating_sub(1); // ~ between values
        let compressed_len = values_len as f64 + separators as f64 + 1.0 + Self::digit_count(count) as f64;
        
        // Original size: all values with separators
        let original_size = original_len as f64;
        let compression_ratio = if compressed_len > 0.0 {
            original_size / compressed_len
        } else {
            1.0
        };

        Self {
            operator,
            compression_ratio,
            pattern_type: PatternType::Toggle,
        }
    }

    /// Create a repeated range detection result.
    pub fn repeated_range(start: i64, end: i64, step: i64, repeat_count: usize, original_len: usize) -> Self {
        let inner = AlsOperator::Range { start, end, step };
        let operator = AlsOperator::Multiply {
            value: Box::new(inner),
            count: repeat_count,
        };

        // Estimate compressed size: (start>end)*count or (start>end:step)*count
        let range_len = Self::estimate_range_length(start, end, step);
        let compressed_len = range_len + 3.0 + Self::digit_count(repeat_count) as f64; // () + * + count
        let original_size = original_len as f64;
        let compression_ratio = if compressed_len > 0.0 {
            original_size / compressed_len
        } else {
            1.0
        };

        Self {
            operator,
            compression_ratio,
            pattern_type: PatternType::RepeatedRange,
        }
    }

    /// Estimate the string length of a range operator.
    fn estimate_range_length(start: i64, end: i64, step: i64) -> f64 {
        let start_len = Self::digit_count_i64(start);
        let end_len = Self::digit_count_i64(end);
        
        if step == 1 || step == -1 {
            // start>end
            (start_len + 1 + end_len) as f64
        } else {
            // start>end:step
            let step_len = Self::digit_count_i64(step);
            (start_len + 1 + end_len + 1 + step_len) as f64
        }
    }

    /// Count digits in a usize.
    fn digit_count(n: usize) -> usize {
        if n == 0 {
            1
        } else {
            (n as f64).log10().floor() as usize + 1
        }
    }

    /// Count digits in an i64 (including sign).
    fn digit_count_i64(n: i64) -> usize {
        if n == 0 {
            1
        } else if n < 0 {
            1 + Self::digit_count(n.unsigned_abs() as usize)
        } else {
            Self::digit_count(n as usize)
        }
    }
}

/// Type of pattern detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternType {
    /// Sequential integer range with step 1 (e.g., 1>5).
    Sequential,
    /// Arithmetic sequence with custom step (e.g., 10>50:10).
    Arithmetic,
    /// Repeated identical values (e.g., val*n).
    Repeat,
    /// Alternating values (e.g., T~F*n).
    Toggle,
    /// Repeated range pattern (e.g., (1>3)*2).
    RepeatedRange,
    /// Repeated toggle pattern (e.g., (A~B)*2).
    RepeatedToggle,
    /// Raw values (no pattern detected).
    Raw,
}

impl PatternType {
    /// Check if this pattern type provides compression benefit.
    pub fn is_compressed(&self) -> bool {
        !matches!(self, PatternType::Raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detection_result_raw_empty() {
        let result = DetectionResult::raw_empty();
        assert_eq!(result.compression_ratio, 1.0);
        assert_eq!(result.pattern_type, PatternType::Raw);
    }

    #[test]
    fn test_detection_result_raw_from_values() {
        let values = vec!["a", "b", "c"];
        let result = DetectionResult::raw_from_values(&values);
        assert_eq!(result.compression_ratio, 1.0);
        assert_eq!(result.pattern_type, PatternType::Raw);
    }

    #[test]
    fn test_detection_result_range() {
        let result = DetectionResult::range(1, 5, 1, 5);
        assert!(result.compression_ratio > 1.0);
        assert_eq!(result.pattern_type, PatternType::Sequential);
        
        if let AlsOperator::Range { start, end, step } = result.operator {
            assert_eq!(start, 1);
            assert_eq!(end, 5);
            assert_eq!(step, 1);
        } else {
            panic!("Expected Range operator");
        }
    }

    #[test]
    fn test_detection_result_arithmetic() {
        // Use a longer sequence to ensure compression benefit
        let result = DetectionResult::range(10, 100, 10, 10);
        assert!(result.compression_ratio > 1.0);
        assert_eq!(result.pattern_type, PatternType::Arithmetic);
    }

    #[test]
    fn test_detection_result_repeat() {
        let result = DetectionResult::repeat("hello", 5, 5);
        assert!(result.compression_ratio > 1.0);
        assert_eq!(result.pattern_type, PatternType::Repeat);
    }

    #[test]
    fn test_detection_result_toggle() {
        let result = DetectionResult::toggle(vec!["T".to_string(), "F".to_string()], 10, 10);
        assert!(result.compression_ratio > 1.0);
        assert_eq!(result.pattern_type, PatternType::Toggle);
    }

    #[test]
    fn test_detection_result_repeated_range() {
        // Use a longer sequence to ensure compression benefit
        // 1,2,3,4,5 repeated 3 times = 15 values
        let result = DetectionResult::repeated_range(1, 5, 1, 3, 15);
        assert!(result.compression_ratio > 1.0);
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
    }

    #[test]
    fn test_pattern_type_is_compressed() {
        assert!(PatternType::Sequential.is_compressed());
        assert!(PatternType::Arithmetic.is_compressed());
        assert!(PatternType::Repeat.is_compressed());
        assert!(PatternType::Toggle.is_compressed());
        assert!(PatternType::RepeatedRange.is_compressed());
        assert!(!PatternType::Raw.is_compressed());
    }

    #[test]
    fn test_digit_count() {
        assert_eq!(DetectionResult::digit_count(0), 1);
        assert_eq!(DetectionResult::digit_count(1), 1);
        assert_eq!(DetectionResult::digit_count(9), 1);
        assert_eq!(DetectionResult::digit_count(10), 2);
        assert_eq!(DetectionResult::digit_count(99), 2);
        assert_eq!(DetectionResult::digit_count(100), 3);
        assert_eq!(DetectionResult::digit_count(1000), 4);
    }

    #[test]
    fn test_digit_count_i64() {
        assert_eq!(DetectionResult::digit_count_i64(0), 1);
        assert_eq!(DetectionResult::digit_count_i64(1), 1);
        assert_eq!(DetectionResult::digit_count_i64(-1), 2);
        assert_eq!(DetectionResult::digit_count_i64(100), 3);
        assert_eq!(DetectionResult::digit_count_i64(-100), 4);
    }
}
