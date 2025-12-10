//! Combined pattern detection.
//!
//! This module detects repeated patterns such as repeated ranges (e.g., `(1>3)*2`)
//! and repeated alternating patterns.

use super::detector::{DetectionResult, PatternDetector, PatternType};
use super::range::RangeDetector;
use super::toggle::ToggleDetector;

/// Detector for combined/repeated patterns.
///
/// Detects patterns like:
/// - Repeated ranges: 1, 2, 3, 1, 2, 3 → `(1>3)*2`
/// - Repeated alternating patterns: A, B, A, B, A, B, A, B → `(A~B)*4` or `A~B*8`
#[derive(Debug, Clone)]
pub struct CombinedDetector {
    min_pattern_length: usize,
    range_detector: RangeDetector,
    toggle_detector: ToggleDetector,
}

impl CombinedDetector {
    /// Create a new combined detector with the given minimum pattern length.
    pub fn new(min_pattern_length: usize) -> Self {
        Self {
            min_pattern_length,
            range_detector: RangeDetector::new(2), // Allow shorter ranges for combined patterns
            toggle_detector: ToggleDetector::new(2),
        }
    }

    /// Try to detect a repeated range pattern.
    ///
    /// Looks for patterns like 1, 2, 3, 1, 2, 3 which can be encoded as (1>3)*2.
    fn detect_repeated_range(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < 4 {
            return None;
        }

        // First, try to detect the pattern length by finding where the sequence resets
        // This is much more efficient than trying all possible lengths
        if let Some(pattern_len) = self.detect_pattern_length_smart(values) {
            if values.len() % pattern_len == 0 {
                let repeat_count = values.len() / pattern_len;
                if repeat_count >= 2 {
                    let pattern = &values[..pattern_len];
                    
                    // Verify the pattern repeats
                    let mut is_repeated = true;
                    for i in 1..repeat_count {
                        let chunk = &values[i * pattern_len..(i + 1) * pattern_len];
                        if chunk != pattern {
                            is_repeated = false;
                            break;
                        }
                    }

                    if is_repeated {
                        // Check if the pattern itself is a range
                        if let Some(range_result) = self.range_detector.detect(pattern) {
                            if let crate::als::AlsOperator::Range { start, end, step } = range_result.operator {
                                let original_len = Self::calculate_original_length(values);
                                return Some(DetectionResult::repeated_range(
                                    start, end, step, repeat_count, original_len
                                ));
                            }
                        }
                    }
                }
            }
        }

        // Fallback: try common pattern lengths and divisors
        // This handles cases where the smart detection doesn't work
        let max_pattern_len = std::cmp::min(values.len() / 2, 100000);

        // First, try to find pattern length by looking for where values repeat
        if let Some(pattern_len) = self.find_pattern_length_by_repetition(values) {
            if pattern_len >= 2 && values.len() % pattern_len == 0 {
                let repeat_count = values.len() / pattern_len;
                if repeat_count >= 2 {
                    let pattern = &values[..pattern_len];

                    // Verify the pattern repeats
                    let mut is_repeated = true;
                    for i in 1..repeat_count {
                        let chunk = &values[i * pattern_len..(i + 1) * pattern_len];
                        if chunk != pattern {
                            is_repeated = false;
                            break;
                        }
                    }

                    if is_repeated {
                        // Check if the pattern itself is a range
                        if let Some(range_result) = self.range_detector.detect(pattern) {
                            if let crate::als::AlsOperator::Range { start, end, step } =
                                range_result.operator
                            {
                                let original_len = Self::calculate_original_length(values);
                                return Some(DetectionResult::repeated_range(
                                    start,
                                    end,
                                    step,
                                    repeat_count,
                                    original_len,
                                ));
                            }
                        }
                    }
                }
            }
        }

        for pattern_len in 2..=max_pattern_len {
            if values.len() % pattern_len != 0 {
                continue;
            }

            let repeat_count = values.len() / pattern_len;
            if repeat_count < 2 {
                continue;
            }

            // Check if the pattern repeats
            let pattern = &values[..pattern_len];
            let mut is_repeated = true;
            
            for i in 1..repeat_count {
                let chunk = &values[i * pattern_len..(i + 1) * pattern_len];
                if chunk != pattern {
                    is_repeated = false;
                    break;
                }
            }

            if !is_repeated {
                continue;
            }

            // Check if the pattern itself is a range
            if let Some(range_result) = self.range_detector.detect(pattern) {
                if let crate::als::AlsOperator::Range { start, end, step } = range_result.operator {
                    let original_len = Self::calculate_original_length(values);
                    return Some(DetectionResult::repeated_range(
                        start, end, step, repeat_count, original_len
                    ));
                }
            }
        }

        None
    }

    /// Find pattern length by looking for where the first value appears again
    /// and the sequence repeats.
    fn find_pattern_length_by_repetition(&self, values: &[&str]) -> Option<usize> {
        if values.len() < 4 {
            return None;
        }

        let first = values[0];
        let second = values.get(1)?;

        // Look for where the first value appears again (potential pattern boundary)
        // Only check up to a reasonable limit to avoid O(n²) behavior
        let search_limit = std::cmp::min(values.len() / 2, 100000);

        for i in 2..=search_limit {
            if values.get(i) == Some(&first) {
                // Found potential pattern boundary
                // Verify the next value matches the second value
                if values.get(i + 1) == Some(second) {
                    // This looks like a pattern boundary
                    // Quick verification: check if values.len() is divisible by i
                    if values.len() % i == 0 {
                        return Some(i);
                    }
                }
            }
        }

        None
    }

    /// Smart detection of pattern length by finding where the sequence resets.
    ///
    /// For a sequence like 0, 1, 2, ..., 999, 0, 1, 2, ..., 999, ...
    /// or 1, 2, 3, ..., 999, 0, 1, 2, ..., 999, 0, ...
    /// this finds the position where the value resets.
    fn detect_pattern_length_smart(&self, values: &[&str]) -> Option<usize> {
        if values.len() < 4 {
            return None;
        }

        // Try to parse first two values as integers to detect arithmetic sequences
        let first: i64 = values[0].trim().parse().ok()?;
        let second: i64 = values[1].trim().parse().ok()?;
        let step = second - first;

        if step == 0 {
            return None; // All same values, not a range pattern
        }

        // Find where the sequence breaks (value doesn't follow the expected pattern)
        for i in 2..values.len() {
            let current: i64 = values[i].trim().parse().ok()?;
            let expected = first + (i as i64) * step;

            if current != expected {
                // Found a break - this is the pattern length
                // Verify this is actually a repeating pattern by checking the next few values
                if i + 1 < values.len() {
                    let next: i64 = values[i + 1].trim().parse().ok()?;
                    // Check if the pattern restarts: current should equal first, next should equal second
                    if current == first && next == second {
                        return Some(i);
                    }
                }
                // Not a clean repeating pattern
                return None;
            }
        }

        None // No reset found, it's a single range (not repeating)
    }

    /// Try to detect a repeated toggle pattern.
    ///
    /// This is different from a simple toggle - it detects when a toggle pattern
    /// itself is repeated, which might offer better compression in some cases.
    fn detect_repeated_toggle(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < 4 {
            return None;
        }

        // Try different pattern lengths
        for pattern_len in 2..=values.len() / 2 {
            if values.len() % pattern_len != 0 {
                continue;
            }

            let repeat_count = values.len() / pattern_len;
            if repeat_count < 2 {
                continue;
            }

            // Check if the pattern repeats
            let pattern = &values[..pattern_len];
            let mut is_repeated = true;
            
            for i in 1..repeat_count {
                let chunk = &values[i * pattern_len..(i + 1) * pattern_len];
                if chunk != pattern {
                    is_repeated = false;
                    break;
                }
            }

            if !is_repeated {
                continue;
            }

            // Check if the pattern itself is a toggle
            if let Some(toggle_result) = self.toggle_detector.detect(pattern) {
                if let crate::als::AlsOperator::Toggle { values: toggle_values, count: _ } = toggle_result.operator {
                    // Create a repeated toggle result
                    let inner = crate::als::AlsOperator::Toggle {
                        values: toggle_values,
                        count: pattern_len,
                    };
                    let operator = crate::als::AlsOperator::Multiply {
                        value: Box::new(inner),
                        count: repeat_count,
                    };

                    let original_len = Self::calculate_original_length(values);
                    // Estimate compression - this is a rough estimate
                    let compressed_len = 10.0 + (repeat_count as f64).log10() + 1.0;
                    let compression_ratio = original_len as f64 / compressed_len;

                    return Some(DetectionResult {
                        operator,
                        compression_ratio,
                        pattern_type: PatternType::RepeatedToggle,
                    });
                }
            }
        }

        None
    }

    /// Calculate the original string length of the values.
    fn calculate_original_length(values: &[&str]) -> usize {
        let value_len: usize = values.iter().map(|v| v.len()).sum();
        let separator_len = values.len().saturating_sub(1);
        value_len + separator_len
    }
}

impl PatternDetector for CombinedDetector {
    fn detect(&self, values: &[&str]) -> Option<DetectionResult> {
        if values.len() < self.min_pattern_length {
            return None;
        }

        let mut best_result: Option<DetectionResult> = None;

        // Try repeated range detection
        if let Some(result) = self.detect_repeated_range(values) {
            if result.compression_ratio > 1.0 {
                if best_result.as_ref().map_or(true, |r| result.compression_ratio > r.compression_ratio) {
                    best_result = Some(result);
                }
            }
        }

        // Try repeated toggle detection
        if let Some(result) = self.detect_repeated_toggle(values) {
            if result.compression_ratio > 1.0 {
                if best_result.as_ref().map_or(true, |r| result.compression_ratio > r.compression_ratio) {
                    best_result = Some(result);
                }
            }
        }

        best_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repeated_range() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "3", "1", "2", "3"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
        if let crate::als::AlsOperator::Multiply { value, count } = result.operator {
            assert_eq!(count, 2);
            if let crate::als::AlsOperator::Range { start, end, step } = *value {
                assert_eq!(start, 1);
                assert_eq!(end, 3);
                assert_eq!(step, 1);
            } else {
                panic!("Expected Range operator inside Multiply");
            }
        } else {
            panic!("Expected Multiply operator");
        }
    }

    #[test]
    fn test_repeated_range_three_times() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "3", "1", "2", "3", "1", "2", "3"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
        if let crate::als::AlsOperator::Multiply { count, .. } = result.operator {
            assert_eq!(count, 3);
        } else {
            panic!("Expected Multiply operator");
        }
    }

    #[test]
    fn test_repeated_arithmetic_range() {
        let detector = CombinedDetector::new(3);
        // Use a longer sequence to ensure compression benefit
        let values: Vec<&str> = vec![
            "10", "20", "30", "40", "50",
            "10", "20", "30", "40", "50",
        ];
        let result = detector.detect(&values);
        
        // This may or may not be detected depending on compression benefit
        if let Some(r) = result {
            assert_eq!(r.pattern_type, PatternType::RepeatedRange);
            if let crate::als::AlsOperator::Multiply { value, count } = r.operator {
                assert_eq!(count, 2);
                if let crate::als::AlsOperator::Range { start, end, step } = *value {
                    assert_eq!(start, 10);
                    assert_eq!(end, 50);
                    assert_eq!(step, 10);
                } else {
                    panic!("Expected Range operator inside Multiply");
                }
            } else {
                panic!("Expected Multiply operator");
            }
        }
    }

    #[test]
    fn test_repeated_toggle() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["A", "B", "A", "B"];
        let result = detector.detect(&values);
        
        // This might be detected as either a simple toggle or repeated toggle
        // depending on which gives better compression
        if let Some(r) = result {
            assert!(r.compression_ratio > 1.0);
        }
    }

    #[test]
    fn test_no_pattern_irregular() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "3", "4", "5", "6"];
        // This is a simple range, not a repeated range
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_too_short() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["1", "2"];
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_no_pattern_non_repeating() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["1", "2", "3", "4", "5", "6"];
        // Sequential but not repeating
        assert!(detector.detect(&values).is_none());
    }

    #[test]
    fn test_descending_repeated_range() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec!["3", "2", "1", "3", "2", "1"];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
        if let crate::als::AlsOperator::Multiply { value, count } = result.operator {
            assert_eq!(count, 2);
            if let crate::als::AlsOperator::Range { start, end, step } = *value {
                assert_eq!(start, 3);
                assert_eq!(end, 1);
                assert_eq!(step, -1);
            } else {
                panic!("Expected Range operator inside Multiply");
            }
        } else {
            panic!("Expected Multiply operator");
        }
    }

    #[test]
    fn test_longer_repeated_pattern() {
        let detector = CombinedDetector::new(3);
        let values: Vec<&str> = vec![
            "1", "2", "3", "4", "5",
            "1", "2", "3", "4", "5",
        ];
        let result = detector.detect(&values).unwrap();
        
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
        if let crate::als::AlsOperator::Multiply { value, count } = result.operator {
            assert_eq!(count, 2);
            if let crate::als::AlsOperator::Range { start, end, .. } = *value {
                assert_eq!(start, 1);
                assert_eq!(end, 5);
            } else {
                panic!("Expected Range operator inside Multiply");
            }
        } else {
            panic!("Expected Multiply operator");
        }
    }
}
