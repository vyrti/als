//! Pattern detection engine for ALS compression.
//!
//! This module provides pattern detection algorithms that analyze column data
//! and identify compressible patterns such as sequential ranges, repetitions,
//! alternations, and combined patterns.

mod detector;
mod range;
mod repeat;
mod toggle;
mod combined;

pub use detector::{DetectionResult, PatternDetector, PatternType};
pub use range::RangeDetector;
pub use repeat::{RepeatDetector, RunDetector};
pub use toggle::ToggleDetector;
pub use combined::CombinedDetector;

use crate::config::CompressorConfig;

/// Main pattern detection engine that combines all detectors.
///
/// The `PatternEngine` analyzes column values and selects the optimal
/// compression encoding by comparing results from multiple detectors.
#[derive(Debug, Clone)]
pub struct PatternEngine {
    config: CompressorConfig,
    range_detector: RangeDetector,
    repeat_detector: RepeatDetector,
    toggle_detector: ToggleDetector,
    combined_detector: CombinedDetector,
}

impl PatternEngine {
    /// Create a new pattern engine with default configuration.
    pub fn new() -> Self {
        Self::with_config(CompressorConfig::default())
    }

    /// Create a new pattern engine with the given configuration.
    pub fn with_config(config: CompressorConfig) -> Self {
        Self {
            range_detector: RangeDetector::new(config.min_pattern_length),
            repeat_detector: RepeatDetector::new(config.min_pattern_length),
            toggle_detector: ToggleDetector::new(config.min_pattern_length),
            combined_detector: CombinedDetector::new(config.min_pattern_length),
            config,
        }
    }

    /// Detect the best pattern for the given values.
    ///
    /// Analyzes the values using all available detectors and returns
    /// the result with the best compression ratio.
    pub fn detect(&self, values: &[&str]) -> DetectionResult {
        if values.is_empty() {
            return DetectionResult::raw_empty();
        }

        if values.len() < self.config.min_pattern_length {
            return DetectionResult::raw_from_values(values);
        }

        // Collect results from all detectors
        let mut best_result = DetectionResult::raw_from_values(values);

        // Try range detection (for integer sequences)
        if let Some(result) = self.range_detector.detect(values) {
            if result.compression_ratio > best_result.compression_ratio {
                best_result = result;
            }
        }

        // Try repeat detection
        if let Some(result) = self.repeat_detector.detect(values) {
            if result.compression_ratio > best_result.compression_ratio {
                best_result = result;
            }
        }

        // Try toggle detection
        if let Some(result) = self.toggle_detector.detect(values) {
            if result.compression_ratio > best_result.compression_ratio {
                best_result = result;
            }
        }

        // Try combined pattern detection
        if let Some(result) = self.combined_detector.detect(values) {
            if result.compression_ratio > best_result.compression_ratio {
                best_result = result;
            }
        }

        best_result
    }

    /// Get the minimum pattern length configuration.
    pub fn min_pattern_length(&self) -> usize {
        self.config.min_pattern_length
    }
}

impl Default for PatternEngine {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_engine_new() {
        let engine = PatternEngine::new();
        assert_eq!(engine.min_pattern_length(), 3);
    }

    #[test]
    fn test_pattern_engine_with_config() {
        let config = CompressorConfig::new().with_min_pattern_length(5);
        let engine = PatternEngine::with_config(config);
        assert_eq!(engine.min_pattern_length(), 5);
    }

    #[test]
    fn test_pattern_engine_empty_input() {
        let engine = PatternEngine::new();
        let result = engine.detect(&[]);
        assert_eq!(result.pattern_type, PatternType::Raw);
        assert_eq!(result.compression_ratio, 1.0);
    }

    #[test]
    fn test_pattern_engine_short_input() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["a", "b"];
        let result = engine.detect(&values);
        // Too short for pattern detection, should return raw
        assert_eq!(result.pattern_type, PatternType::Raw);
    }

    #[test]
    fn test_pattern_engine_selects_range() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
        let result = engine.detect(&values);
        // Should detect sequential range
        assert_eq!(result.pattern_type, PatternType::Sequential);
    }

    #[test]
    fn test_pattern_engine_selects_repeat() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["hello", "hello", "hello", "hello", "hello"];
        let result = engine.detect(&values);
        // Should detect repetition
        assert_eq!(result.pattern_type, PatternType::Repeat);
    }

    #[test]
    fn test_pattern_engine_selects_toggle() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["true", "false", "true", "false", "true", "false"];
        let result = engine.detect(&values);
        // Should detect toggle pattern
        assert_eq!(result.pattern_type, PatternType::Toggle);
    }

    #[test]
    fn test_pattern_engine_selects_repeated_range() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec![
            "1", "2", "3", "4", "5",
            "1", "2", "3", "4", "5",
            "1", "2", "3", "4", "5",
        ];
        let result = engine.detect(&values);
        // Should detect repeated range pattern
        assert_eq!(result.pattern_type, PatternType::RepeatedRange);
    }

    #[test]
    fn test_pattern_engine_falls_back_to_raw() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["apple", "banana", "cherry", "date", "elderberry"];
        let result = engine.detect(&values);
        // No pattern detected, should fall back to raw
        assert_eq!(result.pattern_type, PatternType::Raw);
    }

    #[test]
    fn test_pattern_engine_selects_best_compression() {
        let engine = PatternEngine::new();
        // This could be detected as either toggle or repeat depending on implementation
        // The engine should select the one with better compression
        let values: Vec<&str> = vec!["A", "A", "A", "A", "A", "A"];
        let result = engine.detect(&values);
        // All same values - repeat should be selected
        assert_eq!(result.pattern_type, PatternType::Repeat);
    }

    #[test]
    fn test_pattern_engine_arithmetic_sequence() {
        let engine = PatternEngine::new();
        let values: Vec<&str> = vec!["10", "20", "30", "40", "50", "60", "70", "80", "90", "100"];
        let result = engine.detect(&values);
        // Should detect arithmetic sequence
        assert_eq!(result.pattern_type, PatternType::Arithmetic);
    }

    #[test]
    fn test_pattern_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PatternEngine>();
    }
}
