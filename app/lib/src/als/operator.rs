//! ALS compression operators.
//!
//! This module defines the `AlsOperator` enum which represents the various
//! compression operators used in the ALS format.

use crate::config::CompressorConfig;
use crate::error::{AlsError, Result};

/// Default maximum range expansion limit.
const DEFAULT_MAX_RANGE_EXPANSION: usize = 10_000_000;

/// Represents a single ALS compression operator.
///
/// ALS uses several operators to compress data:
/// - `Raw`: Uncompressed literal values
/// - `Range`: Sequential or arithmetic sequences (`start>end` or `start>end:step`)
/// - `Multiply`: Repeated values (`val*n`)
/// - `Toggle`: Alternating patterns (`val1~val2*n`)
/// - `DictRef`: Dictionary references (`_i`)
///
/// # Serialization
///
/// This type supports zero-copy serialization via rkyv for the non-recursive
/// variants. The `Multiply` variant uses `Box<AlsOperator>` which requires
/// special handling during serialization.
#[derive(Debug, Clone, PartialEq)]
pub enum AlsOperator {
    /// Raw value: uncompressed literal string.
    ///
    /// Used when no pattern is detected or when compression would not
    /// provide benefit.
    Raw(String),

    /// Range operator: `start>end` or `start>end:step`.
    ///
    /// Represents an arithmetic sequence from `start` to `end` (inclusive)
    /// with the given `step`. When step is 1, the simple syntax `start>end`
    /// is used; otherwise `start>end:step` is used.
    ///
    /// # Examples
    ///
    /// - `1>5` expands to `1, 2, 3, 4, 5`
    /// - `10>50:10` expands to `10, 20, 30, 40, 50`
    /// - `5>1:-1` expands to `5, 4, 3, 2, 1`
    Range {
        /// Starting value of the range (inclusive)
        start: i64,
        /// Ending value of the range (inclusive)
        end: i64,
        /// Step between consecutive values (can be negative for descending)
        step: i64,
    },

    /// Multiplier operator: `val*n`.
    ///
    /// Represents a value repeated `count` times. The inner value can be
    /// any operator, allowing for nested patterns like `(1>3)*2`.
    ///
    /// # Examples
    ///
    /// - `hello*3` expands to `hello, hello, hello`
    /// - `(1>3)*2` expands to `1, 2, 3, 1, 2, 3`
    Multiply {
        /// The value to repeat (can be any operator)
        value: Box<AlsOperator>,
        /// Number of times to repeat the value
        count: usize,
    },

    /// Toggle/Alternator operator: `val1~val2*n`.
    ///
    /// Represents an alternating sequence of values. The sequence starts
    /// with the first value and alternates through all values for `count`
    /// total elements.
    ///
    /// # Examples
    ///
    /// - `T~F*4` expands to `T, F, T, F`
    /// - `A~B~C*6` expands to `A, B, C, A, B, C`
    Toggle {
        /// The values to alternate between
        values: Vec<String>,
        /// Total number of elements to generate
        count: usize,
    },

    /// Dictionary reference: `_i`.
    ///
    /// References a value from the document's dictionary by index.
    /// Dictionary references save space when the same string appears
    /// multiple times in the data.
    ///
    /// # Examples
    ///
    /// - `_0` references the first dictionary entry
    /// - `_5` references the sixth dictionary entry
    DictRef(usize),
}

impl AlsOperator {
    /// Create a new Raw operator with the given value.
    pub fn raw<S: Into<String>>(value: S) -> Self {
        AlsOperator::Raw(value.into())
    }

    /// Create a new Range operator with step 1.
    ///
    /// This is a convenience method for creating simple sequential ranges.
    /// For ranges with custom steps, use `range_with_step` or `range_safe`.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting value (inclusive)
    /// * `end` - Ending value (inclusive)
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::als::AlsOperator;
    ///
    /// let op = AlsOperator::range(1, 5);
    /// // Represents: 1, 2, 3, 4, 5
    /// ```
    pub fn range(start: i64, end: i64) -> Self {
        let step = if end >= start { 1 } else { -1 };
        AlsOperator::Range { start, end, step }
    }

    /// Create a new Range operator with a custom step.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting value (inclusive)
    /// * `end` - Ending value (inclusive)
    /// * `step` - Step between consecutive values
    ///
    /// # Panics
    ///
    /// Panics if step is 0.
    pub fn range_with_step(start: i64, end: i64, step: i64) -> Self {
        assert!(step != 0, "Step cannot be zero");
        AlsOperator::Range { start, end, step }
    }

    /// Create a new Range operator with overflow checking.
    ///
    /// This method validates that the range will not produce too many values,
    /// which could cause memory exhaustion. Use this method when creating
    /// ranges from untrusted input.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting value (inclusive)
    /// * `end` - Ending value (inclusive)
    /// * `step` - Step between consecutive values
    ///
    /// # Errors
    ///
    /// Returns `AlsError::RangeOverflow` if the range would produce more
    /// values than `DEFAULT_MAX_RANGE_EXPANSION` (10,000,000).
    ///
    /// # Examples
    ///
    /// ```
    /// use als_compression::als::AlsOperator;
    ///
    /// // Safe range
    /// let op = AlsOperator::range_safe(1, 100, 1).unwrap();
    ///
    /// // Overflow - would produce too many values
    /// let result = AlsOperator::range_safe(1, 1_000_000_000, 1);
    /// assert!(result.is_err());
    /// ```
    pub fn range_safe(start: i64, end: i64, step: i64) -> Result<Self> {
        Self::range_safe_with_limit(start, end, step, DEFAULT_MAX_RANGE_EXPANSION)
    }

    /// Create a new Range operator with overflow checking and custom limit.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting value (inclusive)
    /// * `end` - Ending value (inclusive)
    /// * `step` - Step between consecutive values
    /// * `max_expansion` - Maximum number of values allowed
    ///
    /// # Errors
    ///
    /// Returns `AlsError::RangeOverflow` if the range would produce more
    /// values than `max_expansion`.
    pub fn range_safe_with_limit(
        start: i64,
        end: i64,
        step: i64,
        max_expansion: usize,
    ) -> Result<Self> {
        if step == 0 {
            return Err(AlsError::RangeOverflow { start, end, step });
        }

        let count = Self::calculate_range_count(start, end, step);
        
        if count > max_expansion as u64 {
            return Err(AlsError::RangeOverflow { start, end, step });
        }

        Ok(AlsOperator::Range { start, end, step })
    }

    /// Create a Range operator using configuration limits.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting value (inclusive)
    /// * `end` - Ending value (inclusive)
    /// * `step` - Step between consecutive values
    /// * `config` - Compressor configuration containing limits
    pub fn range_safe_with_config(
        start: i64,
        end: i64,
        step: i64,
        config: &CompressorConfig,
    ) -> Result<Self> {
        Self::range_safe_with_limit(start, end, step, config.max_range_expansion)
    }

    /// Calculate the number of values a range would produce.
    fn calculate_range_count(start: i64, end: i64, step: i64) -> u64 {
        if step == 0 {
            return u64::MAX; // Invalid, will trigger overflow error
        }

        // Check if the range is valid (step direction matches range direction)
        let range_ascending = end >= start;
        let step_positive = step > 0;

        if range_ascending != step_positive {
            // Invalid range direction - would produce 0 or infinite values
            // Return 1 to just include the start value
            return 1;
        }

        // Calculate count safely to avoid overflow
        let diff = if range_ascending {
            (end as i128) - (start as i128)
        } else {
            (start as i128) - (end as i128)
        };

        let abs_step = (step as i128).abs();
        let count = (diff / abs_step) + 1;

        count as u64
    }

    /// Create a new Multiply operator.
    ///
    /// # Arguments
    ///
    /// * `value` - The operator to repeat
    /// * `count` - Number of times to repeat
    pub fn multiply(value: AlsOperator, count: usize) -> Self {
        AlsOperator::Multiply {
            value: Box::new(value),
            count,
        }
    }

    /// Create a new Toggle operator with two values.
    ///
    /// # Arguments
    ///
    /// * `val1` - First value in the alternation
    /// * `val2` - Second value in the alternation
    /// * `count` - Total number of elements to generate
    pub fn toggle<S1: Into<String>, S2: Into<String>>(val1: S1, val2: S2, count: usize) -> Self {
        AlsOperator::Toggle {
            values: vec![val1.into(), val2.into()],
            count,
        }
    }

    /// Create a new Toggle operator with multiple values.
    ///
    /// # Arguments
    ///
    /// * `values` - Values to alternate between
    /// * `count` - Total number of elements to generate
    pub fn toggle_multi<S: Into<String>>(values: Vec<S>, count: usize) -> Self {
        AlsOperator::Toggle {
            values: values.into_iter().map(|s| s.into()).collect(),
            count,
        }
    }

    /// Create a new DictRef operator.
    ///
    /// # Arguments
    ///
    /// * `index` - Index into the dictionary
    pub fn dict_ref(index: usize) -> Self {
        AlsOperator::DictRef(index)
    }

    /// Expand this operator into a vector of string values.
    ///
    /// This method recursively expands all operators to produce the
    /// final sequence of values.
    ///
    /// # Arguments
    ///
    /// * `dictionary` - Optional dictionary for resolving DictRef operators
    ///
    /// # Errors
    ///
    /// Returns `AlsError::InvalidDictRef` if a DictRef references an
    /// index that doesn't exist in the dictionary.
    pub fn expand(&self, dictionary: Option<&[String]>) -> Result<Vec<String>> {
        match self {
            AlsOperator::Raw(value) => Ok(vec![value.clone()]),

            AlsOperator::Range { start, end, step } => {
                let mut values = Vec::new();
                let mut current = *start;

                if *step > 0 {
                    while current <= *end {
                        values.push(current.to_string());
                        current = current.saturating_add(*step);
                        if current < *start {
                            // Overflow occurred
                            break;
                        }
                    }
                } else {
                    while current >= *end {
                        values.push(current.to_string());
                        current = current.saturating_add(*step);
                        if current > *start {
                            // Underflow occurred
                            break;
                        }
                    }
                }

                Ok(values)
            }

            AlsOperator::Multiply { value, count } => {
                let expanded = value.expand(dictionary)?;
                let mut result = Vec::with_capacity(expanded.len() * count);
                for _ in 0..*count {
                    result.extend(expanded.iter().cloned());
                }
                Ok(result)
            }

            AlsOperator::Toggle { values, count } => {
                if values.is_empty() {
                    return Ok(Vec::new());
                }
                let mut result = Vec::with_capacity(*count);
                for i in 0..*count {
                    result.push(values[i % values.len()].clone());
                }
                Ok(result)
            }

            AlsOperator::DictRef(index) => {
                let dict = dictionary.ok_or(AlsError::InvalidDictRef {
                    index: *index,
                    size: 0,
                })?;

                dict.get(*index)
                    .map(|s| vec![s.clone()])
                    .ok_or(AlsError::InvalidDictRef {
                        index: *index,
                        size: dict.len(),
                    })
            }
        }
    }

    /// Returns the number of values this operator will produce when expanded.
    ///
    /// This is useful for pre-allocating buffers or validating that
    /// expansion won't exceed limits.
    pub fn expanded_count(&self) -> usize {
        match self {
            AlsOperator::Raw(_) => 1,
            AlsOperator::Range { start, end, step } => {
                Self::calculate_range_count(*start, *end, *step) as usize
            }
            AlsOperator::Multiply { value, count } => value.expanded_count() * count,
            AlsOperator::Toggle { count, .. } => *count,
            AlsOperator::DictRef(_) => 1,
        }
    }

    /// Returns true if this operator is a Raw value.
    pub fn is_raw(&self) -> bool {
        matches!(self, AlsOperator::Raw(_))
    }

    /// Returns true if this operator is a Range.
    pub fn is_range(&self) -> bool {
        matches!(self, AlsOperator::Range { .. })
    }

    /// Returns true if this operator is a Multiply.
    pub fn is_multiply(&self) -> bool {
        matches!(self, AlsOperator::Multiply { .. })
    }

    /// Returns true if this operator is a Toggle.
    pub fn is_toggle(&self) -> bool {
        matches!(self, AlsOperator::Toggle { .. })
    }

    /// Returns true if this operator is a DictRef.
    pub fn is_dict_ref(&self) -> bool {
        matches!(self, AlsOperator::DictRef(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_operator() {
        let op = AlsOperator::raw("hello");
        assert!(op.is_raw());
        assert_eq!(op.expand(None).unwrap(), vec!["hello"]);
        assert_eq!(op.expanded_count(), 1);
    }

    #[test]
    fn test_range_ascending() {
        let op = AlsOperator::range(1, 5);
        assert!(op.is_range());
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["1", "2", "3", "4", "5"]
        );
        assert_eq!(op.expanded_count(), 5);
    }

    #[test]
    fn test_range_descending() {
        let op = AlsOperator::range(5, 1);
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["5", "4", "3", "2", "1"]
        );
        assert_eq!(op.expanded_count(), 5);
    }

    #[test]
    fn test_range_with_step() {
        let op = AlsOperator::range_with_step(10, 50, 10);
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["10", "20", "30", "40", "50"]
        );
        assert_eq!(op.expanded_count(), 5);
    }

    #[test]
    fn test_range_with_negative_step() {
        let op = AlsOperator::range_with_step(50, 10, -10);
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["50", "40", "30", "20", "10"]
        );
        assert_eq!(op.expanded_count(), 5);
    }

    #[test]
    fn test_range_safe_valid() {
        let op = AlsOperator::range_safe(1, 100, 1).unwrap();
        assert_eq!(op.expanded_count(), 100);
    }

    #[test]
    fn test_range_safe_overflow() {
        let result = AlsOperator::range_safe(1, 1_000_000_000, 1);
        assert!(matches!(result, Err(AlsError::RangeOverflow { .. })));
    }

    #[test]
    fn test_range_safe_with_limit() {
        let result = AlsOperator::range_safe_with_limit(1, 100, 1, 50);
        assert!(matches!(result, Err(AlsError::RangeOverflow { .. })));

        let result = AlsOperator::range_safe_with_limit(1, 50, 1, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_range_safe_zero_step() {
        let result = AlsOperator::range_safe(1, 10, 0);
        assert!(matches!(result, Err(AlsError::RangeOverflow { .. })));
    }

    #[test]
    fn test_multiply_operator() {
        let op = AlsOperator::multiply(AlsOperator::raw("hello"), 3);
        assert!(op.is_multiply());
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["hello", "hello", "hello"]
        );
        assert_eq!(op.expanded_count(), 3);
    }

    #[test]
    fn test_multiply_with_range() {
        let op = AlsOperator::multiply(AlsOperator::range(1, 3), 2);
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["1", "2", "3", "1", "2", "3"]
        );
        assert_eq!(op.expanded_count(), 6);
    }

    #[test]
    fn test_toggle_operator() {
        let op = AlsOperator::toggle("T", "F", 4);
        assert!(op.is_toggle());
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["T", "F", "T", "F"]
        );
        assert_eq!(op.expanded_count(), 4);
    }

    #[test]
    fn test_toggle_multi() {
        let op = AlsOperator::toggle_multi(vec!["A", "B", "C"], 6);
        assert_eq!(
            op.expand(None).unwrap(),
            vec!["A", "B", "C", "A", "B", "C"]
        );
        assert_eq!(op.expanded_count(), 6);
    }

    #[test]
    fn test_toggle_empty() {
        let op = AlsOperator::Toggle {
            values: vec![],
            count: 5,
        };
        assert_eq!(op.expand(None).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_dict_ref_valid() {
        let dict = vec!["apple".to_string(), "banana".to_string(), "cherry".to_string()];
        let op = AlsOperator::dict_ref(1);
        assert!(op.is_dict_ref());
        assert_eq!(op.expand(Some(&dict)).unwrap(), vec!["banana"]);
        assert_eq!(op.expanded_count(), 1);
    }

    #[test]
    fn test_dict_ref_invalid_index() {
        let dict = vec!["apple".to_string(), "banana".to_string()];
        let op = AlsOperator::dict_ref(5);
        let result = op.expand(Some(&dict));
        assert!(matches!(
            result,
            Err(AlsError::InvalidDictRef { index: 5, size: 2 })
        ));
    }

    #[test]
    fn test_dict_ref_no_dictionary() {
        let op = AlsOperator::dict_ref(0);
        let result = op.expand(None);
        assert!(matches!(
            result,
            Err(AlsError::InvalidDictRef { index: 0, size: 0 })
        ));
    }

    #[test]
    fn test_operator_equality() {
        let op1 = AlsOperator::range(1, 5);
        let op2 = AlsOperator::Range {
            start: 1,
            end: 5,
            step: 1,
        };
        assert_eq!(op1, op2);
    }

    #[test]
    fn test_operator_clone() {
        let op = AlsOperator::multiply(AlsOperator::range(1, 3), 2);
        let cloned = op.clone();
        assert_eq!(op, cloned);
    }

    #[test]
    fn test_single_value_range() {
        let op = AlsOperator::range(5, 5);
        assert_eq!(op.expand(None).unwrap(), vec!["5"]);
        assert_eq!(op.expanded_count(), 1);
    }

    #[test]
    fn test_nested_multiply() {
        let inner = AlsOperator::multiply(AlsOperator::raw("x"), 2);
        let outer = AlsOperator::multiply(inner, 2);
        assert_eq!(
            outer.expand(None).unwrap(),
            vec!["x", "x", "x", "x"]
        );
        assert_eq!(outer.expanded_count(), 4);
    }

    #[test]
    #[should_panic(expected = "Step cannot be zero")]
    fn test_range_with_step_zero_panics() {
        AlsOperator::range_with_step(1, 10, 0);
    }

    #[test]
    fn test_is_methods() {
        assert!(AlsOperator::raw("test").is_raw());
        assert!(!AlsOperator::raw("test").is_range());
        
        assert!(AlsOperator::range(1, 5).is_range());
        assert!(!AlsOperator::range(1, 5).is_raw());
        
        assert!(AlsOperator::multiply(AlsOperator::raw("x"), 2).is_multiply());
        assert!(AlsOperator::toggle("a", "b", 4).is_toggle());
        assert!(AlsOperator::dict_ref(0).is_dict_ref());
    }
}
