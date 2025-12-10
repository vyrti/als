//! Scalar (non-SIMD) implementations of SIMD operations.
//!
//! These implementations serve as fallbacks when SIMD instructions are not
//! available or are disabled. They provide the same functionality with
//! portable, standard Rust code.

/// Expand a range of integers into a vector (scalar implementation).
///
/// Generates an arithmetic sequence from `start` to `end` (inclusive)
/// with the given `step`.
///
/// # Arguments
///
/// * `start` - The first value in the sequence
/// * `end` - The last value in the sequence (inclusive)
/// * `step` - The difference between consecutive values (must not be 0)
///
/// # Returns
///
/// A vector containing the arithmetic sequence.
///
/// # Panics
///
/// Panics if `step` is 0.
pub fn expand_range_scalar(start: i64, end: i64, step: i64) -> Vec<i64> {
    assert!(step != 0, "step must not be zero");

    // Calculate the number of elements
    let count = if step > 0 {
        if end >= start {
            ((end - start) / step + 1) as usize
        } else {
            0
        }
    } else {
        // step < 0
        if start >= end {
            ((start - end) / (-step) + 1) as usize
        } else {
            0
        }
    };

    if count == 0 {
        return Vec::new();
    }

    // Pre-allocate the vector
    let mut result = Vec::with_capacity(count);

    // Generate the sequence
    let mut current = start;
    for _ in 0..count {
        result.push(current);
        current = current.wrapping_add(step);
    }

    result
}

/// Find runs of consecutive identical values in a slice (scalar implementation).
///
/// Returns a vector of (start_index, length) pairs representing runs
/// of identical values.
///
/// # Arguments
///
/// * `values` - The slice of values to analyze
///
/// # Returns
///
/// A vector of (start_index, length) pairs for each run.
pub fn find_runs_scalar(values: &[i64]) -> Vec<(usize, usize)> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut runs = Vec::new();
    let mut run_start = 0;
    let mut run_value = values[0];
    let mut run_length = 1;

    for (i, &value) in values.iter().enumerate().skip(1) {
        if value == run_value {
            run_length += 1;
        } else {
            runs.push((run_start, run_length));
            run_start = i;
            run_value = value;
            run_length = 1;
        }
    }

    // Don't forget the last run
    runs.push((run_start, run_length));

    runs
}

/// Find runs of consecutive identical string values (scalar implementation).
///
/// Returns a vector of (start_index, length) pairs representing runs
/// of identical string values.
///
/// # Arguments
///
/// * `values` - The slice of string values to analyze
///
/// # Returns
///
/// A vector of (start_index, length) pairs for each run.
pub fn find_string_runs_scalar(values: &[&str]) -> Vec<(usize, usize)> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut runs = Vec::new();
    let mut run_start = 0;
    let mut run_value = values[0];
    let mut run_length = 1;

    for (i, &value) in values.iter().enumerate().skip(1) {
        if value == run_value {
            run_length += 1;
        } else {
            runs.push((run_start, run_length));
            run_start = i;
            run_value = value;
            run_length = 1;
        }
    }

    // Don't forget the last run
    runs.push((run_start, run_length));

    runs
}

/// Find arithmetic sequences in a slice of integers (scalar implementation).
///
/// An arithmetic sequence is a sequence where consecutive elements differ
/// by a constant value (the step). This function finds all maximal arithmetic
/// sequences in the input.
///
/// # Arguments
///
/// * `values` - The slice of values to analyze
///
/// # Returns
///
/// A vector of (start_index, length, step) tuples for each sequence.
/// Sequences of length 1 are included with step 0.
pub fn find_arithmetic_sequences_scalar(values: &[i64]) -> Vec<(usize, usize, i64)> {
    if values.is_empty() {
        return Vec::new();
    }

    if values.len() == 1 {
        return vec![(0, 1, 0)];
    }

    let mut sequences = Vec::new();
    let mut seq_start = 0;
    let mut seq_step = values[1].wrapping_sub(values[0]);
    let mut seq_length = 2;

    for i in 2..values.len() {
        let current_step = values[i].wrapping_sub(values[i - 1]);
        
        if current_step == seq_step {
            seq_length += 1;
        } else {
            // End current sequence
            sequences.push((seq_start, seq_length, seq_step));
            
            // Start new sequence
            seq_start = i - 1;
            seq_step = current_step;
            seq_length = 2;
        }
    }

    // Don't forget the last sequence
    sequences.push((seq_start, seq_length, seq_step));

    sequences
}

/// Compare two slices for equality using scalar operations.
///
/// This is a simple wrapper around slice comparison, provided for
/// API consistency with SIMD implementations.
pub fn compare_slices_scalar(a: &[i64], b: &[i64]) -> bool {
    a == b
}

/// Sum all values in a slice (scalar implementation).
///
/// Returns the sum of all values, wrapping on overflow.
pub fn sum_scalar(values: &[i64]) -> i64 {
    values.iter().fold(0i64, |acc, &x| acc.wrapping_add(x))
}

/// Find the minimum value in a slice (scalar implementation).
///
/// Returns None if the slice is empty.
pub fn min_scalar(values: &[i64]) -> Option<i64> {
    values.iter().copied().min()
}

/// Find the maximum value in a slice (scalar implementation).
///
/// Returns None if the slice is empty.
pub fn max_scalar(values: &[i64]) -> Option<i64> {
    values.iter().copied().max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_range_ascending() {
        assert_eq!(expand_range_scalar(1, 5, 1), vec![1, 2, 3, 4, 5]);
        assert_eq!(expand_range_scalar(0, 10, 2), vec![0, 2, 4, 6, 8, 10]);
        assert_eq!(expand_range_scalar(10, 50, 10), vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_expand_range_descending() {
        assert_eq!(expand_range_scalar(5, 1, -1), vec![5, 4, 3, 2, 1]);
        assert_eq!(expand_range_scalar(10, 0, -2), vec![10, 8, 6, 4, 2, 0]);
        assert_eq!(expand_range_scalar(50, 10, -10), vec![50, 40, 30, 20, 10]);
    }

    #[test]
    fn test_expand_range_single() {
        assert_eq!(expand_range_scalar(42, 42, 1), vec![42]);
        assert_eq!(expand_range_scalar(42, 42, -1), vec![42]);
    }

    #[test]
    fn test_expand_range_empty() {
        // Ascending step but end < start
        let empty: Vec<i64> = vec![];
        assert_eq!(expand_range_scalar(5, 1, 1), empty);
        // Descending step but end > start
        assert_eq!(expand_range_scalar(1, 5, -1), empty);
    }

    #[test]
    #[should_panic(expected = "step must not be zero")]
    fn test_expand_range_zero_step() {
        expand_range_scalar(1, 5, 0);
    }

    #[test]
    fn test_expand_range_large() {
        let result = expand_range_scalar(1, 1000, 1);
        assert_eq!(result.len(), 1000);
        assert_eq!(result[0], 1);
        assert_eq!(result[999], 1000);
    }

    #[test]
    fn test_find_runs_basic() {
        let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
        let runs = find_runs_scalar(&values);
        assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4)]);
    }

    #[test]
    fn test_find_runs_empty() {
        let values: Vec<i64> = vec![];
        let runs = find_runs_scalar(&values);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_find_runs_single() {
        let values = vec![42];
        let runs = find_runs_scalar(&values);
        assert_eq!(runs, vec![(0, 1)]);
    }

    #[test]
    fn test_find_runs_all_same() {
        let values = vec![5, 5, 5, 5, 5];
        let runs = find_runs_scalar(&values);
        assert_eq!(runs, vec![(0, 5)]);
    }

    #[test]
    fn test_find_runs_all_different() {
        let values = vec![1, 2, 3, 4, 5];
        let runs = find_runs_scalar(&values);
        assert_eq!(runs, vec![(0, 1), (1, 1), (2, 1), (3, 1), (4, 1)]);
    }

    #[test]
    fn test_find_string_runs_basic() {
        let values = vec!["a", "a", "b", "b", "b", "c"];
        let runs = find_string_runs_scalar(&values);
        assert_eq!(runs, vec![(0, 2), (2, 3), (5, 1)]);
    }

    #[test]
    fn test_find_string_runs_empty() {
        let values: Vec<&str> = vec![];
        let runs = find_string_runs_scalar(&values);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_find_arithmetic_sequences_basic() {
        // The algorithm finds maximal sequences where consecutive differences are equal
        // [1,2,3,4] has step 1, then [4,10] has step 6, then [10,20,30] has step 10
        let values = vec![1, 2, 3, 4, 10, 20, 30];
        let seqs = find_arithmetic_sequences_scalar(&values);
        // Sequences: [1,2,3,4] step 1, [4,10] step 6, [10,20,30] step 10
        assert_eq!(seqs, vec![(0, 4, 1), (3, 2, 6), (4, 3, 10)]);
    }

    #[test]
    fn test_find_arithmetic_sequences_constant() {
        let values = vec![5, 5, 5, 5];
        let seqs = find_arithmetic_sequences_scalar(&values);
        assert_eq!(seqs, vec![(0, 4, 0)]);
    }

    #[test]
    fn test_find_arithmetic_sequences_single() {
        let values = vec![42];
        let seqs = find_arithmetic_sequences_scalar(&values);
        assert_eq!(seqs, vec![(0, 1, 0)]);
    }

    #[test]
    fn test_find_arithmetic_sequences_empty() {
        let values: Vec<i64> = vec![];
        let seqs = find_arithmetic_sequences_scalar(&values);
        assert!(seqs.is_empty());
    }

    #[test]
    fn test_find_arithmetic_sequences_descending() {
        let values = vec![10, 8, 6, 4, 2];
        let seqs = find_arithmetic_sequences_scalar(&values);
        assert_eq!(seqs, vec![(0, 5, -2)]);
    }

    #[test]
    fn test_compare_slices() {
        assert!(compare_slices_scalar(&[1, 2, 3], &[1, 2, 3]));
        assert!(!compare_slices_scalar(&[1, 2, 3], &[1, 2, 4]));
        assert!(!compare_slices_scalar(&[1, 2, 3], &[1, 2]));
    }

    #[test]
    fn test_sum() {
        assert_eq!(sum_scalar(&[1, 2, 3, 4, 5]), 15);
        assert_eq!(sum_scalar(&[]), 0);
        assert_eq!(sum_scalar(&[-1, 1]), 0);
    }

    #[test]
    fn test_min_max() {
        assert_eq!(min_scalar(&[3, 1, 4, 1, 5]), Some(1));
        assert_eq!(max_scalar(&[3, 1, 4, 1, 5]), Some(5));
        assert_eq!(min_scalar(&[]), None);
        assert_eq!(max_scalar(&[]), None);
    }
}
