//! NEON SIMD implementations for ARM64.
//!
//! This module provides NEON-accelerated implementations of common operations
//! used in ALS compression. NEON provides 128-bit wide vector operations,
//! allowing processing of 2 i64 values simultaneously.
//!
//! # Safety
//!
//! All functions in this module are unsafe. While NEON is mandatory on ARM64,
//! the functions use unsafe intrinsics and require proper alignment handling.

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Expand a range of integers into a vector using NEON.
///
/// Generates an arithmetic sequence from `start` to `end` (inclusive)
/// with the given `step`. Uses NEON to process 2 values at a time.
///
/// # Safety
///
/// The caller must ensure this is called on an ARM64 platform.
///
/// # Arguments
///
/// * `start` - The first value in the sequence
/// * `end` - The last value in the sequence (inclusive)
/// * `step` - The difference between consecutive values (must not be 0)
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn expand_range_neon(start: i64, end: i64, step: i64) -> Vec<i64> {
    assert!(step != 0, "step must not be zero");

    // Calculate the number of elements
    let count = if step > 0 {
        if end >= start {
            ((end - start) / step + 1) as usize
        } else {
            0
        }
    } else {
        if start >= end {
            ((start - end) / (-step) + 1) as usize
        } else {
            0
        }
    };

    if count == 0 {
        return Vec::new();
    }

    // For small ranges, use scalar implementation
    if count < 4 {
        return super::scalar::expand_range_scalar(start, end, step);
    }

    // Pre-allocate the vector
    let mut result: Vec<i64> = Vec::with_capacity(count);

    // NEON processes 2 i64 values at a time
    let step2 = step * 2;
    
    // Create initial vector [start, start+step]
    let initial = vcombine_s64(
        vdup_n_s64(start),
        vdup_n_s64(start + step),
    );
    
    // Create increment vector [2*step, 2*step]
    let increment = vdupq_n_s64(step2);

    let mut current = initial;
    let full_iterations = count / 2;
    let remainder = count % 2;

    // Process 2 elements at a time
    let ptr = result.as_mut_ptr();
    for i in 0..full_iterations {
        vst1q_s64(ptr.add(i * 2), current);
        current = vaddq_s64(current, increment);
    }

    // Handle remaining element with scalar code
    if remainder > 0 {
        let base = full_iterations * 2;
        *ptr.add(base) = start + (base as i64) * step;
    }

    result.set_len(count);
    result
}

/// Find runs of consecutive identical values using NEON.
///
/// Returns a vector of (start_index, length) pairs representing runs
/// of identical values.
///
/// # Safety
///
/// The caller must ensure this is called on an ARM64 platform.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn find_runs_neon(values: &[i64]) -> Vec<(usize, usize)> {
    if values.len() < 4 {
        return super::scalar::find_runs_scalar(values);
    }

    let mut runs = Vec::new();
    let mut run_start = 0;
    let mut run_value = values[0];
    let mut run_length = 1;

    let len = values.len();
    let ptr = values.as_ptr();

    let mut i = 1;
    
    // Process 2 comparisons at a time where possible
    while i + 2 <= len {
        // Load current and previous values
        let curr = vld1q_s64(ptr.add(i));
        let prev = vld1q_s64(ptr.add(i - 1));
        
        // Compare for equality - vceqq_s64 returns uint64x2_t
        let eq = vceqq_s64(curr, prev);
        
        // Extract comparison results
        // NEON comparison returns all 1s for true, all 0s for false
        let eq_low = vgetq_lane_u64(eq, 0);
        let eq_high = vgetq_lane_u64(eq, 1);
        
        // If both equal, continue the run
        if eq_low == u64::MAX && eq_high == u64::MAX {
            run_length += 2;
            i += 2;
            continue;
        }
        
        // Otherwise, process each position individually
        for j in 0..2 {
            let value = *ptr.add(i + j);
            if value == run_value {
                run_length += 1;
            } else {
                runs.push((run_start, run_length));
                run_start = i + j;
                run_value = value;
                run_length = 1;
            }
        }
        i += 2;
    }

    // Handle remaining elements
    while i < len {
        let value = *ptr.add(i);
        if value == run_value {
            run_length += 1;
        } else {
            runs.push((run_start, run_length));
            run_start = i;
            run_value = value;
            run_length = 1;
        }
        i += 1;
    }

    runs.push((run_start, run_length));
    runs
}

/// Find arithmetic sequences using NEON.
///
/// Returns a vector of (start_index, length, step) tuples for each sequence.
///
/// # Safety
///
/// The caller must ensure this is called on an ARM64 platform.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn find_arithmetic_sequences_neon(values: &[i64]) -> Vec<(usize, usize, i64)> {
    if values.len() < 4 {
        return super::scalar::find_arithmetic_sequences_scalar(values);
    }

    let mut sequences = Vec::new();
    let len = values.len();
    let ptr = values.as_ptr();

    if len < 2 {
        if len == 1 {
            return vec![(0, 1, 0)];
        }
        return Vec::new();
    }

    let mut seq_start = 0;
    let mut seq_step = *ptr.add(1) - *ptr.add(0);
    let mut seq_length = 2;

    let mut i = 2;

    // Process differences using NEON
    while i + 2 <= len {
        // Calculate differences: values[i] - values[i-1] for 2 consecutive positions
        let curr = vld1q_s64(ptr.add(i));
        let prev = vld1q_s64(ptr.add(i - 1));
        let diffs = vsubq_s64(curr, prev);
        
        // Compare with expected step - vceqq_s64 returns uint64x2_t
        let expected = vdupq_n_s64(seq_step);
        let eq = vceqq_s64(diffs, expected);
        
        // Extract comparison results
        let eq_low = vgetq_lane_u64(eq, 0);
        let eq_high = vgetq_lane_u64(eq, 1);
        
        // If both differences match the step
        if eq_low == u64::MAX && eq_high == u64::MAX {
            seq_length += 2;
            i += 2;
            continue;
        }
        
        // Process each position individually
        for j in 0..2 {
            let current_step = *ptr.add(i + j) - *ptr.add(i + j - 1);
            if current_step == seq_step {
                seq_length += 1;
            } else {
                sequences.push((seq_start, seq_length, seq_step));
                seq_start = i + j - 1;
                seq_step = current_step;
                seq_length = 2;
            }
        }
        i += 2;
    }

    // Handle remaining elements
    while i < len {
        let current_step = *ptr.add(i) - *ptr.add(i - 1);
        if current_step == seq_step {
            seq_length += 1;
        } else {
            sequences.push((seq_start, seq_length, seq_step));
            seq_start = i - 1;
            seq_step = current_step;
            seq_length = 2;
        }
        i += 1;
    }

    sequences.push((seq_start, seq_length, seq_step));
    sequences
}

#[cfg(all(test, target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn test_expand_range_neon() {
        unsafe {
            // Basic ascending range
            let result = expand_range_neon(1, 10, 1);
            assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

            // Larger range to exercise SIMD path
            let result = expand_range_neon(1, 100, 1);
            assert_eq!(result.len(), 100);
            assert_eq!(result[0], 1);
            assert_eq!(result[99], 100);

            // Custom step
            let result = expand_range_neon(0, 20, 2);
            assert_eq!(result, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);

            // Descending
            let result = expand_range_neon(10, 1, -1);
            assert_eq!(result, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
        }
    }

    #[test]
    fn test_find_runs_neon() {
        unsafe {
            // Basic runs
            let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4];
            let runs = find_runs_neon(&values);
            assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4), (9, 2)]);

            // All same
            let values = vec![5; 20];
            let runs = find_runs_neon(&values);
            assert_eq!(runs, vec![(0, 20)]);

            // All different
            let values: Vec<i64> = (1..=20).collect();
            let runs = find_runs_neon(&values);
            assert_eq!(runs.len(), 20);
        }
    }

    #[test]
    fn test_find_arithmetic_sequences_neon() {
        unsafe {
            // Basic sequence
            let values: Vec<i64> = (1..=20).collect();
            let seqs = find_arithmetic_sequences_neon(&values);
            assert_eq!(seqs, vec![(0, 20, 1)]);

            // Constant sequence
            let values = vec![5; 20];
            let seqs = find_arithmetic_sequences_neon(&values);
            assert_eq!(seqs, vec![(0, 20, 0)]);
        }
    }

    #[test]
    fn test_neon_matches_scalar() {
        unsafe {
            // Test that NEON produces same results as scalar
            for size in [10, 50, 100, 500] {
                let scalar = super::super::scalar::expand_range_scalar(1, size, 1);
                let neon = expand_range_neon(1, size, 1);
                assert_eq!(scalar, neon, "Mismatch for size {}", size);
            }

            // Test runs
            let values: Vec<i64> = vec![1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 5];
            let scalar_runs = super::super::scalar::find_runs_scalar(&values);
            let neon_runs = find_runs_neon(&values);
            assert_eq!(scalar_runs, neon_runs);
        }
    }
}
