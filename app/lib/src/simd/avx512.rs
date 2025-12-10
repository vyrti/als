//! AVX-512 SIMD implementations for x86_64.
//!
//! This module provides AVX-512-accelerated implementations of common operations
//! used in ALS compression. AVX-512 provides 512-bit wide vector operations,
//! allowing processing of 8 i64 values simultaneously.
//!
//! # Safety
//!
//! All functions in this module are unsafe and require AVX-512F support.
//! The caller must verify that AVX-512F is available before calling these functions.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Expand a range of integers into a vector using AVX-512.
///
/// Generates an arithmetic sequence from `start` to `end` (inclusive)
/// with the given `step`. Uses AVX-512 to process 8 values at a time.
///
/// # Safety
///
/// The caller must ensure that AVX-512F is available on the current CPU.
///
/// # Arguments
///
/// * `start` - The first value in the sequence
/// * `end` - The last value in the sequence (inclusive)
/// * `step` - The difference between consecutive values (must not be 0)
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
pub unsafe fn expand_range_avx512(start: i64, end: i64, step: i64) -> Vec<i64> {
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
    if count < 16 {
        return super::scalar::expand_range_scalar(start, end, step);
    }

    // Pre-allocate the vector
    let mut result = Vec::with_capacity(count);

    // AVX-512 processes 8 i64 values at a time
    let step8 = step * 8;
    
    // Create initial vector [start, start+step, ..., start+7*step]
    let initial = _mm512_set_epi64(
        start + 7 * step,
        start + 6 * step,
        start + 5 * step,
        start + 4 * step,
        start + 3 * step,
        start + 2 * step,
        start + step,
        start,
    );
    
    // Create increment vector [8*step, 8*step, ..., 8*step]
    let increment = _mm512_set1_epi64(step8);

    let mut current = initial;
    let full_iterations = count / 8;
    let remainder = count % 8;

    // Process 8 elements at a time
    let ptr = result.as_mut_ptr();
    for i in 0..full_iterations {
        _mm512_storeu_si512(ptr.add(i * 8) as *mut i64, current);
        current = _mm512_add_epi64(current, increment);
    }

    // Handle remaining elements with scalar code
    let base = full_iterations * 8;
    for i in 0..remainder {
        *ptr.add(base + i) = start + ((base + i) as i64) * step;
    }

    result.set_len(count);
    result
}

/// Find runs of consecutive identical values using AVX-512.
///
/// Returns a vector of (start_index, length) pairs representing runs
/// of identical values.
///
/// # Safety
///
/// The caller must ensure that AVX-512F is available on the current CPU.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
pub unsafe fn find_runs_avx512(values: &[i64]) -> Vec<(usize, usize)> {
    if values.len() < 16 {
        return super::scalar::find_runs_scalar(values);
    }

    let mut runs = Vec::new();
    let mut run_start = 0;
    let mut run_value = values[0];
    let mut run_length = 1;

    let len = values.len();
    let ptr = values.as_ptr();

    let mut i = 1;
    
    // Process 8 comparisons at a time where possible
    while i + 8 <= len {
        // Load current and previous values
        let curr = _mm512_loadu_si512(ptr.add(i) as *const i64);
        let prev = _mm512_loadu_si512(ptr.add(i - 1) as *const i64);
        
        // Compare for equality - returns a mask
        let eq_mask = _mm512_cmpeq_epi64_mask(curr, prev);
        
        // If all equal (mask = 0xFF), continue the run
        if eq_mask == 0xFF {
            run_length += 8;
            i += 8;
            continue;
        }
        
        // Otherwise, process each position individually
        for j in 0..8 {
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
        i += 8;
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

/// Find arithmetic sequences using AVX-512.
///
/// Returns a vector of (start_index, length, step) tuples for each sequence.
///
/// # Safety
///
/// The caller must ensure that AVX-512F is available on the current CPU.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
pub unsafe fn find_arithmetic_sequences_avx512(values: &[i64]) -> Vec<(usize, usize, i64)> {
    if values.len() < 16 {
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

    // Process differences using AVX-512
    while i + 8 <= len {
        // Calculate differences: values[i] - values[i-1] for 8 consecutive positions
        let curr = _mm512_loadu_si512(ptr.add(i) as *const i64);
        let prev = _mm512_loadu_si512(ptr.add(i - 1) as *const i64);
        let diffs = _mm512_sub_epi64(curr, prev);
        
        // Compare with expected step
        let expected = _mm512_set1_epi64(seq_step);
        let eq_mask = _mm512_cmpeq_epi64_mask(diffs, expected);
        
        // If all differences match the step
        if eq_mask == 0xFF {
            seq_length += 8;
            i += 8;
            continue;
        }
        
        // Process each position individually
        for j in 0..8 {
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
        i += 8;
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

#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::*;

    fn has_avx512() -> bool {
        std::arch::is_x86_feature_detected!("avx512f")
    }

    #[test]
    fn test_expand_range_avx512() {
        if !has_avx512() {
            println!("AVX-512 not available, skipping test");
            return;
        }

        unsafe {
            // Basic ascending range
            let result = expand_range_avx512(1, 20, 1);
            let expected: Vec<i64> = (1..=20).collect();
            assert_eq!(result, expected);

            // Larger range to exercise SIMD path
            let result = expand_range_avx512(1, 100, 1);
            assert_eq!(result.len(), 100);
            assert_eq!(result[0], 1);
            assert_eq!(result[99], 100);

            // Custom step
            let result = expand_range_avx512(0, 40, 2);
            let expected: Vec<i64> = (0..=20).map(|x| x * 2).collect();
            assert_eq!(result, expected);

            // Descending
            let result = expand_range_avx512(20, 1, -1);
            let expected: Vec<i64> = (1..=20).rev().collect();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_find_runs_avx512() {
        if !has_avx512() {
            println!("AVX-512 not available, skipping test");
            return;
        }

        unsafe {
            // Basic runs with enough elements for SIMD
            let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 5, 5, 5, 5, 5, 6, 6];
            let runs = find_runs_avx512(&values);
            assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4), (9, 2), (11, 5), (16, 2)]);

            // All same
            let values = vec![5; 32];
            let runs = find_runs_avx512(&values);
            assert_eq!(runs, vec![(0, 32)]);

            // All different
            let values: Vec<i64> = (1..=32).collect();
            let runs = find_runs_avx512(&values);
            assert_eq!(runs.len(), 32);
        }
    }

    #[test]
    fn test_find_arithmetic_sequences_avx512() {
        if !has_avx512() {
            println!("AVX-512 not available, skipping test");
            return;
        }

        unsafe {
            // Basic sequence
            let values: Vec<i64> = (1..=32).collect();
            let seqs = find_arithmetic_sequences_avx512(&values);
            assert_eq!(seqs, vec![(0, 32, 1)]);

            // Constant sequence
            let values = vec![5; 32];
            let seqs = find_arithmetic_sequences_avx512(&values);
            assert_eq!(seqs, vec![(0, 32, 0)]);
        }
    }

    #[test]
    fn test_avx512_matches_scalar() {
        if !has_avx512() {
            println!("AVX-512 not available, skipping test");
            return;
        }

        unsafe {
            // Test that AVX-512 produces same results as scalar
            for size in [20, 50, 100, 500] {
                let scalar = super::super::scalar::expand_range_scalar(1, size, 1);
                let avx512 = expand_range_avx512(1, size, 1);
                assert_eq!(scalar, avx512, "Mismatch for size {}", size);
            }

            // Test runs
            let values: Vec<i64> = vec![1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 7];
            let scalar_runs = super::super::scalar::find_runs_scalar(&values);
            let avx512_runs = find_runs_avx512(&values);
            assert_eq!(scalar_runs, avx512_runs);
        }
    }
}
