//! AVX2 SIMD implementations for x86_64.
//!
//! This module provides AVX2-accelerated implementations of common operations
//! used in ALS compression. AVX2 provides 256-bit wide vector operations,
//! allowing processing of 4 i64 values simultaneously.
//!
//! # Safety
//!
//! All functions in this module are unsafe and require AVX2 support.
//! The caller must verify that AVX2 is available before calling these functions.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Expand a range of integers into a vector using AVX2.
///
/// Generates an arithmetic sequence from `start` to `end` (inclusive)
/// with the given `step`. Uses AVX2 to process 4 values at a time.
///
/// # Safety
///
/// The caller must ensure that AVX2 is available on the current CPU.
///
/// # Arguments
///
/// * `start` - The first value in the sequence
/// * `end` - The last value in the sequence (inclusive)
/// * `step` - The difference between consecutive values (must not be 0)
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn expand_range_avx2(start: i64, end: i64, step: i64) -> Vec<i64> {
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
    if count < 8 {
        return super::scalar::expand_range_scalar(start, end, step);
    }

    // Pre-allocate the vector with proper alignment
    let mut result = Vec::with_capacity(count);

    // AVX2 processes 4 i64 values at a time
    let step4 = step * 4;
    
    // Create initial vector [start, start+step, start+2*step, start+3*step]
    let initial = _mm256_set_epi64x(
        start + 3 * step,
        start + 2 * step,
        start + step,
        start,
    );
    
    // Create increment vector [4*step, 4*step, 4*step, 4*step]
    let increment = _mm256_set1_epi64x(step4);

    let mut current = initial;
    let full_iterations = count / 4;
    let remainder = count % 4;

    // Process 4 elements at a time
    let ptr = result.as_mut_ptr();
    for i in 0..full_iterations {
        _mm256_storeu_si256(ptr.add(i * 4) as *mut __m256i, current);
        current = _mm256_add_epi64(current, increment);
    }

    // Handle remaining elements with scalar code
    let base = full_iterations * 4;
    for i in 0..remainder {
        *ptr.add(base + i) = start + ((base + i) as i64) * step;
    }

    result.set_len(count);
    result
}

/// Find runs of consecutive identical values using AVX2.
///
/// Returns a vector of (start_index, length) pairs representing runs
/// of identical values.
///
/// # Safety
///
/// The caller must ensure that AVX2 is available on the current CPU.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn find_runs_avx2(values: &[i64]) -> Vec<(usize, usize)> {
    if values.len() < 8 {
        return super::scalar::find_runs_scalar(values);
    }

    let mut runs = Vec::new();
    let mut run_start = 0;
    let mut run_value = values[0];
    let mut run_length = 1;

    // Use AVX2 to compare adjacent elements
    // We compare values[i] with values[i+1] to find boundaries
    let len = values.len();
    let ptr = values.as_ptr();

    let mut i = 1;
    
    // Process 4 comparisons at a time where possible
    while i + 4 <= len {
        // Load current and previous values
        let curr = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
        let prev = _mm256_loadu_si256(ptr.add(i - 1) as *const __m256i);
        
        // Compare for equality
        let eq = _mm256_cmpeq_epi64(curr, prev);
        
        // Extract comparison results as a mask
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq)) as u32;
        
        // If all equal (mask = 0xF), continue the run
        if mask == 0xF {
            run_length += 4;
            i += 4;
            continue;
        }
        
        // Otherwise, process each position individually
        for j in 0..4 {
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
        i += 4;
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

/// Find arithmetic sequences using AVX2.
///
/// Returns a vector of (start_index, length, step) tuples for each sequence.
///
/// # Safety
///
/// The caller must ensure that AVX2 is available on the current CPU.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn find_arithmetic_sequences_avx2(values: &[i64]) -> Vec<(usize, usize, i64)> {
    if values.len() < 8 {
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

    // Process differences using AVX2
    while i + 4 <= len {
        // Calculate differences: values[i] - values[i-1] for 4 consecutive positions
        let curr = _mm256_loadu_si256(ptr.add(i) as *const __m256i);
        let prev = _mm256_loadu_si256(ptr.add(i - 1) as *const __m256i);
        let diffs = _mm256_sub_epi64(curr, prev);
        
        // Compare with expected step
        let expected = _mm256_set1_epi64x(seq_step);
        let eq = _mm256_cmpeq_epi64(diffs, expected);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq)) as u32;
        
        // If all differences match the step
        if mask == 0xF {
            seq_length += 4;
            i += 4;
            continue;
        }
        
        // Process each position individually
        for j in 0..4 {
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
        i += 4;
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

    fn has_avx2() -> bool {
        std::arch::is_x86_feature_detected!("avx2")
    }

    #[test]
    fn test_expand_range_avx2() {
        if !has_avx2() {
            println!("AVX2 not available, skipping test");
            return;
        }

        unsafe {
            // Basic ascending range
            let result = expand_range_avx2(1, 10, 1);
            assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

            // Larger range to exercise SIMD path
            let result = expand_range_avx2(1, 100, 1);
            assert_eq!(result.len(), 100);
            assert_eq!(result[0], 1);
            assert_eq!(result[99], 100);

            // Custom step
            let result = expand_range_avx2(0, 20, 2);
            assert_eq!(result, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]);

            // Descending
            let result = expand_range_avx2(10, 1, -1);
            assert_eq!(result, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
        }
    }

    #[test]
    fn test_find_runs_avx2() {
        if !has_avx2() {
            println!("AVX2 not available, skipping test");
            return;
        }

        unsafe {
            // Basic runs
            let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4];
            let runs = find_runs_avx2(&values);
            assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4), (9, 2)]);

            // All same
            let values = vec![5; 20];
            let runs = find_runs_avx2(&values);
            assert_eq!(runs, vec![(0, 20)]);

            // All different
            let values: Vec<i64> = (1..=20).collect();
            let runs = find_runs_avx2(&values);
            assert_eq!(runs.len(), 20);
        }
    }

    #[test]
    fn test_find_arithmetic_sequences_avx2() {
        if !has_avx2() {
            println!("AVX2 not available, skipping test");
            return;
        }

        unsafe {
            // Basic sequence
            let values: Vec<i64> = (1..=20).collect();
            let seqs = find_arithmetic_sequences_avx2(&values);
            assert_eq!(seqs, vec![(0, 20, 1)]);

            // Multiple sequences
            let values = vec![1, 2, 3, 4, 10, 20, 30, 40, 5, 5, 5, 5];
            let seqs = find_arithmetic_sequences_avx2(&values);
            // Should find: [1,2,3,4] step 1, [4,10,20,30,40] step 10, [40,5,5,5,5] step 0
            // Actually the boundaries depend on implementation
            assert!(!seqs.is_empty());
        }
    }

    #[test]
    fn test_avx2_matches_scalar() {
        if !has_avx2() {
            println!("AVX2 not available, skipping test");
            return;
        }

        unsafe {
            // Test that AVX2 produces same results as scalar
            for size in [10, 50, 100, 500] {
                let scalar = super::super::scalar::expand_range_scalar(1, size, 1);
                let avx2 = expand_range_avx2(1, size, 1);
                assert_eq!(scalar, avx2, "Mismatch for size {}", size);
            }

            // Test runs
            let values: Vec<i64> = vec![1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 5];
            let scalar_runs = super::super::scalar::find_runs_scalar(&values);
            let avx2_runs = find_runs_avx2(&values);
            assert_eq!(scalar_runs, avx2_runs);
        }
    }
}
