//! SIMD dispatcher with runtime CPU feature detection.
//!
//! This module provides the main entry point for SIMD-accelerated operations,
//! automatically selecting the best available implementation based on CPU features.

use crate::config::SimdConfig;

/// Detected CPU features for SIMD acceleration.
///
/// This struct holds the results of runtime CPU feature detection,
/// indicating which SIMD instruction sets are available on the current CPU.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuFeatures {
    /// AVX-512F (Foundation) is available (x86_64 only).
    pub avx512: bool,
    /// AVX2 is available (x86_64 only).
    pub avx2: bool,
    /// NEON is available (ARM64 only).
    pub neon: bool,
}

impl CpuFeatures {
    /// Detect CPU features at runtime.
    ///
    /// This function queries the CPU to determine which SIMD instruction sets
    /// are available. The detection is performed once and the results can be
    /// cached for the lifetime of the program.
    #[cfg(target_arch = "x86_64")]
    pub fn detect() -> Self {
        Self {
            avx512: std::arch::is_x86_feature_detected!("avx512f"),
            avx2: std::arch::is_x86_feature_detected!("avx2"),
            neon: false,
        }
    }

    /// Detect CPU features at runtime (ARM64 version).
    #[cfg(target_arch = "aarch64")]
    pub fn detect() -> Self {
        // NEON is mandatory on ARM64, so it's always available
        Self {
            avx512: false,
            avx2: false,
            neon: true,
        }
    }

    /// Detect CPU features at runtime (fallback for other architectures).
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub fn detect() -> Self {
        Self {
            avx512: false,
            avx2: false,
            neon: false,
        }
    }

    /// Create a CpuFeatures with no SIMD support.
    ///
    /// Useful for testing scalar fallback implementations.
    pub fn none() -> Self {
        Self {
            avx512: false,
            avx2: false,
            neon: false,
        }
    }

    /// Check if any SIMD instruction set is available.
    pub fn has_any(&self) -> bool {
        self.avx512 || self.avx2 || self.neon
    }
}

impl Default for CpuFeatures {
    fn default() -> Self {
        Self::detect()
    }
}

/// The SIMD implementation level being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdLevel {
    /// AVX-512 (512-bit vectors, x86_64).
    Avx512,
    /// AVX2 (256-bit vectors, x86_64).
    Avx2,
    /// NEON (128-bit vectors, ARM64).
    Neon,
    /// Scalar fallback (no SIMD).
    Scalar,
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimdLevel::Avx512 => write!(f, "AVX-512"),
            SimdLevel::Avx2 => write!(f, "AVX2"),
            SimdLevel::Neon => write!(f, "NEON"),
            SimdLevel::Scalar => write!(f, "Scalar"),
        }
    }
}

/// SIMD dispatcher for hardware-accelerated operations.
///
/// The dispatcher automatically selects the best available SIMD implementation
/// based on runtime CPU feature detection and user configuration.
///
/// # Example
///
/// ```rust
/// use als_compression::simd::SimdDispatcher;
///
/// let dispatcher = SimdDispatcher::detect();
/// println!("Using SIMD level: {}", dispatcher.level());
///
/// // Expand a range using the best available implementation
/// let values = dispatcher.expand_range(1, 100, 1);
/// assert_eq!(values.len(), 100);
/// ```
#[derive(Debug, Clone)]
pub struct SimdDispatcher {
    /// Detected CPU features.
    features: CpuFeatures,
    /// User configuration for SIMD.
    config: SimdConfig,
    /// The selected SIMD level.
    level: SimdLevel,
}

impl SimdDispatcher {
    /// Create a new dispatcher with automatic CPU detection.
    ///
    /// This detects available CPU features and selects the best SIMD level
    /// with all instruction sets enabled.
    pub fn detect() -> Self {
        Self::with_config(SimdConfig::default())
    }

    /// Create a new dispatcher with the given configuration.
    ///
    /// The configuration allows disabling specific SIMD instruction sets,
    /// which can be useful for testing or compatibility.
    pub fn with_config(config: SimdConfig) -> Self {
        let features = CpuFeatures::detect();
        let level = Self::select_level(&features, &config);
        Self {
            features,
            config,
            level,
        }
    }

    /// Create a dispatcher that only uses scalar operations.
    ///
    /// This is useful for testing or when SIMD causes issues.
    pub fn scalar_only() -> Self {
        Self::with_config(SimdConfig::disabled())
    }

    /// Select the best SIMD level based on features and configuration.
    fn select_level(features: &CpuFeatures, config: &SimdConfig) -> SimdLevel {
        // Priority: AVX-512 > AVX2 > NEON > Scalar
        if features.avx512 && config.enable_avx512 {
            SimdLevel::Avx512
        } else if features.avx2 && config.enable_avx2 {
            SimdLevel::Avx2
        } else if features.neon && config.enable_neon {
            SimdLevel::Neon
        } else {
            SimdLevel::Scalar
        }
    }

    /// Get the detected CPU features.
    pub fn features(&self) -> CpuFeatures {
        self.features
    }

    /// Get the current SIMD configuration.
    pub fn config(&self) -> &SimdConfig {
        &self.config
    }

    /// Get the selected SIMD level.
    pub fn level(&self) -> SimdLevel {
        self.level
    }

    /// Check if SIMD acceleration is being used.
    pub fn is_accelerated(&self) -> bool {
        self.level != SimdLevel::Scalar
    }


    /// Expand a range of integers into a vector.
    ///
    /// Generates an arithmetic sequence from `start` to `end` (inclusive)
    /// with the given `step`. Uses the best available SIMD implementation.
    ///
    /// # Arguments
    ///
    /// * `start` - The first value in the sequence
    /// * `end` - The last value in the sequence (inclusive)
    /// * `step` - The difference between consecutive values
    ///
    /// # Returns
    ///
    /// A vector containing the arithmetic sequence.
    ///
    /// # Example
    ///
    /// ```rust
    /// use als_compression::simd::SimdDispatcher;
    ///
    /// let dispatcher = SimdDispatcher::detect();
    ///
    /// // Ascending range
    /// let values = dispatcher.expand_range(1, 5, 1);
    /// assert_eq!(values, vec![1, 2, 3, 4, 5]);
    ///
    /// // Descending range
    /// let values = dispatcher.expand_range(10, 6, -1);
    /// assert_eq!(values, vec![10, 9, 8, 7, 6]);
    ///
    /// // Custom step
    /// let values = dispatcher.expand_range(0, 10, 2);
    /// assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    /// ```
    pub fn expand_range(&self, start: i64, end: i64, step: i64) -> Vec<i64> {
        match self.level {
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx512 => {
                // Safety: We've verified AVX-512 is available
                unsafe { super::avx512::expand_range_avx512(start, end, step) }
            }
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx2 => {
                // Safety: We've verified AVX2 is available
                unsafe { super::avx2::expand_range_avx2(start, end, step) }
            }
            #[cfg(target_arch = "aarch64")]
            SimdLevel::Neon => {
                // Safety: NEON is always available on ARM64
                unsafe { super::neon::expand_range_neon(start, end, step) }
            }
            _ => super::scalar::expand_range_scalar(start, end, step),
        }
    }

    /// Find runs of consecutive identical values in a slice.
    ///
    /// Returns a vector of (start_index, length) pairs representing runs
    /// of identical values. Uses the best available SIMD implementation.
    ///
    /// # Arguments
    ///
    /// * `values` - The slice of values to analyze
    ///
    /// # Returns
    ///
    /// A vector of (start_index, length) pairs for each run.
    ///
    /// # Example
    ///
    /// ```rust
    /// use als_compression::simd::SimdDispatcher;
    ///
    /// let dispatcher = SimdDispatcher::detect();
    /// let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
    /// let runs = dispatcher.find_runs(&values);
    ///
    /// assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4)]);
    /// ```
    pub fn find_runs(&self, values: &[i64]) -> Vec<(usize, usize)> {
        match self.level {
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx512 => {
                // Safety: We've verified AVX-512 is available
                unsafe { super::avx512::find_runs_avx512(values) }
            }
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx2 => {
                // Safety: We've verified AVX2 is available
                unsafe { super::avx2::find_runs_avx2(values) }
            }
            #[cfg(target_arch = "aarch64")]
            SimdLevel::Neon => {
                // Safety: NEON is always available on ARM64
                unsafe { super::neon::find_runs_neon(values) }
            }
            _ => super::scalar::find_runs_scalar(values),
        }
    }

    /// Find runs of consecutive identical string values.
    ///
    /// This is a convenience wrapper that works with string slices.
    /// Since SIMD can't directly compare strings, this uses the scalar
    /// implementation but may use SIMD for preprocessing in the future.
    ///
    /// # Arguments
    ///
    /// * `values` - The slice of string values to analyze
    ///
    /// # Returns
    ///
    /// A vector of (start_index, length) pairs for each run.
    pub fn find_string_runs(&self, values: &[&str]) -> Vec<(usize, usize)> {
        super::scalar::find_string_runs_scalar(values)
    }

    /// Detect arithmetic sequences in a slice of integers.
    ///
    /// Returns information about detected arithmetic sequences including
    /// start index, length, and step value.
    ///
    /// # Arguments
    ///
    /// * `values` - The slice of values to analyze
    ///
    /// # Returns
    ///
    /// A vector of (start_index, length, step) tuples for each sequence.
    pub fn find_arithmetic_sequences(&self, values: &[i64]) -> Vec<(usize, usize, i64)> {
        match self.level {
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx512 => {
                // Safety: We've verified AVX-512 is available
                unsafe { super::avx512::find_arithmetic_sequences_avx512(values) }
            }
            #[cfg(target_arch = "x86_64")]
            SimdLevel::Avx2 => {
                // Safety: We've verified AVX2 is available
                unsafe { super::avx2::find_arithmetic_sequences_avx2(values) }
            }
            #[cfg(target_arch = "aarch64")]
            SimdLevel::Neon => {
                // Safety: NEON is always available on ARM64
                unsafe { super::neon::find_arithmetic_sequences_neon(values) }
            }
            _ => super::scalar::find_arithmetic_sequences_scalar(values),
        }
    }
}

impl Default for SimdDispatcher {
    fn default() -> Self {
        Self::detect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_features_detect() {
        let features = CpuFeatures::detect();
        // Just verify it doesn't panic
        println!("Detected features: {:?}", features);
    }

    #[test]
    fn test_cpu_features_none() {
        let features = CpuFeatures::none();
        assert!(!features.avx512);
        assert!(!features.avx2);
        assert!(!features.neon);
        assert!(!features.has_any());
    }

    #[test]
    fn test_dispatcher_detect() {
        let dispatcher = SimdDispatcher::detect();
        println!("Using SIMD level: {}", dispatcher.level());
    }

    #[test]
    fn test_dispatcher_scalar_only() {
        let dispatcher = SimdDispatcher::scalar_only();
        assert_eq!(dispatcher.level(), SimdLevel::Scalar);
        assert!(!dispatcher.is_accelerated());
    }

    #[test]
    fn test_expand_range_ascending() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = dispatcher.expand_range(1, 5, 1);
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_expand_range_descending() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = dispatcher.expand_range(5, 1, -1);
        assert_eq!(values, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_expand_range_custom_step() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = dispatcher.expand_range(0, 10, 2);
        assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_expand_range_single_value() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = dispatcher.expand_range(42, 42, 1);
        assert_eq!(values, vec![42]);
    }

    #[test]
    fn test_find_runs() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
        let runs = dispatcher.find_runs(&values);
        assert_eq!(runs, vec![(0, 3), (3, 2), (5, 4)]);
    }

    #[test]
    fn test_find_runs_empty() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values: Vec<i64> = vec![];
        let runs = dispatcher.find_runs(&values);
        assert!(runs.is_empty());
    }

    #[test]
    fn test_find_runs_single() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = vec![42];
        let runs = dispatcher.find_runs(&values);
        assert_eq!(runs, vec![(0, 1)]);
    }

    #[test]
    fn test_find_string_runs() {
        let dispatcher = SimdDispatcher::detect();
        let values = vec!["a", "a", "b", "b", "b", "c"];
        let runs = dispatcher.find_string_runs(&values);
        assert_eq!(runs, vec![(0, 2), (2, 3), (5, 1)]);
    }

    #[test]
    fn test_find_arithmetic_sequences() {
        let dispatcher = SimdDispatcher::scalar_only();
        let values = vec![1, 2, 3, 4, 10, 20, 30, 5, 5, 5];
        let seqs = dispatcher.find_arithmetic_sequences(&values);
        // Should find: [1,2,3,4] with step 1, [10,20,30] with step 10, [5,5,5] with step 0
        assert!(!seqs.is_empty());
    }

    #[test]
    fn test_simd_level_display() {
        assert_eq!(format!("{}", SimdLevel::Avx512), "AVX-512");
        assert_eq!(format!("{}", SimdLevel::Avx2), "AVX2");
        assert_eq!(format!("{}", SimdLevel::Neon), "NEON");
        assert_eq!(format!("{}", SimdLevel::Scalar), "Scalar");
    }

    #[test]
    fn test_dispatcher_with_config() {
        // Test with AVX2 disabled
        let config = SimdConfig::default().with_avx2(false);
        let dispatcher = SimdDispatcher::with_config(config);
        
        // On x86_64 without AVX-512, this should fall back to scalar
        // On ARM64, this should use NEON
        // On other platforms, this should use scalar
        println!("Level with AVX2 disabled: {}", dispatcher.level());
    }
}
