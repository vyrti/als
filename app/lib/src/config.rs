//! Configuration types for the ALS compression library.
//!
//! This module provides configuration structs for controlling compression behavior,
//! SIMD optimization, parallelism, and security limits.

/// Configuration for the ALS compressor.
///
/// Controls compression behavior including CTX fallback, dictionary optimization,
/// pattern detection, SIMD usage, and parallelism.
#[derive(Debug, Clone)]
pub struct CompressorConfig {
    /// Minimum compression ratio before falling back to CTX format.
    ///
    /// If ALS compression ratio (original_size / compressed_size) falls below
    /// this threshold, the compressor will use CTX format instead.
    ///
    /// Default: 1.2 (20% compression required)
    pub ctx_fallback_threshold: f64,

    /// Size threshold for switching from HashMap to DashMap.
    ///
    /// When the expected number of dictionary entries exceeds this threshold,
    /// the compressor will use DashMap (concurrent hashmap) instead of
    /// standard HashMap for better performance with large datasets.
    ///
    /// Default: 10,000 entries
    pub hashmap_threshold: usize,

    /// Minimum pattern length to consider for compression.
    ///
    /// Patterns shorter than this length will not be detected or compressed.
    /// This prevents overhead from compressing very short patterns that may
    /// not provide compression benefit.
    ///
    /// Default: 3 values
    pub min_pattern_length: usize,

    /// SIMD instruction set configuration.
    ///
    /// Controls which SIMD instruction sets are enabled for acceleration.
    pub simd_config: SimdConfig,

    /// Number of threads for parallel processing.
    ///
    /// - 0: Auto-detect based on available CPU cores
    /// - 1: Single-threaded processing (no parallelism)
    /// - N: Use N threads for parallel processing
    ///
    /// Default: 0 (auto)
    pub parallelism: usize,

    /// Maximum number of values to expand from a single range operator.
    ///
    /// This security limit prevents memory exhaustion from malicious or
    /// malformed ALS documents with very large ranges.
    ///
    /// Default: 10,000,000 values
    pub max_range_expansion: usize,

    /// Maximum number of entries in a dictionary.
    ///
    /// This security limit prevents memory exhaustion from malicious or
    /// malformed ALS documents with very large dictionaries.
    ///
    /// Default: 65,536 entries
    pub max_dictionary_entries: usize,

    /// Maximum input size for non-streaming operations (in bytes).
    ///
    /// This security limit prevents memory exhaustion from very large inputs.
    /// For larger inputs, use streaming APIs.
    ///
    /// Default: 1,073,741,824 bytes (1 GB)
    pub max_input_size: usize,
}

impl Default for CompressorConfig {
    fn default() -> Self {
        Self {
            ctx_fallback_threshold: 1.2,
            hashmap_threshold: 10_000,
            min_pattern_length: 3,
            simd_config: SimdConfig::default(),
            parallelism: 0, // auto-detect
            max_range_expansion: 10_000_000,
            max_dictionary_entries: 65_536,
            max_input_size: 1_073_741_824, // 1 GB
        }
    }
}

impl CompressorConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the CTX fallback threshold.
    ///
    /// # Arguments
    ///
    /// * `threshold` - Minimum compression ratio (must be >= 1.0)
    ///
    /// # Panics
    ///
    /// Panics if threshold is less than 1.0.
    pub fn with_ctx_fallback_threshold(mut self, threshold: f64) -> Self {
        assert!(threshold >= 1.0, "CTX fallback threshold must be >= 1.0");
        self.ctx_fallback_threshold = threshold;
        self
    }

    /// Set the HashMap/DashMap size threshold.
    pub fn with_hashmap_threshold(mut self, threshold: usize) -> Self {
        self.hashmap_threshold = threshold;
        self
    }

    /// Set the minimum pattern length.
    pub fn with_min_pattern_length(mut self, length: usize) -> Self {
        self.min_pattern_length = length;
        self
    }

    /// Set the SIMD configuration.
    pub fn with_simd_config(mut self, config: SimdConfig) -> Self {
        self.simd_config = config;
        self
    }

    /// Set the parallelism level.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set the maximum range expansion limit.
    pub fn with_max_range_expansion(mut self, max: usize) -> Self {
        self.max_range_expansion = max;
        self
    }

    /// Set the maximum dictionary entries limit.
    pub fn with_max_dictionary_entries(mut self, max: usize) -> Self {
        self.max_dictionary_entries = max;
        self
    }

    /// Set the maximum input size limit.
    pub fn with_max_input_size(mut self, max: usize) -> Self {
        self.max_input_size = max;
        self
    }
}

/// Configuration for the ALS parser.
///
/// Controls decompression behavior including SIMD usage and parallelism.
#[derive(Debug, Clone)]
pub struct ParserConfig {
    /// SIMD instruction set configuration.
    ///
    /// Controls which SIMD instruction sets are enabled for acceleration
    /// during decompression.
    pub simd_config: SimdConfig,

    /// Number of threads for parallel processing.
    ///
    /// - 0: Auto-detect based on available CPU cores
    /// - 1: Single-threaded processing (no parallelism)
    /// - N: Use N threads for parallel processing
    ///
    /// Default: 0 (auto)
    pub parallelism: usize,

    /// Maximum number of values to expand from a single range operator.
    ///
    /// This security limit prevents memory exhaustion from malicious or
    /// malformed ALS documents with very large ranges.
    ///
    /// Default: 10,000,000 values
    pub max_range_expansion: usize,

    /// Maximum number of entries in a dictionary.
    ///
    /// This security limit prevents memory exhaustion from malicious or
    /// malformed ALS documents with very large dictionaries.
    ///
    /// Default: 65,536 entries
    pub max_dictionary_entries: usize,

    /// Maximum input size for non-streaming operations (in bytes).
    ///
    /// This security limit prevents memory exhaustion from very large inputs.
    /// For larger inputs, use streaming APIs.
    ///
    /// Default: 1,073,741,824 bytes (1 GB)
    pub max_input_size: usize,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            simd_config: SimdConfig::default(),
            parallelism: 0, // auto-detect
            max_range_expansion: 10_000_000,
            max_dictionary_entries: 65_536,
            max_input_size: 1_073_741_824, // 1 GB
        }
    }
}

impl ParserConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the SIMD configuration.
    pub fn with_simd_config(mut self, config: SimdConfig) -> Self {
        self.simd_config = config;
        self
    }

    /// Set the parallelism level.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set the maximum range expansion limit.
    pub fn with_max_range_expansion(mut self, max: usize) -> Self {
        self.max_range_expansion = max;
        self
    }

    /// Set the maximum dictionary entries limit.
    pub fn with_max_dictionary_entries(mut self, max: usize) -> Self {
        self.max_dictionary_entries = max;
        self
    }

    /// Set the maximum input size limit.
    pub fn with_max_input_size(mut self, max: usize) -> Self {
        self.max_input_size = max;
        self
    }
}

/// SIMD instruction set configuration.
///
/// Controls which SIMD instruction sets are enabled for hardware acceleration.
/// The library will automatically detect available CPU features at runtime and
/// use the best available instruction set that is enabled in this configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimdConfig {
    /// Enable AVX-512 instructions (x86_64 only).
    ///
    /// AVX-512 provides 512-bit wide vector operations for maximum throughput
    /// on supported CPUs (Intel Xeon Scalable, AMD Zen 4+).
    ///
    /// Default: true
    pub enable_avx512: bool,

    /// Enable AVX2 instructions (x86_64 only).
    ///
    /// AVX2 provides 256-bit wide vector operations and is widely supported
    /// on modern x86_64 CPUs (Intel Haswell+, AMD Excavator+).
    ///
    /// Default: true
    pub enable_avx2: bool,

    /// Enable NEON instructions (ARM64 only).
    ///
    /// NEON provides 128-bit wide vector operations and is standard on
    /// ARM64 CPUs (Apple Silicon, AWS Graviton, etc.).
    ///
    /// Default: true
    pub enable_neon: bool,
}

impl Default for SimdConfig {
    fn default() -> Self {
        Self {
            enable_avx512: true,
            enable_avx2: true,
            enable_neon: true,
        }
    }
}

impl SimdConfig {
    /// Create a new SIMD configuration with all instruction sets enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration with all SIMD instruction sets disabled.
    ///
    /// This forces the library to use scalar (non-SIMD) implementations,
    /// which can be useful for debugging or compatibility testing.
    pub fn disabled() -> Self {
        Self {
            enable_avx512: false,
            enable_avx2: false,
            enable_neon: false,
        }
    }

    /// Enable or disable AVX-512 instructions.
    pub fn with_avx512(mut self, enable: bool) -> Self {
        self.enable_avx512 = enable;
        self
    }

    /// Enable or disable AVX2 instructions.
    pub fn with_avx2(mut self, enable: bool) -> Self {
        self.enable_avx2 = enable;
        self
    }

    /// Enable or disable NEON instructions.
    pub fn with_neon(mut self, enable: bool) -> Self {
        self.enable_neon = enable;
        self
    }

    /// Check if any SIMD instruction set is enabled.
    pub fn is_any_enabled(&self) -> bool {
        self.enable_avx512 || self.enable_avx2 || self.enable_neon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressor_config_default() {
        let config = CompressorConfig::default();
        assert_eq!(config.ctx_fallback_threshold, 1.2);
        assert_eq!(config.hashmap_threshold, 10_000);
        assert_eq!(config.min_pattern_length, 3);
        assert_eq!(config.parallelism, 0);
        assert_eq!(config.max_range_expansion, 10_000_000);
        assert_eq!(config.max_dictionary_entries, 65_536);
        assert_eq!(config.max_input_size, 1_073_741_824);
    }

    #[test]
    fn test_compressor_config_builder() {
        let config = CompressorConfig::new()
            .with_ctx_fallback_threshold(1.5)
            .with_hashmap_threshold(5_000)
            .with_min_pattern_length(5)
            .with_parallelism(4)
            .with_max_range_expansion(1_000_000)
            .with_max_dictionary_entries(10_000)
            .with_max_input_size(500_000_000);

        assert_eq!(config.ctx_fallback_threshold, 1.5);
        assert_eq!(config.hashmap_threshold, 5_000);
        assert_eq!(config.min_pattern_length, 5);
        assert_eq!(config.parallelism, 4);
        assert_eq!(config.max_range_expansion, 1_000_000);
        assert_eq!(config.max_dictionary_entries, 10_000);
        assert_eq!(config.max_input_size, 500_000_000);
    }

    #[test]
    #[should_panic(expected = "CTX fallback threshold must be >= 1.0")]
    fn test_compressor_config_invalid_threshold() {
        CompressorConfig::new().with_ctx_fallback_threshold(0.5);
    }

    #[test]
    fn test_parser_config_default() {
        let config = ParserConfig::default();
        assert_eq!(config.parallelism, 0);
        assert_eq!(config.max_range_expansion, 10_000_000);
        assert_eq!(config.max_dictionary_entries, 65_536);
        assert_eq!(config.max_input_size, 1_073_741_824);
    }

    #[test]
    fn test_parser_config_builder() {
        let config = ParserConfig::new()
            .with_parallelism(8)
            .with_max_range_expansion(5_000_000)
            .with_max_dictionary_entries(32_768)
            .with_max_input_size(2_000_000_000);

        assert_eq!(config.parallelism, 8);
        assert_eq!(config.max_range_expansion, 5_000_000);
        assert_eq!(config.max_dictionary_entries, 32_768);
        assert_eq!(config.max_input_size, 2_000_000_000);
    }

    #[test]
    fn test_simd_config_default() {
        let config = SimdConfig::default();
        assert!(config.enable_avx512);
        assert!(config.enable_avx2);
        assert!(config.enable_neon);
        assert!(config.is_any_enabled());
    }

    #[test]
    fn test_simd_config_disabled() {
        let config = SimdConfig::disabled();
        assert!(!config.enable_avx512);
        assert!(!config.enable_avx2);
        assert!(!config.enable_neon);
        assert!(!config.is_any_enabled());
    }

    #[test]
    fn test_simd_config_builder() {
        let config = SimdConfig::new()
            .with_avx512(false)
            .with_avx2(true)
            .with_neon(false);

        assert!(!config.enable_avx512);
        assert!(config.enable_avx2);
        assert!(!config.enable_neon);
        assert!(config.is_any_enabled());
    }

    #[test]
    fn test_simd_config_partial_enable() {
        let config = SimdConfig::disabled()
            .with_avx2(true);

        assert!(!config.enable_avx512);
        assert!(config.enable_avx2);
        assert!(!config.enable_neon);
        assert!(config.is_any_enabled());
    }

    #[test]
    fn test_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompressorConfig>();
        assert_send_sync::<ParserConfig>();
        assert_send_sync::<SimdConfig>();
    }

    #[test]
    fn test_config_is_clone() {
        let compressor_config = CompressorConfig::default();
        let _cloned = compressor_config.clone();

        let parser_config = ParserConfig::default();
        let _cloned = parser_config.clone();

        let simd_config = SimdConfig::default();
        let _cloned = simd_config.clone();
    }

    #[test]
    fn test_simd_config_equality() {
        let config1 = SimdConfig::new();
        let config2 = SimdConfig::default();
        assert_eq!(config1, config2);

        let config3 = SimdConfig::disabled();
        assert_ne!(config1, config3);
    }
}
