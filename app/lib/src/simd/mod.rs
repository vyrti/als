//! SIMD acceleration module for ALS compression operations.
//!
//! This module provides hardware-accelerated implementations of common operations
//! used in ALS compression and decompression:
//!
//! - Range expansion: Generating arithmetic sequences efficiently
//! - Run detection: Finding consecutive identical values
//! - Pattern matching: Detecting repeating patterns
//!
//! The module automatically detects available CPU features at runtime and selects
//! the best available implementation:
//!
//! - **AVX-512**: 512-bit vectors on supported x86_64 CPUs
//! - **AVX2**: 256-bit vectors on modern x86_64 CPUs
//! - **NEON**: 128-bit vectors on ARM64 CPUs
//! - **Scalar**: Fallback for all platforms
//!
//! # Example
//!
//! ```rust
//! use als_compression::simd::SimdDispatcher;
//! use als_compression::config::SimdConfig;
//!
//! let dispatcher = SimdDispatcher::detect();
//! let values = dispatcher.expand_range(1, 10, 1);
//! assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
//! ```

mod dispatcher;
mod scalar;

#[cfg(target_arch = "x86_64")]
mod avx2;

#[cfg(target_arch = "x86_64")]
mod avx512;

#[cfg(target_arch = "aarch64")]
mod neon;

pub use dispatcher::{CpuFeatures, SimdDispatcher, SimdLevel};
