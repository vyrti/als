//! Adaptive HashMap implementation for ALS compression.
//!
//! This module provides an `AdaptiveMap` that automatically selects between
//! a standard `HashMap` and a concurrent `DashMap` based on the expected
//! dataset size, providing optimal performance for both small and large datasets.

mod adaptive;

pub use adaptive::AdaptiveMap;
