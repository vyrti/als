//! Compression components for ALS format.
//!
//! This module contains the dictionary builder, compressor, statistics tracking,
//! and other compression utilities used to optimize ALS output.

mod compressor;
mod dictionary;
mod stats;

pub use compressor::AlsCompressor;
pub use dictionary::{DictionaryBuilder, DictionaryEntry, EnumDetector};
pub use stats::{ColumnStats, CompressionReport, CompressionStats, StatsSnapshot};
