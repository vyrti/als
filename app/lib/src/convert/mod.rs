//! Data conversion types and utilities.
//!
//! This module contains types for representing tabular data in a format-agnostic
//! way, enabling conversion between CSV, JSON, ALS, and log formats.

pub mod csv;
pub mod json;
pub mod log_compress;
pub mod syslog;
pub mod syslog_optimized;
mod tabular;

pub use tabular::{Column, ColumnType, TabularData, Value};
pub use syslog::{parse_syslog, to_syslog, MessageType, SyslogEntry};
pub use syslog_optimized::parse_syslog_optimized;
pub use log_compress::compress_syslog;
