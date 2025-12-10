//! ALS (Adaptive Logic Stream) format types and operations.
//!
//! This module contains the core data structures for representing ALS documents,
//! including operators, column streams, and document structures.

mod document;
pub mod escape;
mod operator;
mod parser;
mod serializer;
mod tokenizer;

pub use document::{AlsDocument, ColumnStream, FormatIndicator};
pub use escape::{
    decode_als_value, encode_als_value, escape_als_string, is_empty_token, is_null_token,
    needs_escaping, unescape_als_string, EMPTY_TOKEN, NULL_TOKEN,
};
pub use operator::AlsOperator;
pub use parser::AlsParser;
pub use serializer::{AlsPrettyPrinter, AlsSerializer};
pub use tokenizer::{Token, Tokenizer, VersionType};
