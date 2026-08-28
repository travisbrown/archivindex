//! HTTP/1 message parsing and body extraction.
//!
//! The [`message`] module reads request and response metadata without changing the input bytes.
//! The [`body`] module extracts message bodies and removes transfer coding while preserving content
//! coding.

pub mod body;
pub mod message;
mod parsing;
