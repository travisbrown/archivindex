//! HTTP/1 message parsing and body extraction.
//!
//! The [`message`] module reads request and response metadata without changing the input bytes.
//! The [`body`] module extracts message bodies and removes transfer coding while preserving content
//! coding.
//!
//! ```
//! use archivindex_http::{body, message::ResponseMetadata};
//!
//! let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
//! let metadata = ResponseMetadata::parse(response).unwrap();
//! assert_eq!(metadata.status, 200);
//! assert_eq!(body::entity_body(response)?.as_ref(), b"hello");
//! # Ok::<(), archivindex_http::body::Error>(())
//! ```
//!
//! Supply exactly one message. Body extraction does not enforce `Content-Length` or determine
//! whether the request method and response status permit a body. Handle bodyless responses first.
//!
//! [`body::entity_body`] uses [`Framing::Declared`](body::Framing::Declared), which requires complete
//! chunk framing. For archives whose chunk framing may have been removed without updating the
//! headers, use [`body::entity_body_with_policy`] with [`Framing::Inferred`](body::Framing::Inferred).
//! That policy can return incomplete framing unchanged; consult its documentation before using it.

pub mod body;
pub mod message;
mod parsing;
