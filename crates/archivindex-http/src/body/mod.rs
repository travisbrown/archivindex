//! HTTP entity-body extraction.
//!
//! The entity-body is the message body after transfer-coding has been removed.

use std::borrow::Cow;

use crate::parsing::{HeaderLine, lossy, next_line, scan_head};

const TRANSFER_ENCODING: &[u8] = b"transfer-encoding";
const CHUNKED: &[u8] = b"chunked";
const IDENTITY: &[u8] = b"identity";

/// Errors returned while extracting an HTTP entity-body.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// The header section has no terminating empty line.
    #[error("the HTTP message does not end its header section with an empty line")]
    UnterminatedHeaders,
    /// The message has an unsupported `Transfer-Encoding` value.
    #[error("unsupported Transfer-Encoding value: `{0}`")]
    UnsupportedTransferEncoding(String),

    /// A chunk has an invalid size line.
    #[error("the chunked message declares `{0}` where a chunk size belongs")]
    MalformedChunkSize(String),
    /// The chunked body is incomplete.
    #[error("the chunked message ends before the chunk that closes it")]
    IncompleteChunkedBody,
}

/// Extract the HTTP entity-body defined by RFC 2616 section 7.2.
///
/// Chunk framing and trailers are removed. `identity` is ignored, while content-coding is
/// preserved. The caller supplies the bytes of one message; the HTTP `Content-Length` is not used
/// to limit the body. If no decoding is needed, the returned value borrows from `message`.
///
/// Only chunked transfer decoding is supported. `Transfer-Encoding: gzip, chunked` is valid HTTP,
/// but is rejected because extracting its entity-body requires gzip decompression after dechunking.
/// In contrast, `Content-Encoding: gzip` with `Transfer-Encoding: chunked` is supported: gzip is
/// content coding in that case and remains part of the entity-body.
///
/// A body declared chunked must be complete, through the empty line that ends its trailer section.
/// The message alone does not say whether it has a body at all, which depends on the request method
/// and the status. Callers must handle bodyless responses before invoking this function.
///
/// # Errors
///
/// Returns an error for an unterminated header section, invalid or incomplete chunk framing, or an
/// unsupported transfer-coding. This does not fully validate HTTP headers.
pub fn entity_body(message: &[u8]) -> Result<Cow<'_, [u8]>, Error> {
    entity_body_with_policy(message, Framing::Declared)
}

/// How to determine an HTTP message's transfer framing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Framing {
    /// Follow the headers, requiring complete chunk framing when chunked encoding is declared.
    ///
    /// The trailer section must end with an empty line.
    Declared,
    /// Infer whether a declared chunked body needs decoding, and allow missing trailers.
    ///
    /// Decode chunks if the body opens with a complete chunk-size line. Otherwise, assume it has
    /// already been dechunked and return it unchanged. A zero-size chunk ends decoding even if its
    /// trailers are missing. Content coding is always preserved.
    Inferred,
}

/// Extract an entity-body using an explicit framing policy.
///
/// The transfer-coding support described in [`entity_body`] applies under either policy.
/// Unsupported codings and incomplete chunk data are errors under either policy.
pub fn entity_body_with_policy(message: &[u8], policy: Framing) -> Result<Cow<'_, [u8]>, Error> {
    let (body, transfer_encoding) = split_message(message)?;
    if is_chunked(&transfer_encoding)?
        && (policy == Framing::Declared
            || next_line(body, 0).is_some_and(|line| chunk_size(&body[..line.end]).is_ok()))
    {
        Ok(Cow::Owned(dechunk(body, policy)?))
    } else {
        Ok(Cow::Borrowed(body))
    }
}

/// Return the HTTP message-body described by RFC 2616 section 4.3 without decoding it.
///
/// The caller supplies the bytes of one message. All bytes after the head are returned unchanged,
/// including any chunk framing and trailers. This does not enforce `Content-Length` or determine
/// whether the request method and response status permit a body.
pub fn message_body(message: &[u8]) -> Result<&[u8], Error> {
    split_message(message).map(|(body, _)| body)
}

/// Split an HTTP message into its body and combined `Transfer-Encoding` value.
///
/// Repeated and folded fields are combined into one comma-separated value.
fn split_message(message: &[u8]) -> Result<(&[u8], Vec<u8>), Error> {
    let mut transfer_encoding = Vec::new();
    let mut folding = false;
    let head = scan_head(message, |line| {
        match line {
            HeaderLine::Continuation(content) if folding => {
                transfer_encoding.push(b' ');
                transfer_encoding.extend_from_slice(content);
            }
            HeaderLine::Continuation(_) => {}
            HeaderLine::Field { name, value, .. }
                if name.eq_ignore_ascii_case(TRANSFER_ENCODING) =>
            {
                if !transfer_encoding.is_empty() {
                    transfer_encoding.push(b',');
                }
                transfer_encoding.extend_from_slice(value);
                folding = true;
            }
            _ => folding = false,
        }
        Some(())
    })
    .ok_or(Error::UnterminatedHeaders)?;
    Ok((&message[head.body_offset..], transfer_encoding))
}

/// Validate the supported transfer codings and report whether chunk decoding is needed.
///
/// After ignoring empty elements and `identity`, accept either no coding or a single `chunked`.
/// This checks decoder support, not just the presence of `chunked`: valid HTTP values such as
/// `gzip, chunked` are rejected because this crate cannot remove the additional transfer coding.
fn is_chunked(transfer_encoding: &[u8]) -> Result<bool, Error> {
    // Remove ignored elements.
    let mut filtered = transfer_encoding
        .split(|&byte| byte == b',')
        .map(<[u8]>::trim_ascii)
        .filter(|coding| !coding.is_empty() && !coding.eq_ignore_ascii_case(IDENTITY));

    match filtered.next() {
        // There's one element, and it matches `chunked`.
        Some(coding) if coding.eq_ignore_ascii_case(CHUNKED) && filtered.next().is_none() => {
            Ok(true)
        }
        // No elements.
        None => Ok(false),
        // Unsupported elements.
        Some(_) => Err(Error::UnsupportedTransferEncoding(lossy(
            transfer_encoding.trim_ascii(),
        ))),
    }
}

/// Decode a chunked body as defined by RFC 2616 section 3.6.1.
///
/// Chunk extensions, framing, and trailers are omitted from the result.
fn dechunk(body: &[u8], policy: Framing) -> Result<Vec<u8>, Error> {
    let mut decoded = Vec::with_capacity(body.len());
    let mut offset = 0;

    loop {
        let line = next_line(body, offset).ok_or(Error::IncompleteChunkedBody)?;
        let size = chunk_size(&body[offset..line.end])?;
        offset = line.next;

        if size == 0 {
            if policy == Framing::Inferred {
                return Ok(decoded);
            }
            // The trailer section ends at an empty line, which a complete body includes.
            loop {
                let line = next_line(body, offset).ok_or(Error::IncompleteChunkedBody)?;
                if line.end == offset {
                    return Ok(decoded);
                }
                offset = line.next;
            }
        }

        let end = offset
            .checked_add(size)
            .filter(|end| *end <= body.len())
            .ok_or(Error::IncompleteChunkedBody)?;
        decoded.extend_from_slice(&body[offset..end]);

        // The chunk data must be followed immediately by a line ending.
        let line = next_line(body, end).ok_or(Error::IncompleteChunkedBody)?;
        if line.end != end {
            return Err(Error::IncompleteChunkedBody);
        }
        offset = line.next;
    }
}

/// Parse a hexadecimal chunk size, ignoring extensions.
fn chunk_size(line: &[u8]) -> Result<usize, Error> {
    let digits = line
        .iter()
        .position(|&byte| byte == b';')
        .map_or(line, |extensions| &line[..extensions])
        .trim_ascii();

    std::str::from_utf8(digits)
        .ok()
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .and_then(|digits| usize::from_str_radix(digits, 16).ok())
        .ok_or_else(|| Error::MalformedChunkSize(lossy(line)))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod inferred_tests;
