//! Low-level scanning helpers shared by the message parsers.

/// Whether a byte is linear white space (`SP` or `HT`).
const fn is_lws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
}

/// Whether a byte is allowed by the `token` grammar (RFC 9110 section 5.6.2).
const fn is_token_char(byte: u8) -> bool {
    !matches!(byte, 0..=31
        | 127..=255
        | b'('
        | b')'
        | b'<'
        | b'>'
        | b'@'
        | b','
        | b';'
        | b':'
        | b'"'
        | b'/'
        | b'['
        | b']'
        | b'?'
        | b'='
        | b'{'
        | b'}'
        | b' '
        | b'\\')
}

/// Whether every byte of a value is a `token` character, and there is at least one.
pub fn is_token(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().copied().all(is_token_char)
}

/// Where a line's content ends and where the line after it begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Line {
    /// The offset just past the content, which is the first byte of the line ending.
    pub end: usize,
    /// The offset the following line begins at, past the line ending.
    pub next: usize,
}

/// Find the line beginning at `start`, ended by `CRLF` or a bare `LF`.
///
/// Returns `None` when no complete line remains: nothing is left, the last line has no line ending,
/// or `start` is past the end of the block.
pub fn next_line(block: &[u8], start: usize) -> Option<Line> {
    let offset = block.get(start..)?.iter().position(|&byte| byte == b'\n')?;
    let line_feed = start + offset;
    let crlf = offset > 0 && block[line_feed - 1] == b'\r';

    Some(Line {
        end: if crlf { line_feed - 1 } else { line_feed },
        next: line_feed + 1,
    })
}

/// Split a field line into the name it opens with and the offset of the colon that closes it.
///
/// Returns `None` unless the line begins with a token followed by optional white space and a colon.
fn split_field_line(line: &[u8]) -> Option<(&[u8], usize)> {
    let name_end = line
        .iter()
        .position(|&byte| !is_token_char(byte))
        .unwrap_or(line.len());
    if name_end == 0 {
        return None;
    }

    let mut colon = name_end;
    while line.get(colon).copied().is_some_and(is_lws) {
        colon += 1;
    }

    (line.get(colon) == Some(&b':')).then_some((&line[..name_end], colon))
}

/// A header line with its original value bytes and whitespace.
pub enum HeaderLine<'a> {
    /// A token field name followed by optional whitespace, a colon, and a value.
    Field {
        name: &'a [u8],
        value: &'a [u8],
        whitespace_before_colon: bool,
    },
    /// A line beginning with a space or tab, including that whitespace.
    Continuation(&'a [u8]),
    /// A line that is neither a field nor a continuation.
    Invalid,
}

/// Message boundaries shared by metadata parsing and body extraction.
pub struct Head<'a> {
    pub start_line: &'a [u8],
    pub body_offset: usize,
}

/// Scan a message head, accepting `CRLF` and bare `LF` line endings.
///
/// The visitor decides which fields to retain and whether malformed lines, whitespace before a
/// colon, or continuations are acceptable. Values are left unchanged for the visitor to unfold.
/// Returns `None` if the head is incomplete or the visitor rejects a line.
pub fn scan_head<'a>(
    message: &'a [u8],
    mut visit: impl FnMut(HeaderLine<'a>) -> Option<()>,
) -> Option<Head<'a>> {
    let start_line = next_line(message, 0)?;
    let mut offset = start_line.next;

    loop {
        let line = next_line(message, offset)?;
        let content = &message[offset..line.end];
        offset = line.next;

        if content.is_empty() {
            return Some(Head {
                start_line: &message[..start_line.end],
                body_offset: offset,
            });
        }

        let header = if content.first().copied().is_some_and(is_lws) {
            HeaderLine::Continuation(content)
        } else if let Some((name, colon)) = split_field_line(content) {
            HeaderLine::Field {
                name,
                value: &content[colon + 1..],
                whitespace_before_colon: name.len() != colon,
            }
        } else {
            HeaderLine::Invalid
        };
        visit(header)?;
    }
}

/// Render bytes as text for an error message.
pub fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{Line, is_token, next_line, split_field_line};

    #[test]
    fn token_grammar() {
        assert!(is_token(b"x-header_1"));
        for value in [&b""[..], b"a:b", b"a b", b"a\tb", b"a\x7fb", b"caf\xc3\xa9"] {
            assert!(!is_token(value), "{value:?}");
        }
    }

    /// A line ends at its line ending, whether that is `CRLF` or a bare `LF`.
    #[test]
    fn line_bounds() {
        let block = b"one\r\ntwo\nthree";

        assert_eq!(next_line(block, 0), Some(Line { end: 3, next: 5 }));
        assert_eq!(next_line(block, 5), Some(Line { end: 8, next: 9 }));
        // The last line is unterminated, so it is not reported.
        assert_eq!(next_line(block, 9), None);
    }

    /// A start past the end of the block reports no line, the way the end of a block does.
    #[test]
    fn line_bounds_past_the_end() {
        let block = b"one\r\n";

        assert_eq!(next_line(block, block.len()), None);
        assert_eq!(next_line(block, block.len() + 1), None);
    }

    /// An empty line is still a line: it is what ends a header section.
    #[test]
    fn empty_line_bounds() {
        assert_eq!(next_line(b"\r\n", 0), Some(Line { end: 0, next: 2 }));
    }

    #[test]
    fn field_line_splitting() {
        assert_eq!(
            split_field_line(b"some-header: value"),
            Some((&b"some-header"[..], 11))
        );
        assert_eq!(
            split_field_line(b"some-header \t: value"),
            Some((&b"some-header"[..], 13))
        );

        // No name, no token where the name belongs, and no colon at all.
        for line in [
            &b": value"[..],
            &b" continuation"[..],
            &b"evil\x7fname: value"[..],
            &b"some-header value"[..],
            &b"some-header"[..],
        ] {
            assert_eq!(split_field_line(line), None, "{line:?}");
        }
    }
}
