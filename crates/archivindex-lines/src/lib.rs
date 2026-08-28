//! Read lines with size limits and source locations for errors.
//!
//! When parsing an index file, we often need to report which file and line caused an error.
//! [`Lines`] keeps track of this information while reading UTF-8 lines. It limits the size of each
//! line and removes trailing CR and LF bytes. Blank lines are skipped by default, or you can reject
//! them with [`Lines::reject_blank_lines`]. Lines containing other whitespace are not considered
//! blank.
//!
//! Each successful read returns the line's content, source name, and line number, borrowing from
//! the reader. If parsing fails, use [`LineContextRef::into_owned`] to keep the location and an
//! excerpt for an error message.
//!
//! [`Error`] distinguishes I/O failures from invalid content. You can convert it to
//! [`std::io::Error`] when your API uses that error type.
//!
//! ```
//! let mut lines =
//!     archivindex_lines::Lines::with_source(&b"first\r\n\nsecond\n"[..], "test.jsonl");
//!
//! let context = lines.next_content()?.expect("the first line");
//! assert_eq!((context.line, context.content), (1, "first"));
//!
//! // The blank line is skipped but still counted.
//! let context = lines.next_content()?.expect("the third line");
//! assert_eq!((context.line, context.content), (3, "second"));
//! assert_eq!(lines.next_content()?, None);
//! # Ok::<(), archivindex_lines::Error>(())
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::io::{BufRead, Read};

/// The maximum number of content characters in an excerpt, excluding the trailing ellipsis.
const EXCERPT_CHAR_LIMIT: usize = 160;

/// The default line size limit, which can be changed with [`Lines::with_max_line_bytes`].
const DEFAULT_MAX_LINE_BYTES: usize = 16 << 20;

/// A source name, line number, and optional excerpt for an error message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineContext {
    /// The source name supplied by the caller, such as a file path.
    pub source: String,
    /// One-based line number.
    pub line: usize,
    /// A short excerpt of the line, included when converting a [`LineContextRef`].
    /// Read errors do not include an excerpt.
    pub excerpt: Option<String>,
}

/// A line's content and location, borrowed from the reader.
///
/// This value borrows the source name and content without allocating. Call [`Self::into_owned`]
/// when you need to keep the location after reading another line, for example to report a parsing
/// error. The reader cannot be used mutably while its source name or content is still borrowed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineContextRef<'a> {
    /// The source name supplied by the caller, such as a file path.
    pub source: &'a str,
    /// One-based line number.
    pub line: usize,
    /// The line content with trailing CR and LF bytes removed.
    pub content: &'a str,
}

impl LineContextRef<'_> {
    /// Copy the source name and a short excerpt so this location can outlive the reader.
    #[must_use]
    pub fn into_owned(self) -> LineContext {
        context(self.source, self.line, self.content)
    }
}

impl std::fmt::Display for LineContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.source, self.line)?;
        if let Some(excerpt) = &self.excerpt {
            write!(formatter, ": {excerpt}")?;
        }
        Ok(())
    }
}

/// A read or line-validation failure annotated with its source and line number.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A line was longer than the reader accepts.
    #[error("line at {context} is longer than {limit} bytes")]
    TooLong {
        /// Location of the over-long line.
        context: LineContext,
        /// The limit the line exceeded; see [`Lines::with_max_line_bytes`].
        limit: usize,
    },
    /// A line was blank and the source rejects blank lines.
    ///
    /// Only sources built with [`Lines::reject_blank_lines`] produce this.
    #[error("blank line at {context}")]
    Blank {
        /// Location of the blank line.
        context: LineContext,
    },
    /// A line was not valid UTF-8.
    #[error("invalid UTF-8 at {context}")]
    InvalidUtf8 {
        /// Location of the invalid line.
        context: LineContext,
        /// Where in the line decoding failed.
        #[source]
        source: std::str::Utf8Error,
    },

    /// The underlying reader failed.
    #[error("failed to read {context}")]
    Io {
        /// Location of the failed read.
        context: LineContext,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

impl Error {
    /// The source name and line number the failure occurred on.
    #[must_use]
    pub const fn context(&self) -> &LineContext {
        match self {
            Self::Io { context, .. }
            | Self::TooLong { context, .. }
            | Self::InvalidUtf8 { context, .. }
            | Self::Blank { context } => context,
        }
    }
}

/// Preserve the location and reason for APIs that expose only I/O errors.
///
/// The original [`Error`] can be recovered with [`std::io::Error::get_ref`] and `downcast_ref`.
/// Rejected content becomes [`std::io::ErrorKind::InvalidData`].
impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        let kind = match &error {
            Error::Io { source, .. } => source.kind(),
            Error::TooLong { .. } | Error::InvalidUtf8 { .. } | Error::Blank { .. } => {
                std::io::ErrorKind::InvalidData
            }
        };

        Self::new(kind, error)
    }
}

/// A line source that trims line endings, tracks line numbers, and skips blank lines (unless it was
/// built with [`Self::reject_blank_lines`]).
pub struct Lines<R> {
    underlying: R,
    /// Scratch buffer reused across lines; returned content is only valid until the next call.
    line: Vec<u8>,
    line_number: usize,
    source: String,
    reject_blanks: bool,
    max_line_bytes: usize,
    fused: bool,
}

impl<R: BufRead> Lines<R> {
    /// Create a line reader with a source name, such as a file path, to use in error messages.
    pub fn with_source(underlying: R, source: impl Into<String>) -> Self {
        Self {
            underlying,
            line: Vec::new(),
            line_number: 0,
            source: source.into(),
            reject_blanks: false,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            fused: false,
        }
    }

    /// Report blank lines as invalid data instead of skipping them.
    #[must_use]
    pub const fn reject_blank_lines(mut self) -> Self {
        self.reject_blanks = true;
        self
    }

    /// Accept lines of up to `max_line_bytes`, excluding the line ending.
    ///
    /// The limit prevents a single line from using an unbounded amount of memory. The default is
    /// 16 MiB to allow for records containing extracted full text. You can lower it for formats
    /// with shorter records.
    #[must_use]
    pub const fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    /// Read the next non-blank line, returning its borrowed context and content together.
    /// Trailing CR and LF bytes are removed. A line containing other whitespace is not considered
    /// blank, even if that is the only content.
    ///
    /// Blank lines are skipped rather than returned, but still counted, unless the source was built
    /// with [`Self::reject_blank_lines`]. `None` marks the end of the stream.
    ///
    /// Successful reads borrow the source name and content, though the read buffer may need to
    /// grow. Read errors include a copy of the source name but no excerpt. Use
    /// [`LineContextRef::into_owned`] to keep a successful read's location and a short excerpt.
    ///
    /// ```
    /// let mut lines = archivindex_lines::Lines::with_source(&b"42\n"[..], "numbers");
    /// let location = lines.next_content()?.expect("a number");
    /// let number = location.content.parse::<u64>().map_err(|_| location.into_owned());
    /// assert_eq!(number, Ok(42));
    /// # Ok::<(), archivindex_lines::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] when the underlying read fails, [`Error::TooLong`] when a line is
    /// longer than the source's limit, [`Error::InvalidUtf8`] when a line is not valid UTF-8, and
    /// [`Error::Blank`] when a line is blank and the source was built with
    /// [`Self::reject_blank_lines`]. After any error, further calls return `Ok(None)`.
    pub fn next_content(&mut self) -> Result<Option<LineContextRef<'_>>, Error> {
        if self.fused {
            return Ok(None);
        }

        loop {
            self.line.clear();

            // Allow both bytes of CRLF after content at the limit. A full buffer without LF cannot
            // contain a complete permitted line ending and must not become a split line.
            let buffered = self.max_line_bytes.saturating_add(2);
            let read = Read::by_ref(&mut self.underlying)
                .take(buffered as u64)
                .read_until(b'\n', &mut self.line)
                .map_err(|source| {
                    self.fail_on(self.line_number + 1, |context| Error::Io {
                        context,
                        source,
                    })
                })?;
            if read == 0 {
                self.fused = true;
                return Ok(None);
            }

            self.line_number += 1;
            let trimmed = self.line.len()
                - self
                    .line
                    .iter()
                    .rev()
                    .take_while(|byte| matches!(byte, b'\r' | b'\n'))
                    .count();

            if trimmed > self.max_line_bytes || (read == buffered && !self.line.ends_with(b"\n")) {
                let limit = self.max_line_bytes;
                return Err(self.fail_on(self.line_number, |context| Error::TooLong {
                    context,
                    limit,
                }));
            }

            if trimmed == 0 && self.reject_blanks {
                return Err(self.fail_on(self.line_number, |context| Error::Blank { context }));
            }

            if trimmed > 0 {
                // The arms touch disjoint fields, so the returned borrow of `line` may coexist with
                // fusing the source; a `&mut self` helper could not be called here.
                let line_text = match std::str::from_utf8(&self.line[..trimmed]) {
                    Ok(line_text) => line_text,
                    Err(source) => {
                        self.fused = true;
                        return Err(Error::InvalidUtf8 {
                            context: failure(&self.source, self.line_number),
                            source,
                        });
                    }
                };
                let location = LineContextRef {
                    source: &self.source,
                    line: self.line_number,
                    content: line_text,
                };

                return Ok(Some(location));
            }
        }
    }

    /// Stop further reads and build an error for line `line`.
    fn fail_on(&mut self, line: usize, build: impl FnOnce(LineContext) -> Error) -> Error {
        self.fused = true;
        build(failure(&self.source, line))
    }
}

/// The location of a failure on line `line`, which carries no excerpt.
fn failure(source: &str, line: usize) -> LineContext {
    LineContext {
        source: source.to_owned(),
        line,
        excerpt: None,
    }
}

fn context(source: &str, line: usize, content: &str) -> LineContext {
    let mut chars = content.chars();
    let excerpt = chars.by_ref().take(EXCERPT_CHAR_LIMIT).collect::<String>();
    let excerpt = if chars.next().is_some() {
        format!("{excerpt}…")
    } else {
        excerpt
    };
    LineContext {
        source: source.to_owned(),
        line,
        excerpt: Some(excerpt),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, Read};

    use proptest::prelude::*;
    use proptest::sample::select;

    use super::{EXCERPT_CHAR_LIMIT, Error, Lines};

    /// A small line limit, so that the tests filling a line to it stay cheap.
    const LIMIT: usize = 64;

    /// The tokens free text is built from, including ones that line reading must not split.
    const TEXT_TOKENS: &[&str] = &[
        "a",
        "Z",
        "0",
        " ",
        "\t",
        "\"",
        "\u{7f}",
        "é",
        "日",
        "\u{1f600}",
    ];

    /// Lines with their line endings, and whether the last line ends with one.
    fn lines() -> impl Strategy<Value = (Vec<(String, &'static str)>, bool)> {
        (
            proptest::collection::vec(
                (
                    archivindex_test_support::prop::tokens_of(TEXT_TOKENS, 0..=200),
                    select(vec!["\n", "\r\n"]),
                ),
                0..=8,
            ),
            any::<bool>(),
        )
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("failed"))
        }
    }

    impl BufRead for FailingReader {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            Err(std::io::Error::other("failed"))
        }

        fn consume(&mut self, _amount: usize) {}
    }

    #[test]
    fn next_content_skips_blanks_and_counts_lines() -> Result<(), Box<dyn std::error::Error>> {
        let mut lines = Lines::with_source(&b"first\r\n\n \nsecond"[..], "test");

        let location = lines.next_content()?.expect("first line");
        assert_eq!((location.line, location.content), (1, "first"));
        // The blank second line is skipped but counted; the third holds a space.
        let location = lines.next_content()?.expect("third line");
        assert_eq!((location.line, location.content), (3, " "));
        let location = lines.next_content()?.expect("fourth line");
        assert_eq!((location.line, location.content), (4, "second"));
        assert_eq!(lines.next_content()?, None);

        Ok(())
    }

    #[test]
    fn blank_lines_are_rejected_when_the_source_is_strict() {
        let mut lines = Lines::with_source(&b"first\n\nsecond\n"[..], "test").reject_blank_lines();

        let location = lines.next_content().expect("a line").expect("first line");
        assert_eq!((location.line, location.content), (1, "first"));
        let error = lines.next_content().expect_err("the blank second line");
        assert!(matches!(error, Error::Blank { ref context } if context.line == 2));
        assert!(lines.next_content().expect("fused source").is_none());
    }

    /// A file that simply ends with a line ending has no blank line to reject.
    #[test]
    fn a_trailing_line_ending_is_not_a_blank_line() -> Result<(), Error> {
        let mut lines = Lines::with_source(&b"only\r\n"[..], "test").reject_blank_lines();

        assert_eq!(lines.next_content()?.map(|line| line.content), Some("only"));
        assert_eq!(lines.next_content()?, None);

        Ok(())
    }

    #[test]
    fn over_long_and_invalid_lines_are_rejected() {
        let mut input = vec![b'a'; LIMIT];
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(&[b'b'; LIMIT + 1]);
        input.push(b'\n');
        let mut lines = Lines::with_source(&input[..], "long.jsonl").with_max_line_bytes(LIMIT);

        let location = lines
            .next_content()
            .expect("a line at the limit")
            .expect("l");
        assert_eq!((location.line, location.content.len()), (1, LIMIT));
        let error = lines.next_content().expect_err("one byte over the limit");
        assert!(matches!(
            error,
            Error::TooLong { ref context, limit } if context.line == 2 && limit == LIMIT
        ));
        assert!(lines.next_content().expect("fused source").is_none());

        let mut lines = Lines::with_source(&b"\xff\n"[..], "bad.jsonl");
        let error = lines.next_content().expect_err("invalid UTF-8");
        assert!(matches!(error, Error::InvalidUtf8 { .. }));
    }

    #[test]
    fn limit_sized_crlf_line_does_not_leave_a_blank_line() -> Result<(), Error> {
        let mut input = vec![b'a'; LIMIT];
        input.extend_from_slice(b"\r\nnext\n");
        let mut lines = Lines::with_source(&input[..], "limit.jsonl")
            .reject_blank_lines()
            .with_max_line_bytes(LIMIT);

        let location = lines.next_content()?.expect("line at the limit");
        assert_eq!((location.line, location.content.len()), (1, LIMIT));
        let location = lines.next_content()?.expect("following line");
        assert_eq!((location.line, location.content), (2, "next"));
        assert_eq!(lines.next_content()?, None);
        Ok(())
    }

    #[test]
    fn carriage_returns_at_the_limit_cannot_split_a_physical_line() {
        let mut input = vec![b'a'; LIMIT];
        input.extend_from_slice(b"\r\rmore\n");
        let mut lines = Lines::with_source(&input[..], "long.jsonl").with_max_line_bytes(LIMIT);

        let error = lines.next_content().expect_err("overlong physical line");
        assert!(matches!(error, Error::TooLong { ref context, .. } if context.line == 1));
        assert!(lines.next_content().expect("fused source").is_none());
    }

    /// A line the default limit accepts is rejected once a smaller limit replaces it.
    #[test]
    fn the_line_limit_is_configurable() {
        let input = b"aaaaaaaa\n";

        let mut lines = Lines::with_source(&input[..], "short.jsonl").with_max_line_bytes(4);
        let error = lines
            .next_content()
            .expect_err("a line over the custom limit");
        assert!(matches!(error, Error::TooLong { limit: 4, .. }));

        let mut lines = Lines::with_source(&input[..], "short.jsonl");
        let location = lines.next_content().expect("a line").expect("the line");
        assert_eq!(location.content, "aaaaaaaa");
    }

    #[test]
    fn an_io_failure_fuses_the_line_source() {
        let mut lines = Lines::with_source(FailingReader, "broken.cdxj");

        let error = lines.next_content().expect_err("the first read fails");
        assert!(matches!(error, Error::Io { .. }));
        assert_eq!(error.context().source, "broken.cdxj");
        assert_eq!(error.context().line, 1);
        assert!(lines.next_content().expect("fused source").is_none());
    }

    #[test]
    fn owned_context_survives_later_reads() -> Result<(), Error> {
        let input = format!("{}\nnext\n", "日".repeat(EXCERPT_CHAR_LIMIT + 1));
        let mut lines = Lines::with_source(input.as_bytes(), "unicode.jsonl");
        let location = lines.next_content()?.expect("first line");
        let context = location.into_owned();
        assert_eq!(lines.next_content()?.map(|line| line.content), Some("next"));
        drop(lines);
        assert_eq!(context.source, "unicode.jsonl");
        assert_eq!(context.line, 1);
        assert_eq!(
            context.excerpt,
            Some(format!("{}…", "日".repeat(EXCERPT_CHAR_LIMIT)))
        );
        Ok(())
    }

    /// Every non-blank line is returned once, in order, under its own line number, and each carries
    /// an excerpt bounded by a character count rather than a byte count.
    #[proptest::property_test]
    fn content_lines_are_returned_with_their_numbers(
        #[strategy = lines()] input: (Vec<(String, &'static str)>, bool),
    ) {
        let (lines, ends_with_a_line_ending) = input;
        let mut text = String::new();
        for (index, (content, ending)) in lines.iter().enumerate() {
            text.push_str(content);
            if ends_with_a_line_ending || index + 1 < lines.len() {
                text.push_str(ending);
            }
        }

        let mut source = Lines::with_source(text.as_bytes(), "test.jsonl");
        let mut read = Vec::new();
        while let Some(location) = source.next_content().unwrap() {
            let content = location.content;
            let context = location.into_owned();
            let excerpt = context.excerpt.clone().expect("content has an excerpt");
            // The generated alphabet has no ellipsis, so only truncation can add one.
            if let Some(prefix) = excerpt.strip_suffix('\u{2026}') {
                prop_assert_eq!(prefix.chars().count(), EXCERPT_CHAR_LIMIT);
                prop_assert!(content.starts_with(prefix));
                prop_assert!(content.chars().count() > EXCERPT_CHAR_LIMIT);
            } else {
                prop_assert_eq!(&excerpt, content);
                prop_assert!(excerpt.chars().count() <= EXCERPT_CHAR_LIMIT);
            }
            prop_assert_eq!(&context.source, "test.jsonl");
            read.push((context.line, content.to_owned()));
        }

        let expected = lines
            .into_iter()
            .enumerate()
            .filter(|(_, (content, _))| !content.is_empty())
            .map(|(index, (content, _))| (index + 1, content))
            .collect::<Vec<_>>();

        prop_assert_eq!(read, expected);
    }
}
