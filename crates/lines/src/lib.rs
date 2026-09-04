//! Line-oriented reading with bounded lines and source diagnostics.
//!
//! [`Lines`] reads UTF-8 lines with a size limit, strips trailing CR and LF bytes, and tracks line
//! numbers. It skips blank lines by default and can instead reject them. Successful reads return
//! borrowed context; [`LineContextRef::into_owned`] creates an owned location with a bounded
//! excerpt.
//!
//! ```
//! let mut lines =
//!     archivindex_lines::Lines::with_source(&b"first\r\n\nsecond\n"[..], "test.jsonl");
//!
//! let (context, content) = lines.next_content()?.expect("the first line");
//! assert_eq!((context.line, content), (1, "first"));
//!
//! // The blank line is skipped but still counted.
//! let (context, content) = lines.next_content()?.expect("the third line");
//! assert_eq!((context.line, content), (3, "second"));
//! assert_eq!(lines.next_content()?, None);
//! # Ok::<(), archivindex_lines::Error>(())
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::io::{BufRead, Read};

/// The maximum number of content characters in an excerpt, excluding the trailing ellipsis.
const EXCERPT_CHAR_LIMIT: usize = 160;

/// The longest line accepted from a source, excluding its line ending.
///
/// Records can carry extracted full text, so the bound is generous; it exists so that a hostile
/// file cannot make the reader buffer an unbounded line.
pub const MAX_LINE_BYTES: usize = 16 << 20;

/// Bounded source context for an error on one line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineContext {
    /// Member path or caller-supplied stream name.
    pub source: String,
    /// One-based line number.
    pub line: usize,
    /// A bounded excerpt, present for converted [`LineContextRef`] values and absent for read
    /// errors.
    pub excerpt: Option<String>,
}

/// Borrowed source context for a successfully read line.
///
/// Reading this context does not allocate diagnostic strings. Call [`Self::into_owned`] when
/// retaining a location, for example when parsing the accompanying content fails. The context
/// borrows the reader and remains valid until its next mutable use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineContextRef<'a> {
    /// Member path or caller-supplied stream name.
    pub source: &'a str,
    /// One-based line number.
    pub line: usize,
    content: &'a str,
}

impl LineContextRef<'_> {
    /// Detach this location from the reader, constructing its bounded excerpt.
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
#[error("failed to read {context}")]
pub struct Error {
    /// Location of the failed read.
    pub context: LineContext,
    /// Underlying I/O error.
    #[source]
    pub source: std::io::Error,
}

/// A line source that trims line endings, tracks line numbers, and either skips or rejects blank
/// lines.
pub struct Lines<R> {
    underlying: R,
    /// Scratch buffer reused across lines; returned content is only valid until the next call.
    line: Vec<u8>,
    line_number: usize,
    source: String,
    reject_blanks: bool,
    fused: bool,
}

impl<R: BufRead> Lines<R> {
    /// Create a line source carrying a member path or other source name for diagnostics.
    pub fn with_source(underlying: R, source: impl Into<String>) -> Self {
        Self {
            underlying,
            line: Vec::new(),
            line_number: 0,
            source: source.into(),
            reject_blanks: false,
            fused: false,
        }
    }

    /// Report blank lines as invalid data instead of skipping them.
    #[must_use]
    pub const fn rejecting_blank_lines(mut self) -> Self {
        self.reject_blanks = true;
        self
    }

    /// Read the next non-blank line, returning its borrowed location and its content with any
    /// trailing CR and LF bytes removed. Spaces and tabs alone do not make a line blank.
    ///
    /// Blank lines are skipped rather than returned, but still counted, unless the source was built
    /// with [`Self::rejecting_blank_lines`]; `None` marks the end of the stream.
    ///
    /// Successful reads allocate no diagnostic strings, though the scratch buffer may grow. Read
    /// errors copy the source name but carry no excerpt; [`LineContextRef::into_owned`] copies the
    /// source name and constructs an excerpt from successfully read content.
    ///
    /// ```
    /// let mut lines = archivindex_lines::Lines::with_source(&b"42\n"[..], "numbers");
    /// let (location, content) = lines.next_content()?.expect("a number");
    /// let number = content.parse::<u64>().map_err(|_| location.into_owned());
    /// assert_eq!(number, Ok(42));
    /// # Ok::<(), archivindex_lines::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error, and yields nothing further, when the underlying read fails, when a line is
    /// longer than [`MAX_LINE_BYTES`], when a line is not valid UTF-8, or when a line is blank and
    /// the source was built with [`Self::rejecting_blank_lines`].
    pub fn next_content(&mut self) -> Result<Option<(LineContextRef<'_>, &str)>, Error> {
        if self.fused {
            return Ok(None);
        }

        loop {
            self.line.clear();

            // Allow both bytes of CRLF after content at the limit. A full buffer without LF cannot
            // contain a complete permitted line ending and must not become a split line.
            let read = Read::by_ref(&mut self.underlying)
                .take(MAX_LINE_BYTES as u64 + 2)
                .read_until(b'\n', &mut self.line)
                .map_err(|source| self.fail(self.line_number + 1, source))?;
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

            if trimmed > MAX_LINE_BYTES
                || (read == MAX_LINE_BYTES + 2 && !self.line.ends_with(b"\n"))
            {
                let source = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("line exceeds {MAX_LINE_BYTES} bytes"),
                );
                return Err(self.fail(self.line_number, source));
            }

            if trimmed == 0 && self.reject_blanks {
                let source = std::io::Error::new(std::io::ErrorKind::InvalidData, "blank line");
                return Err(self.fail(self.line_number, source));
            }

            if trimmed > 0 {
                // The arms touch disjoint fields, so the returned borrow of `line` may coexist with
                // fusing the source; a `&mut self` helper could not be called here.
                let line_text = match std::str::from_utf8(&self.line[..trimmed]) {
                    Ok(line_text) => line_text,
                    Err(error) => {
                        self.fused = true;
                        let source = std::io::Error::new(std::io::ErrorKind::InvalidData, error);
                        return Err(line_error(&self.source, self.line_number, source));
                    }
                };
                let location = LineContextRef {
                    source: &self.source,
                    line: self.line_number,
                    content: line_text,
                };

                return Ok(Some((location, line_text)));
            }
        }
    }

    /// Fuse the source and describe a failure on line `line`.
    fn fail(&mut self, line: usize, source: std::io::Error) -> Error {
        self.fused = true;
        line_error(&self.source, line, source)
    }
}

/// Describe an I/O failure on line `line`, without an excerpt.
fn line_error(source_name: &str, line: usize, source: std::io::Error) -> Error {
    Error {
        context: LineContext {
            source: source_name.to_owned(),
            line,
            excerpt: None,
        },
        source,
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
    use std::io::{self, BufRead, Read};

    use proptest::prelude::*;
    use proptest::sample::select;

    use super::{EXCERPT_CHAR_LIMIT, Error, Lines, MAX_LINE_BYTES};

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
                    archivindex_test_support::strategies::tokens_of(TEXT_TOKENS, 0..=200),
                    select(vec!["\n", "\r\n"]),
                ),
                0..=8,
            ),
            any::<bool>(),
        )
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("failed"))
        }
    }

    impl BufRead for FailingReader {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            Err(io::Error::other("failed"))
        }

        fn consume(&mut self, _amount: usize) {}
    }

    #[test]
    fn next_content_skips_blanks_and_counts_lines() -> Result<(), Box<dyn std::error::Error>> {
        let mut lines = Lines::with_source(&b"first\r\n\n \nsecond"[..], "test");

        let (location, line_text) = lines.next_content()?.expect("first line");
        assert_eq!((location.line, line_text), (1, "first"));
        // The blank second line is skipped but counted; the third holds a space.
        let (location, line_text) = lines.next_content()?.expect("third line");
        assert_eq!((location.line, line_text), (3, " "));
        let (location, line_text) = lines.next_content()?.expect("fourth line");
        assert_eq!((location.line, line_text), (4, "second"));
        assert_eq!(lines.next_content()?, None);

        Ok(())
    }

    #[test]
    fn blank_lines_are_rejected_when_the_source_is_strict() {
        let mut lines =
            Lines::with_source(&b"first\n\nsecond\n"[..], "test").rejecting_blank_lines();

        let (location, line_text) = lines.next_content().expect("a line").expect("first line");
        assert_eq!((location.line, line_text), (1, "first"));
        let error = lines.next_content().expect_err("the blank second line");
        assert_eq!(
            (error.context.line, error.source.kind()),
            (2, io::ErrorKind::InvalidData)
        );
        assert!(lines.next_content().expect("fused source").is_none());
    }

    /// A file that simply ends with a line ending has no blank line to reject.
    #[test]
    fn a_trailing_line_ending_is_not_a_blank_line() -> Result<(), Error> {
        let mut lines = Lines::with_source(&b"only\r\n"[..], "test").rejecting_blank_lines();

        assert_eq!(lines.next_content()?.map(|(_, text)| text), Some("only"));
        assert_eq!(lines.next_content()?, None);

        Ok(())
    }

    #[test]
    fn over_long_and_invalid_lines_are_rejected() {
        let mut input = vec![b'a'; MAX_LINE_BYTES];
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(&vec![b'b'; MAX_LINE_BYTES + 1]);
        input.push(b'\n');
        let mut lines = Lines::with_source(&input[..], "long.jsonl");

        let (location, line_text) = lines
            .next_content()
            .expect("a line at the limit")
            .expect("l");
        assert_eq!((location.line, line_text.len()), (1, MAX_LINE_BYTES));
        let error = lines.next_content().expect_err("one byte over the limit");
        assert_eq!(
            (error.context.line, error.source.kind()),
            (2, io::ErrorKind::InvalidData)
        );
        assert!(lines.next_content().expect("fused source").is_none());

        let mut lines = Lines::with_source(&b"\xff\n"[..], "bad.jsonl");
        let error = lines.next_content().expect_err("invalid UTF-8");
        assert_eq!(error.source.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn limit_sized_crlf_line_does_not_leave_a_blank_line() -> Result<(), Error> {
        let mut input = vec![b'a'; MAX_LINE_BYTES];
        input.extend_from_slice(b"\r\nnext\n");
        let mut lines = Lines::with_source(&input[..], "limit.jsonl").rejecting_blank_lines();

        let (location, content) = lines.next_content()?.expect("line at the limit");
        assert_eq!((location.line, content.len()), (1, MAX_LINE_BYTES));
        let (location, content) = lines.next_content()?.expect("following line");
        assert_eq!((location.line, content), (2, "next"));
        assert_eq!(lines.next_content()?, None);
        Ok(())
    }

    #[test]
    fn carriage_returns_at_the_limit_cannot_split_a_physical_line() {
        let mut input = vec![b'a'; MAX_LINE_BYTES];
        input.extend_from_slice(b"\r\rmore\n");
        let mut lines = Lines::with_source(&input[..], "long.jsonl");

        let error = lines.next_content().expect_err("overlong physical line");
        assert_eq!(error.context.line, 1);
        assert_eq!(error.source.kind(), io::ErrorKind::InvalidData);
        assert!(lines.next_content().expect("fused source").is_none());
    }

    #[test]
    fn an_io_failure_fuses_the_line_source() {
        let mut lines = Lines::with_source(FailingReader, "broken.cdxj");

        let error = lines.next_content().expect_err("the first read fails");
        assert_eq!(error.context.source, "broken.cdxj");
        assert_eq!(error.context.line, 1);
        assert!(lines.next_content().expect("fused source").is_none());
    }

    #[test]
    fn owned_context_survives_later_reads() -> Result<(), Error> {
        let input = format!("{}\nnext\n", "日".repeat(EXCERPT_CHAR_LIMIT + 1));
        let mut lines = Lines::with_source(input.as_bytes(), "unicode.jsonl");
        let (location, _) = lines.next_content()?.expect("first line");
        let context = location.into_owned();
        assert_eq!(lines.next_content()?.map(|(_, text)| text), Some("next"));
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
    #[test_strategy::proptest]
    fn content_lines_are_returned_with_their_numbers(
        #[strategy(lines())] input: (Vec<(String, &'static str)>, bool),
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
        while let Some((location, content)) = source.next_content().unwrap() {
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
