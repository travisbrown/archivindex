//! Compare deferred and eager diagnostics over a million valid CDXJ-shaped lines.
//!
//! Compare borrowing each location with copying its source name and constructing a bounded excerpt.
//! Each input line exceeds the excerpt limit, so every owned location includes a truncated excerpt.
use std::hint::black_box;
use std::time::Instant;

use archivindex_lines::Lines;

/// A representative index line, longer than [`archivindex_lines`]'s excerpt limit.
const LINE: &str = concat!(
    r#"com,example)/path/to/page?a=1 20240101120000 {"url": "https://example.com/path/to/page"#,
    r#"?a=1", "mime": "text/html", "status": "200", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5Q"#,
    r#"BYJ", "length": "12345", "offset": "987654", "filename": "ARCHIVE-20240101120000.warc.gz"}"#,
);

const LINE_COUNT: usize = 1_000_000;

fn main() {
    let input = format!("{LINE}\n").repeat(LINE_COUNT);
    for round in 0..5 {
        // Alternate order to reduce systematic warm-up bias.
        for owning in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let mut lines = Lines::with_source(input.as_bytes(), "indexes/captures.cdxj");
            let start = Instant::now();
            let mut count = 0;
            while let Some((location, content)) = lines.next_content().unwrap() {
                // Owning every location is what a caller pays when diagnostics are eager.
                if owning {
                    black_box(location.into_owned());
                } else {
                    black_box(location);
                }
                black_box(content);
                count += 1;
            }
            assert_eq!(count, LINE_COUNT);
            println!(
                "{}: {:?}",
                if owning { "eager" } else { "deferred" },
                start.elapsed()
            );
        }
    }
}
