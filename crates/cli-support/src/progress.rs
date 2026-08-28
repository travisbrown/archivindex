//! Progress indicators for long-running commands.

use std::borrow::Cow;

use indicatif::{ProgressBar, ProgressStyle};

/// The spinner template showing the message alone.
const MESSAGE_TEMPLATE: &str = "{msg} {spinner}";

/// The spinner template showing the message, the count, and the unit held in the prefix.
const COUNTING_TEMPLATE: &str = "{msg} {human_pos} {prefix} {spinner}";

/// The bar template showing the message, the bar, the position out of the length, and the time
/// remaining.
const BAR_TEMPLATE: &str = "{msg} [{bar:40.cyan/blue}] {human_pos}/{human_len} ({eta})";

/// The bar template with the unit held in the prefix, written after the counts.
const COUNTING_BAR_TEMPLATE: &str =
    "{msg} [{bar:40.cyan/blue}] {human_pos}/{human_len} {prefix} ({eta})";

/// The characters the bar is drawn with: filled, the leading edge, then unfilled.
const BAR_CHARS: &str = "=>-";

/// A spinner on standard error showing `message`, and a count of `unit` when one is given.
///
/// With a unit, each [`ProgressBar::inc`] advances the count shown after the message. The spinner
/// draws nothing when standard error is not a terminal.
#[must_use]
#[expect(
    clippy::missing_panics_doc,
    reason = "the templates are constants that parse"
)]
pub fn spinner(message: impl Into<Cow<'static, str>>, unit: Option<&str>) -> ProgressBar {
    let template = unit.map_or(MESSAGE_TEMPLATE, |_| COUNTING_TEMPLATE);
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template(template)
            .expect("invariant violation: the spinner template is well formed"),
    );
    progress.set_message(message);
    if let Some(unit) = unit {
        progress.set_prefix(unit.to_owned());
    }

    progress
}

/// A bar on standard error of `length` steps, showing `message` and a count of `unit` when one is
/// given.
///
/// Each [`ProgressBar::inc`] advances the bar towards `length`. Use [`spinner`] instead when the
/// number of steps is not known ahead of time. The bar draws nothing when standard error is not a
/// terminal, and can be added to an [`indicatif::MultiProgress`] to draw alongside others.
#[must_use]
#[expect(
    clippy::missing_panics_doc,
    reason = "the templates are constants that parse"
)]
pub fn bar(length: u64, message: impl Into<Cow<'static, str>>, unit: Option<&str>) -> ProgressBar {
    let template = unit.map_or(BAR_TEMPLATE, |_| COUNTING_BAR_TEMPLATE);
    let progress = ProgressBar::new(length);
    progress.set_style(
        ProgressStyle::with_template(template)
            .expect("invariant violation: the bar template is well formed")
            .progress_chars(BAR_CHARS),
    );
    progress.set_message(message);
    if let Some(unit) = unit {
        progress.set_prefix(unit.to_owned());
    }

    progress
}

#[cfg(test)]
mod tests {
    use super::{bar, spinner};

    #[test]
    fn a_spinner_shows_its_message_and_counts_without_a_length() {
        let counting = spinner("Archiving", Some("URLs"));
        let plain = spinner("Downloading", None);

        counting.inc(2);

        assert_eq!(counting.message(), "Archiving");
        assert_eq!(counting.prefix(), "URLs");
        assert_eq!(counting.position(), 2);
        assert_eq!(counting.length(), None);
        assert_eq!(plain.message(), "Downloading");
        assert_eq!(plain.prefix(), "");
    }

    #[test]
    fn a_bar_shows_its_message_and_counts_against_its_length() {
        let counting = bar(10, "Indexing", Some("files"));
        let plain = bar(4, "Verifying", None);

        counting.inc(3);

        assert_eq!(counting.message(), "Indexing");
        assert_eq!(counting.prefix(), "files");
        assert_eq!(counting.position(), 3);
        assert_eq!(counting.length(), Some(10));
        assert_eq!(plain.length(), Some(4));
        assert_eq!(plain.prefix(), "");
    }
}
