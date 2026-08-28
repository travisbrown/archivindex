//! How a client reports a transport that fails or stalls.
//!
//! Every client reports a failure in the same way: an I/O error keeps its kind, and a response
//! head that does not arrive in time is a timed-out I/O operation.

use std::error::Error as StdError;
use std::io::ErrorKind;

use crate::Error;
use crate::framing::{ResponseCapture, ResponseError, Truncation};

/// End `capture` as [`ResponseCapture::end`] does, reporting a timeout before a complete
/// head as a timed-out I/O operation.
///
/// [`ResponseError::IncompleteHead`] says that the connection ended, which a timeout does
/// not mean.
pub fn end(capture: &mut ResponseCapture, reason: Option<Truncation>) -> Result<(), Error> {
    capture.end(reason).map_err(|error| match (error, reason) {
        (ResponseError::IncompleteHead, Some(Truncation::Time)) => timed_out().into(),
        (error, _) => error.into(),
    })
}

/// The error for a response head that did not arrive in time.
pub fn timed_out() -> std::io::Error {
    std::io::Error::new(ErrorKind::TimedOut, "no response head arrived in time")
}

/// Report the failure of an HTTP library as an I/O error of the kind that caused it, when one did.
///
/// `timed_out` is the library's own verdict, for a timeout that it does not report as an I/O
/// error. Failures without an I/O cause retain their client-specific variant.
pub fn error(error: impl StdError + Send + Sync + Into<Error> + 'static, timed_out: bool) -> Error {
    let kind = if timed_out {
        Some(ErrorKind::TimedOut)
    } else {
        io_kind(&error)
    };

    match kind {
        Some(kind) => std::io::Error::new(kind, error).into(),
        None => error.into(),
    }
}

/// Whether a failure means that the transport stopped before the message ended.
///
/// These are the kinds a blocking read reports for the same event. Malformed chunk framing has
/// other kinds, and is an error instead of a truncation.
pub fn is_disconnect(error: &(dyn StdError + 'static)) -> bool {
    matches!(
        io_kind(error),
        Some(ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset)
    )
}

/// The kind of the first I/O error among `error` and its sources.
fn io_kind(error: &(dyn StdError + 'static)) -> Option<ErrorKind> {
    std::iter::successors(Some(error), |error| (*error).source())
        .find_map(|error| error.downcast_ref::<std::io::Error>())
        .map(std::io::Error::kind)
}

#[cfg(test)]
mod tests {
    use super::{Error, error};

    /// A client configuration failure has no I/O cause and keeps its library error type.
    #[test]
    fn reqwest_failures_without_io_causes_keep_their_type() {
        let source = ::reqwest::Client::builder()
            .user_agent("\n")
            .build()
            .unwrap_err();

        assert!(matches!(error(source, false), Error::Reqwest(source) if source.is_builder()));
    }

    #[cfg(feature = "wreq")]
    #[test]
    fn wreq_failures_without_io_causes_keep_their_type() {
        let source = ::wreq::Client::builder()
            .user_agent("\n")
            .build()
            .err()
            .expect("an invalid user agent");

        assert!(matches!(error(source, false), Error::Wreq(source) if source.is_builder()));
    }
}
