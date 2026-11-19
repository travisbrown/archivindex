//! Synchronous entry to the clients that run on Tokio.

use std::time::Instant;

use http::Uri;

use crate::{Error, Exchange, request};

pub fn fetch<F: Future<Output = Result<Exchange, Error>>>(
    target: &Uri,
    deadline: Option<Instant>,
    start: impl FnOnce(fluent_uri::Uri<String>) -> F + Send,
) -> Result<Exchange, Error> {
    let target_uri = request::validate(target, deadline)?;

    // Use a dedicated thread so callers already running on Tokio do not nest `block_on`.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let captured = runtime.block_on(start(target_uri));
                // System DNS uses `spawn_blocking`. Waiting for it while the runtime drops would
                // undo a connect or capture timeout; it may finish after the caller returns.
                runtime.shutdown_background();

                captured
            })
            .join()
            .map_err(|_| std::io::Error::other("the capture worker panicked"))?
    })
}
