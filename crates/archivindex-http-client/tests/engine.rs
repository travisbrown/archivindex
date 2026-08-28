//! The engine each client reports.

use std::sync::Arc;

use archivindex_http_client::recorder::Recorder;
use archivindex_http_client::reqwest::ReqwestClient;
use archivindex_http_client::{Client, Engine};

/// Whether the workspace lockfile resolves `engine` to the version it reports.
fn is_locked(engine: Engine) -> bool {
    let lockfile =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"))
            .expect("a readable lockfile");
    let version = engine.version.expect("a versioned engine");

    let name_line = format!("name = \"{}\"", engine.name);
    let version_line = format!("version = \"{version}\"");

    lockfile
        .lines()
        .zip(lockfile.lines().skip(1))
        .any(|(name, version)| name == name_line && version == version_line)
}

/// Each client names its own engine, including when it is shared as a trait object.
#[test]
fn clients_report_their_engines() {
    let recorder: Arc<dyn Client> = Arc::new(Recorder::new());
    let reqwest: Arc<dyn Client> = Arc::new(ReqwestClient::new());

    assert_eq!(recorder.engine(), Engine::RECORDER);
    assert_eq!(reqwest.engine(), Engine::REQWEST);
}

/// An engine is displayed in the `User-Agent` product syntax, in which the version and the comment
/// holding the profile are each optional. The engines here are made up so that the expected text
/// does not change with a dependency's version.
#[test]
fn engines_are_displayed_as_products() {
    let engine = Engine {
        name: "example",
        version: None,
        profile: None,
    };
    let versioned = Engine {
        version: Some("1.2.3"),
        ..engine
    };
    let profiled = Engine {
        profile: Some("chrome_136"),
        ..engine
    };
    let complete = Engine {
        profile: Some("chrome_136"),
        ..versioned
    };

    assert_eq!(engine.to_string(), "example");
    assert_eq!(versioned.to_string(), "example/1.2.3");
    assert_eq!(profiled.to_string(), "example (chrome_136)");
    assert_eq!(complete.to_string(), "example/1.2.3 (chrome_136)");
}

/// The `reqwest` version is written out in the source, so a dependency update can leave it
/// behind. Comparing it with the lockfile turns that into a test failure.
#[test]
fn the_reqwest_version_is_the_locked_one() {
    assert!(is_locked(Engine::REQWEST));
}

/// The `wreq` engine carries the client's profile, and its version is written out in the source
/// like the `reqwest` one.
#[cfg(feature = "wreq")]
#[test]
fn the_wreq_engine_has_its_profile_and_the_locked_version() {
    use archivindex_http_client::wreq::WreqClient;
    use wreq_util::Profile;

    let engine = WreqClient::new(Profile::Chrome136).engine();

    assert_eq!(engine, Engine::wreq_with_profile(Profile::Chrome136));
    assert_eq!(engine.name, "wreq");
    assert_eq!(engine.profile, Some("chrome_136"));
    assert!(is_locked(engine));
}
